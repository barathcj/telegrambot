# talos_watcher_fn.py
import hmac, hashlib, base64, datetime, json, time, threading
from urllib.parse import urlparse
import certifi, requests
from websocket import create_connection, WebSocketTimeoutException, WebSocketBadStatusException

# ===== Multi-watcher registry =====
_WATCHERS: dict[str, dict] = {}  # name -> {"thread": Thread, "stop": Event}

def _now_utc_iso() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _headers(ws_url: str, api_key: str, api_secret: str):
    ws_url = (ws_url or "").strip()
    api_key = (api_key or "").strip()
    api_secret = (api_secret or "").strip()
    u = urlparse(ws_url)
    host, path = u.netloc, (u.path or "/ws/v1")
    ts = _now_utc_iso()
    payload = "\n".join(["GET", ts, host, path])
    sig = base64.urlsafe_b64encode(
        hmac.new(api_secret.encode("ascii"), payload.encode("ascii"), hashlib.sha256).digest()
    ).decode()
    headers = [f"TALOS-KEY: {api_key}", f"TALOS-TS: {ts}", f"TALOS-SIGN: {sig}"]
    dbg = {"ts": ts, "host": host, "path": path}
    return headers, dbg

def _notify_http(tg_token: str, chat_id: int, text: str, md: bool = True):
    try:
        url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        data = {"chat_id": chat_id, "text": text}
        if md:
            data["parse_mode"] = "Markdown"
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print("telegram send failed:", e)

# ---------- formatting helpers ----------
def _fmt_qty(x):
    try:
        return f"{float(x):.8f}"
    except Exception:
        return str(x or "0")

def _fmt_px(p):
    if p is None or p == "":
        return "—"
    try:
        p = float(p)
        return f"{p:,.6f}" if abs(p) < 1 else f"{p:,.8f}"
    except Exception:
        return str(p)

def _fmt_usd(n):
    try:
        return f"${float(n):,.2f}"
    except Exception:
        return "—"

def _price_from(d: dict):
    return (
        d.get("Price")
        or d.get("LimitPx")
        or d.get("Px")
        or d.get("StopPx")
        or d.get("AvgPx")
        or d.get("LastPx")
    )

def _notional(qty, px):
    try:
        return float(qty) * float(px)
    except Exception:
        return None

def _short_id(s: str | None, n: int = 8) -> str:
    if not s:
        return "-"
    return s[:n] + "…" if len(s) > n else s

def _md_escape(s: str | None) -> str:
    if s is None:
        return "-"
    return (
        str(s)
        .replace("_", "\\_").replace("*", "\\*")
        .replace("[", "\\[").replace("]", "\\]")
        .replace("`", "\\`")
    )

def _ordtype_str(ot):
    m = {"1":"Market","2":"Limit","3":"Stop","4":"StopLimit","K":"Mkt→Limit","P":"Pegged"}
    if ot is None:
        return "-"
    s = str(ot)
    return m.get(s, s)

def _qty_ccy(d: dict) -> str:
    for k in ("QtyCurrency","OrderQtyCurrency","Currency","BaseCurrency","Base","BaseCcy"):
        v = d.get(k)
        if v:
            return str(v).upper()
    sym = d.get("Symbol") or ""
    if "/" in sym: return sym.split("/")[0].upper()
    if "-" in sym: return sym.split("-")[0].upper()
    return "-"

def _sym_split(sym: str):
    if not sym:
        return "-", "-"
    s = sym.replace("_","-").replace("/","-")
    parts = [p for p in s.split("-") if p]
    base = parts[0].upper() if parts else "-"
    quote = parts[1].upper() if len(parts) > 1 else "-"
    return base, quote

def _qty_is_quote(d: dict, base: str, quote: str) -> bool:
    for k in ("OrderQtyInQuote","QtyInQuote","IsQuoteQty","QtyIsQuote","QuoteQtyFlag"):
        v = d.get(k)
        if isinstance(v, bool): return v
        if isinstance(v, str) and v.strip().lower() in ("true","1","yes"): return True
    qty_ccy = (d.get("QtyCurrency") or d.get("OrderQtyCurrency") or d.get("Currency") or "").upper()
    if qty_ccy == quote and quote not in ("","-"): return True
    if qty_ccy == base  and base  not in ("","-"): return False
    return False

def _fmt_amt_ccy(n, ccy: str):
    c = (ccy or "").upper()
    if c in ("USD","USDT","USDC"):
        return _fmt_usd(n)
    return f"{_fmt_qty(n)} {c or '-'}"

def _fmt_dur(sec: float) -> str:
    sec = int(max(sec, 0))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"
# ---------------------------------------

def _talos_loop(
    stop_evt: threading.Event,
    tg_token: str, chat_id: int,
    ws_url: str, api_key: str, api_secret: str,
    subscribe_user: str | None,
    exclude_users: set[str],
    show_per_exec_fill: bool,
    account_label: str | None,
    subaccount_filter: set[str] | None
):
    exclude_upper = {u.upper() for u in (exclude_users or set())}
    filter_upper = {s.upper() for s in (subaccount_filter or set())}
    filled_announced = set()
    backoff = 1

    learned_acct = account_label  # for banner label
    hello_info = None
    connected_banner_sent = False

    # --- reconnect state ---
    downtime_start = None   # time.monotonic() would also work; using time.time() for simplicity
    retries = 0             # reconnect attempts since last good link
    prev_session = None     # last known session id
    # ---

    while not stop_evt.is_set():
        try:
            headers, dbg = _headers(ws_url, api_key, api_secret)
            ws = create_connection(ws_url, header=headers, sslopt={"ca_certs": certifi.where()})
            ws.settimeout(25)

            # hello
            hello_raw = ws.recv()
            try:
                hello = json.loads(hello_raw)
            except Exception:
                hello = {}

            new_session = hello.get("session_id")
            hello_ts = hello.get("ts") or _now_utc_iso()

            # If we were down, announce a RECONNECT; else, the initial CONNECT banner
            if downtime_start is not None:
                delta = _fmt_dur(time.time() - downtime_start)
                acct_str = _md_escape(learned_acct) if learned_acct else "-"
                if prev_session:
                    sess_line = f"Session: `{_short_id(prev_session)} → {_short_id(new_session)}`"
                else:
                    sess_line = f"Session: `{_short_id(new_session)}`"
                _notify_http(
                    tg_token, chat_id,
                    (
                        f"🟢 *Reconnected to Talos* — {acct_str}\n"
                        f"{sess_line}\n"
                        f"Time: {hello_ts}\n"
                        f"(after {delta}, attempt {retries})"
                    )
                )
                downtime_start = None
                retries = 0
                connected_banner_sent = True   # don’t also send the normal Connected banner
                backoff = 1                    # reset backoff after a good handshake
            else:
                if learned_acct and not connected_banner_sent:
                    _notify_http(
                        tg_token, chat_id,
                        f"🟢 *Connected to Talos* — {_md_escape(learned_acct)}\n"
                        f"Session: `{_short_id(new_session)}`\n"
                        f"Time: {hello_ts}"
                    )
                    connected_banner_sent = True
                    backoff = 1                # reset backoff after a good handshake

            prev_session = new_session
            hello_info = {"session": new_session, "ts": hello_ts}

            # subscribe
            stream = {"name": "ExecutionReport", "StartDate": _now_utc_iso()}
            if subscribe_user:
                stream["User"] = subscribe_user
            ws.send(json.dumps({"reqid": 100, "type": "subscribe", "streams": [stream]}))

            while not stop_evt.is_set():
                try:
                    msg = json.loads(ws.recv())
                except WebSocketTimeoutException:
                    ws.send(json.dumps({"reqid": 1, "type": "ping", "ts": _now_utc_iso()}))
                    continue

                if msg.get("type") == "error":
                    _notify_http(tg_token, chat_id, f"⚠️ Talos error:\n`{json.dumps(msg, indent=2)}`")
                    continue
                if msg.get("type") != "ExecutionReport":
                    continue

                initial = msg.get("initial", False)
                for d in msg.get("data", []):
                    # event account/sub-account
                    event_acct = (
                        d.get("AccountName")
                        or d.get("SubAccount")
                        or d.get("TradingAccountName")
                        or d.get("Account")
                    )

                    # learn banner label if not provided
                    if not learned_acct and event_acct:
                        learned_acct = str(event_acct)

                    # send merged banner once
                    if learned_acct and hello_info and not connected_banner_sent:
                        _notify_http(
                            tg_token, chat_id,
                            f"🟢 *Connected to Talos* — {_md_escape(learned_acct)}\n"
                            f"Session: `{_short_id(hello_info['session'])}`\n"
                            f"Time: {hello_info['ts']}"
                        )
                        connected_banner_sent = True

                    # optional sub-account filter per watcher
                    if filter_upper:
                        ev = (str(event_acct or "")).upper()
                        if ev not in filter_upper:
                            continue

                    user = (d.get("RequestUser") or d.get("CustomerUser") or d.get("User") or "")
                    if exclude_upper and user.upper() in exclude_upper:
                        continue

                    exec_type  = d.get("ExecType")
                    ord_status = d.get("OrdStatus")
                    side  = d.get("Side") or "-"
                    sym   = d.get("Symbol") or "-"
                    oid   = d.get("OrderID") or "-"
                    ordqty  = d.get("OrderQty")
                    avgpx = d.get("AvgPx")
                    lastpx  = d.get("LastPx")
                    lastqty = d.get("LastQty")
                    cumqty  = d.get("CumQty")
                    leaves  = d.get("LeavesQty")
                    when    = d.get("TransactTime") or d.get("Ts") or d.get("Timestamp") or _now_utc_iso()
                    limitpx = _price_from(d)
                    ordtype = _ordtype_str(d.get("OrdType"))
                    comment = d.get("Comments")

                    acct_line = f"Acct: {_md_escape(event_acct) if event_acct else (_md_escape(learned_acct) if learned_acct else '-')}"
                    actor_line = f"By: `{_md_escape(user)}`"

                    # ===== Shared headline builder (base vs quote logic) =====
                    def _headline_from(q_raw, px_view):
                        base, quote = _sym_split(sym)
                        qty_in_quote = _qty_is_quote(d, base, quote)
                        base_qty, quote_ntn = None, None
                        try:
                            if q_raw is not None and px_view not in (None, "", 0, "0"):
                                qf = float(q_raw); pf = float(px_view)
                                if qty_in_quote:
                                    base_qty  = (qf / pf) if pf else None
                                    quote_ntn = qf
                                else:
                                    base_qty  = qf
                                    quote_ntn = qf * pf
                            elif q_raw is not None:
                                qf = float(q_raw)
                                if qty_in_quote: quote_ntn = qf
                                else:            base_qty  = qf
                        except Exception:
                            pass

                        if qty_in_quote and quote_ntn is not None:
                            return f"{_md_escape(side)} {base} ({_fmt_amt_ccy(quote_ntn, quote)})"
                        if base_qty is not None:
                            return f"{_md_escape(side)} { _fmt_qty(base_qty) } {base}"
                        unit     = _qty_ccy(d)
                        unit_str = f' {unit}' if unit and unit != '-' else ''
                        return f"{_md_escape(side)} { _fmt_qty(q_raw) }{unit_str}"
                    # ========================================================

                    # 🆕 New
                    if exec_type == "New" or ord_status == "New":
                        lines = [
                            f"🆕 *New order* - {_md_escape(learned_acct) if learned_acct else '-'}",
                            f"Sym: {_md_escape(sym)} · OrdType: {ordtype}",
                            _headline_from(ordqty, limitpx),
                            f"Px: { _fmt_px(limitpx) }",
                        ]
                        if comment:
                            lines.append(f"Comment: {_md_escape(comment)}")
                        lines.append("")  # spacer
                        lines.extend([
                            f"{acct_line}",
                            f"{actor_line}",
                            f"OrderID: `{oid}`",
                            f"Time: {when}",
                        ])
                        _notify_http(tg_token, chat_id, "\n".join(lines))
                        continue

                    # 🚫 Canceled
                    if ord_status == "Canceled" or exec_type == "Canceled":
                        reason = d.get("Text") or d.get("CancelReason")
                        q_raw = ordqty if ordqty not in (None, "", 0, "0") else (cumqty or lastqty)
                        px_for_view = limitpx or avgpx or lastpx
                        _notify_http(
                            tg_token, chat_id,
                            (
                                f"🚫 *Order Cancelled*{' [snapshot]' if initial else ''} — {_md_escape(sym)}\n"
                                f"{acct_line}  ·  OrdType: {ordtype}\n"
                                f"{_headline_from(q_raw, px_for_view)}\n"
                                f"Px: { _fmt_px(px_for_view) }\n"
                                f"{actor_line}\n"
                                f"Reason: {_md_escape(reason) if reason else '-'}\n"
                                f"OrderID: `{oid}`\n"
                                f"Time: {when}"
                            )
                        )
                        continue

                    # ✅ Per-exec fills (optional)
                    if show_per_exec_fill and exec_type == "Trade":
                        try:
                            if float(lastqty or 0) > 0:
                                _notify_http(
                                    tg_token, chat_id,
                                    (
                                        f"✅ *Fill*{' [snapshot]' if initial else ''} — {_md_escape(sym)}\n"
                                        f"{acct_line}  ·  OrdType: {ordtype}\n"
                                        f"{_headline_from(lastqty, lastpx)}\n"
                                        f"Px: { _fmt_px(lastpx) }\n"
                                        f"{actor_line}\n"
                                        f"OrderID: `{oid}`\n"
                                        f"Time: {when}"
                                    )
                                )
                        except Exception:
                            pass

                    # 🎯 Fully filled (once)
                    leaves_zero = (leaves == 0 or leaves == "0")
                    if ord_status == "Filled" or leaves_zero:
                        if oid and oid not in filled_announced:
                            q_raw       = cumqty or ordqty or lastqty
                            px_for_view = avgpx or lastpx or limitpx
                            _notify_http(
                                tg_token, chat_id,
                                (
                                    f"🎯 *Order Filled*{' [snapshot]' if initial else ''} — {_md_escape(sym)}\n"
                                    f"{acct_line}  ·  OrdType: {ordtype}\n"
                                    f"{_headline_from(q_raw, px_for_view)}\n"
                                    f"Px: { _fmt_px(px_for_view) }\n"
                                    f"{actor_line}\n"
                                    f"OrderID: `{oid}`\n"
                                    f"Time: {when}"
                                )
                            )
                            filled_announced.add(oid)

            try:
                ws.close()
            except Exception:
                pass

        except WebSocketBadStatusException as e:
            if downtime_start is None:
                downtime_start = time.time()
                retries = 0
            retries += 1
            _notify_http(tg_token, chat_id,
                f"🛑 Talos handshake error: `{e}`\n"
                f"Check WS URL / keys / system clock.\n"
                f"Debug: host={dbg.get('host')} path={dbg.get('path')} ts={dbg.get('ts')}")
            time.sleep(backoff); backoff = min(backoff * 2, 60)
        except Exception as e:
            if downtime_start is None:
                downtime_start = time.time()
                retries = 0
            retries += 1
            _notify_http(tg_token, chat_id, f"🛑 Talos loop error: `{type(e).__name__}: {e}` — reconnecting…")
            time.sleep(backoff); backoff = min(backoff * 2, 60)

# ===== Public API =====
def start_talos_watcher(
    name: str,
    tg_token: str, chat_id: int,
    ws_url: str, api_key: str, api_secret: str,
    subscribe_user: str | None = None,
    exclude_users: set[str] | None = None,
    show_per_exec_fill: bool = False,
    account_label: str | None = None,
    subaccount_filter: set[str] | None = None
):
    """Start (or no-op if running) an independent watcher identified by 'name'."""
    if name in _WATCHERS and _WATCHERS[name]["thread"].is_alive():
        return
    stop_evt = threading.Event()
    th = threading.Thread(
        target=_talos_loop,
        args=(
            stop_evt,
            tg_token, chat_id,
            ws_url, api_key, api_secret,
            subscribe_user,
            (exclude_users or set()),
            show_per_exec_fill,
            account_label,
            (subaccount_filter or set())
        ),
        daemon=True
    )
    _WATCHERS[name] = {"thread": th, "stop": stop_evt}
    th.start()

def stop_talos_watcher(name: str, timeout: float = 5.0):
    w = _WATCHERS.get(name)
    if not w:
        return
    w["stop"].set()
    t = w["thread"]
    if t and t.is_alive():
        t.join(timeout=timeout)
    _WATCHERS.pop(name, None)

def stop_all_talos_watchers(timeout: float = 5.0):
    names = list(_WATCHERS.keys())
    for n in names:
        stop_talos_watcher(n, timeout=timeout)

def list_talos_watchers() -> dict:
    return {k: {"alive": v["thread"].is_alive()} for k, v in _WATCHERS.items()}
