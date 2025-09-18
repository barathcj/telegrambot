# talos_handlers.py
from html import escape
from urllib.parse import urlparse
from telegram import Update
from telegram.ext import ContextTypes
from auth import require_auth
from providers.talos_rest import TalosRestClient, OPEN_STATUSES_DEFAULT as OPEN_STATUSES

from config import (
    PRIME_TALOS_WS_URL, PRIME_TALOS_API_KEY, PRIME_TALOS_API_SECRET,
    ASIA_TALOS_WS_URL,  ASIA_TALOS_API_KEY,  ASIA_TALOS_API_SECRET,
)

# ---- Filters & constants ----
EXCLUDE_USERS  = {"BITGO-API"}
ROW_LIMIT_PER_ACCOUNT = 300  # soft cap to avoid huge Telegram messages

# ---- Helpers ----
def _host_only_from_ws(ws_url: str) -> str:
    u = urlparse(ws_url.strip())
    return (u.netloc or ws_url.replace("wss://","").replace("ws://","")).split("/")[0]

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _fmt2(x):
    xf = _to_float(x)
    return "—" if xf is None else f"{xf:,.2f}"

def _abbr_ordtype(ot):
    m = {
        "1": "MKT", "Market": "MKT",
        "2": "LMT", "Limit": "LMT", "LimitAllIn": "LMT-AI",
        "3": "STP", "Stop": "STP",
        "4": "STPL", "StopLimit": "STPL",
        "K": "M->L", "P": "PEG",
    }
    s = (ot or "").strip()
    return m.get(s, s[:8] or "-")

def _detect_algo(d: dict) -> str | None:
    keys = ("Strategy","StrategyName","Algo","AlgoType","Algorithm","ExecutionStrategy","OrderType")
    txt = ""
    for k in keys:
        v = d.get(k)
        if v:
            txt = str(v).upper()
            break
    if not txt:
        return None
    if "TWAP" in txt: return "TWAP"
    if "VWAP" in txt: return "VWAP"
    if "POV" in txt:  return "POV"
    if "ICE" in txt:  return "ICEBERG"
    if "PEG" in txt:  return "PEG"
    return None

def _type_label(d: dict) -> str:
    base = _abbr_ordtype(d.get("OrdType") or d.get("OrderType"))
    algo = _detect_algo(d)
    return base if not algo else f"{base}/{algo}"

def _side_letter(side: str | None) -> str:
    s = (side or "-").strip().lower()
    if s.startswith("b"): return "B"
    if s.startswith("s"): return "S"
    return s[:1].upper() or "-"

COMPACT_NUMBERS = True   # set False to keep full 2dp numbers
ABBR_USERS      = True   # set False to show full names

def _abbr_user(name: str | None) -> str:
    if not name:
        return "-"
    s = str(name).strip()
    # split "name – suffix" or "name - suffix"
    sep = "–" if "–" in s else ("-" if " - " in s else None)
    if sep:
        parts = [p.strip() for p in s.split(sep, 1)]
        base = parts[0]
        suf  = parts[1] if len(parts) > 1 else ""
    else:
        base, suf = s, ""
    # initials from base
    initials = "".join(w[0] for w in base.split() if w and w[0].isalpha()).upper()
    if suf:
        dash = " – " if sep == "–" else " - "
        return f"{initials}{dash}{suf}"
    return initials or base

def _fmt_compact_num(x) -> str:
    try:
        v = float(x)
    except Exception:
        return "—"
    a = abs(v)
    if a >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B"
    if a >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if a >= 1_000:
        return f"{v/1_000:.2f}K"
    # small values: keep your existing 2dp rule
    return _fmt2(v)

def _fit(s: str, width: int, align: str = "left") -> str:
    """Trim with ellipsis if too long, preserving alignment."""
    if width <= 0:
        return ""
    if len(s) <= width:
        return s
    if width == 1:
        return "…"
    return ("…" + s[-(width-1):]) if align == "right" else (s[:width-1] + "…")

def _format_table(label: str, rows: list[dict]) -> str:
    header = f"{label} — {len(rows)} orders"
    if not rows:
        return f"{header}\n(none)"

    # precompute display strings per row
    prepared = []
    for d in rows:
        sym  = str(d.get("Symbol") or "-")
        side = _side_letter(d.get("Side"))
        typ  = _type_label(d)

        # numbers: optionally compact
        qty_val    = d.get("OrderQty")
        px_val     = d.get("Price") or d.get("LimitPx") or d.get("Px")
        leaves_val = d.get("LeavesQty") or d.get("Leaves")

        qty_str    = _fmt_compact_num(qty_val)    if COMPACT_NUMBERS else _fmt2(qty_val)
        px_str     = _fmt2(px_val)                # price stays precise(ish)
        leaves_str = _fmt_compact_num(leaves_val) if COMPACT_NUMBERS else _fmt2(leaves_val)

        user_raw   = (d.get("RequestUser") or d.get("CustomerUser") or d.get("User") or "-")
        user_str   = _abbr_user(user_raw) if ABBR_USERS else str(user_raw)

        prepared.append({
            "SYM": sym, "S": side, "TYPE": typ,
            "QTY": qty_str, "PX": px_str, "LEAVES": leaves_str, "USER": user_str,
        })

    # dynamic widths with sensible caps
    caps = {"SYM": 14, "S": 1, "TYPE": 12, "QTY": 12, "PX": 12, "LEAVES": 12, "USER": 16}
    headers = {"SYM": "SYM", "S": "S", "TYPE": "TYPE", "QTY": "QTY", "PX": "PX", "LEAVES": "LEAVES", "USER": "USER"}

    widths = {}
    for k in headers:
        max_len = max([len(headers[k])] + [len(r[k]) for r in prepared]) if prepared else len(headers[k])
        widths[k] = min(caps[k], max_len)

    # build lines (monospace)
    lines = [header]
    # header row
    head = (
        f"{headers['SYM']:<{widths['SYM']}}  "
        f"{headers['S']:<{widths['S']}}  "
        f"{headers['TYPE']:<{widths['TYPE']}}  "
        f"{headers['QTY']:>{widths['QTY']}}  "
        f"{headers['PX']:>{widths['PX']}}  "
        f"{headers['LEAVES']:>{widths['LEAVES']}}  "
        f"{headers['USER']:<{widths['USER']}}"
    )
    lines.append(head)

    # rows
    shown = 0
    for r in prepared:
        if shown >= ROW_LIMIT_PER_ACCOUNT:
            lines.append(f"... and {len(prepared) - shown} more")
            break
        sym    = _fit(r["SYM"],    widths["SYM"],    "left")
        slet   = _fit(r["S"],      widths["S"],      "left")
        typ    = _fit(r["TYPE"],   widths["TYPE"],   "left")
        qty    = _fit(r["QTY"],    widths["QTY"],    "right")
        px     = _fit(r["PX"],     widths["PX"],     "right")
        leaves = _fit(r["LEAVES"], widths["LEAVES"], "right")
        user   = _fit(r["USER"],   widths["USER"],   "left")

        line = (
            f"{sym:<{widths['SYM']}}  "
            f"{slet:<{widths['S']}}  "
            f"{typ:<{widths['TYPE']}}  "
            f"{qty:>{widths['QTY']}}  "
            f"{px:>{widths['PX']}}  "
            f"{leaves:>{widths['LEAVES']}}  "
            f"{user:<{widths['USER']}}"
        )
        lines.append(line)
        shown += 1

    return "\n".join(lines)

# ---- Telegram handler ----
@require_auth
async def talos_orders_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Optional args: "debug" (no-op here but kept for parity)
    _ = {a.lower() for a in (context.args or [])}

    cfgs = [
        ("BitGo Prime Delaware",
         _host_only_from_ws(PRIME_TALOS_WS_URL),
         PRIME_TALOS_API_KEY, PRIME_TALOS_API_SECRET,
         None),
        ("BitGo Asia",
         _host_only_from_ws(ASIA_TALOS_WS_URL),
         ASIA_TALOS_API_KEY, ASIA_TALOS_API_SECRET,
         ("BitGo SG","BitGo HK")),
    ]

    blocks = []
    for label, host, key, secret, subaccounts in cfgs:
        client = TalosRestClient(host=host, api_key=key, api_secret=secret)
        rows_live = client.list_open_orders(
            statuses=OPEN_STATUSES,
            subaccounts=subaccounts,
                # NOTE: providers.talos_rest already filters LeavesQty>0 & statuses,
                #       and excludes users below:
            exclude_users=EXCLUDE_USERS,
        )
        blocks.append(_format_table(label, rows_live))

    body = "Current working orders\n" + "\n\n".join(blocks)
    await update.message.chat.send_message(f"<pre>{escape(body)}</pre>", parse_mode="HTML")
