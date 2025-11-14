# deribit_handlers.py
from html import escape
from telegram import Update
from telegram.ext import ContextTypes
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from providers.deribit import (
    get_index_price,
    get_perpetual_summary,
    get_funding_rate_history,
    ms_to_dt_utc,
    InstrumentNotOpenError,
)
from futures import fs_matrix_table, render_swap_table
from options import render_option_chain_sections, pack_sections_into_messages
from auth import require_auth

@require_auth
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("hey! i’m online. try /ping, /spot, /bsbtc 30")

@require_auth
async def talos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("Talos Connected!")

@require_auth
async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("pong")

@require_auth
async def spot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = context.args or []
    ccy = (parts[0].upper() if parts else "BTC")
    price = get_index_price(ccy)
    if price is None:
        await update.message.chat.send_message(f"Couldn't fetch {ccy} index right now.")
        return
    await update.message.chat.send_message(f"{ccy} spot ≈ {price:,.2f} USD")

async def _run_swap(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, coin: str):

    #Parse inputs
    parts = context.args or []
    raw = (parts[0] if parts else "0").strip()
    spread = float(raw)/10000.0 if raw.isdigit() else 0.0

    #Get spot price
    spot = get_index_price(coin)
    if spot is None:
        await update.message.chat.send_message(f"Couldn't fetch {coin} spot.")
        return
    spot_int = int(round(float(spot)))

    #Get future price
    main_html, delta_html = render_swap_table(coin, spot_int, side, spread)
    await update.message.chat.send_message(main_html, parse_mode="HTML")
    if delta_html:
        await update.message.chat.send_message(delta_html, parse_mode="HTML")

@require_auth
async def bsbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="b", coin="BTC")

@require_auth
async def ssbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="s", coin="BTC")

@require_auth
async def bseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="b", coin="ETH")

@require_auth
async def sseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="s", coin="ETH")

@require_auth
async def fsbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    html = fs_matrix_table("BTC")
    await update.message.chat.send_message(html, parse_mode="HTML")

@require_auth
async def fseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    html = fs_matrix_table("ETH")
    await update.message.chat.send_message(html, parse_mode="HTML")

@require_auth
async def option_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text[1:]
    parts = raw.split()
    cmd = parts[0]
    arg = parts[1] if len(parts) > 1 else "0"
    try:
        spread = float(arg) / 100.0
    except ValueError:
        spread = 0.0
    side = "sell" if cmd[0].lower() == "s" else "buy"
    opt_type = "C" if cmd[1].lower() == "c" else "P"
    coin = "ETH" if "eth" in cmd else "BTC"
    sections = render_option_chain_sections(coin, side, opt_type, spread)
    messages = pack_sections_into_messages(sections)
    chat = update.message.chat
    for msg in messages:
        await chat.send_message(text=f"<pre>{escape(msg)}</pre>", parse_mode="HTML")


def _format_pct(x: float, dp: int = 4) -> str:
    return f"{x*100:.{dp}f}%"

def _resolve_perp_targets(raw: str | None) -> tuple[str, str, list[str]]:
    """
    Returns (display_label, base_coin, instrument_candidates)
    """
    s = (raw or "BTC").strip().upper()
    s = s.replace("/", "_").replace(" ", "")
    display = s
    if s.endswith("-PERPETUAL"):
        core = s[:-len("-PERPETUAL")]
    else:
        core = s
    # normalize repeated separators
    core = core.replace("--", "-").replace("__", "_")
    core = core.strip("-_")
    if not core:
        core = "BTC"
    if "_" in core:
        base = core.split("_", 1)[0]
    else:
        base = core.split("-", 1)[0]
    if "_" not in display and "-" not in display:
        display = base

    candidates: list[str] = []

    def add_candidate(name: str):
        name = name.replace("--", "-").replace("__", "_").strip()
        if name and name not in candidates:
            candidates.append(name)

    if s.endswith("-PERPETUAL"):
        add_candidate(s)
    else:
        # primary guess (keep underscores between base/quote if provided)
        add_candidate(f"{core}-PERPETUAL")
    # Always attempt classic inverse plus USDC linear variants.
    add_candidate(f"{base}-PERPETUAL")
    add_candidate(f"{base}_USDC-PERPETUAL")

    return (display, base, candidates)

def _iter_open_perp_summaries(candidates: list[str]):
    """
    Yields (instrument_name, summary) pairs for instruments that appear open.
    """
    for inst in candidates:
        summary = get_perpetual_summary(inst)
        if not summary:
            continue
        state = str(summary.get("state") or "").lower()
        if state and state not in {"open", "live"}:
            continue
        resolved = str(summary.get("instrument_name") or inst).upper()
        yield resolved, summary

@require_auth
async def dfund_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = context.args or []
    display, _base, instruments = _resolve_perp_targets(parts[0] if parts else "BTC")

    summary = None
    instrument_used = None
    for inst_name, inst_summary in _iter_open_perp_summaries(instruments):
        summary = inst_summary
        instrument_used = inst_name
        break

    if not summary or not instrument_used:
        await update.message.chat.send_message(f"Deribit {display} funding unavailable.")
        return

    mark = summary.get("mark_price")
    index = summary.get("index_price")

    def _per_interval() -> float:
        val = summary.get("funding_8h")
        try:
            return float(val) if val is not None else 0.0
        except Exception:
            return 0.0

    per_interval = _per_interval()
    interval_hours = 8.0  # Deribit funding is every 8h
    periods_per_day = 24.0 / interval_hours
    daily = per_interval * periods_per_day
    annual = daily * 365.0

    lines = [
        f"{instrument_used} funding (Deribit)",
        f"Mark: {mark:,.2f}" if mark is not None else "Mark: N/A",
        f"Index: {index:,.2f}" if index is not None else "Index: N/A",
        f"Last funding: {_format_pct(per_interval, 4)}",
        f"≈ {_format_pct(daily, 4)} daily",
        f"≈ {_format_pct(annual, 2)} annualized",
    ]

    text = "<pre>" + escape("\n".join(lines)) + "</pre>"
    await update.message.chat.send_message(text, parse_mode="HTML")

@require_auth
async def dfundhist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /dfundhist [coin] [days<=20]
    /dfundhist <coin> <start> - <end|now>
    """
    parts = context.args or []
    display, _base, instruments = _resolve_perp_targets(parts[0] if parts else "BTC")

    def _parse_date(s: str) -> datetime:
        if s.lower() == "now":
            return datetime.now(timezone.utc)
        fmts = ["%d%b%y", "%d%b%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]
        for fmt in fmts:
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
        raise ValueError(f"Unrecognized date: {s}")

    def _pick_history(fetch_rows):
        had_summary = False
        for inst_name, inst_summary in _iter_open_perp_summaries(instruments):
            had_summary = True
            try:
                rows = fetch_rows(inst_name)
            except InstrumentNotOpenError:
                continue
            if rows:
                return inst_name, inst_summary, rows, had_summary
        return None, None, [], had_summary

    # Absolute range mode
    if len(parts) == 4 and parts[2] == "-":
        try:
            dt0 = _parse_date(parts[1])
            dt1 = _parse_date(parts[3])
        except Exception as e:
            await update.message.chat.send_message(f"Date parse error: {e}")
            return
        if dt0 > dt1:
            dt0, dt1 = dt1, dt0
        def _fetch(inst_name: str):
            return get_funding_rate_history(inst_name, start_ms=int(dt0.timestamp()*1000), end_ms=int(dt1.timestamp()*1000))

        instrument_used, summary, rows, had_summary = _pick_history(_fetch)
        if not rows:
            if not had_summary:
                await update.message.chat.send_message(f"Deribit {display} funding unavailable.")
            else:
                await update.message.chat.send_message(f"No funding data for {display}.")
            return

        total = sum(r["funding_rate"] for r in rows)
        t_first = ms_to_dt_utc(rows[0]["timestamp"])
        t_last = ms_to_dt_utc(rows[-1]["timestamp"])
        wall_days = max((t_last - t_first).total_seconds() / 86400.0, 1e-9)
        apr_simple = total * (365.0 / wall_days)

        msg = (
            f"{instrument_used} funding (Deribit)\n"
            f"Range: {t_first.strftime('%d-%b-%Y %H:%M UTC')} → {t_last.strftime('%d-%b-%Y %H:%M UTC')}\n"
            f"Observations: {len(rows)}: 8h interval\n"
            f"Cumulative funding: {_format_pct(total, 4)}\n"
            f"Days: {wall_days:.2f}\n"
            f"Annualized (simple): {_format_pct(apr_simple, 2)}"
        )
        await update.message.chat.send_message(f"<pre>{escape(msg)}</pre>", parse_mode="HTML")
        return

    # Rolling daily history
    try:
        limit_days = int(parts[1]) if len(parts) > 1 else 10
    except Exception:
        limit_days = 10
    limit_days = max(1, min(limit_days, 20))

    now = datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(days=limit_days + 1)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    def _fetch(inst_name: str):
        return get_funding_rate_history(inst_name, start_ms=cutoff_ms, end_ms=end_ms)

    instrument_used, _summary, rows, had_summary = _pick_history(_fetch)
    if not rows:
        if not had_summary:
            await update.message.chat.send_message(f"Deribit {display} funding unavailable.")
        else:
            await update.message.chat.send_message(f"No funding history for {display}.")
        return

    daily = defaultdict(float)
    for r in rows:
        if r["timestamp"] < cutoff_ms:
            continue
        dt = ms_to_dt_utc(r["timestamp"])
        date_str = dt.strftime("%Y-%m-%d")
        daily[date_str] += r["funding_rate"]

    if not daily:
        await update.message.chat.send_message(f"No funding history for {display}.")
        return

    sorted_days = sorted(daily.keys(), reverse=True)[:limit_days]
    lines = [f"{instrument_used} daily funding history (Deribit):", ""]
    lines.append("Date        Daily Rate   Annualized")
    lines.append("-----------------------------------")
    for d in sorted_days:
        dr = daily[d]
        ann = dr * 365.0
        lines.append(f"{d}   {dr*100:.4f}%   {ann*100:.2f}%")

    body = "\n".join(lines)
    await update.message.chat.send_message(f"<pre>{escape(body)}</pre>", parse_mode="HTML")
