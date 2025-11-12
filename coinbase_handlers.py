# coinbase_handlers.py
from html import escape
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Update
from telegram.ext import ContextTypes
from auth import require_auth

from providers.coinbase import (
    get_instrument_details as cb_details,
    get_funding_history   as cb_fhist,
    iso_to_dt_utc,
)


def _pct(x: float, dp: int) -> str:
    return f"{x*100:.{dp}f}%"


@require_auth
async def cbfund_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cbfund [coin|instrument]
    Examples:
      /cbfund btc
      /cbfund BTC-PERP
      /cbfund ETH-USDC
    Output mirrors your Binance /bfund format.
    """
    parts = context.args or []
    user_arg = parts[0] if parts else "BTC"

    info = cb_details(user_arg)
    if not info:
        await update.message.chat.send_message(f"Coinbase INTX {user_arg} funding unavailable.")
        return

    rows = cb_fhist(user_arg, limit=1)
    if not rows:
        await update.message.chat.send_message(f"Coinbase INTX {info['instrument']} has no funding prints yet.")
        return

    last = rows[-1]
    lr = float(last["funding_rate"])  # per interval
    fi_ms = info.get("funding_interval_ms") or 3_600_000
    print (f"{lr:.8f}")
    print (fi_ms)
    periods_per_day = 86_400_000.0 / float(fi_ms)
    daily = lr * periods_per_day
    annual = daily * 365.0

    # next funding time = last event_time + funding_interval
    try:
        t_last = iso_to_dt_utc(last["event_time"])
        next_dt = t_last + timedelta(milliseconds=fi_ms)
        nxt_str = next_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        nxt_str = "N/A"

    lines = [
        f"{info['instrument']} funding (Coinbase INTX)",
        f"Mark: {info['mark_price']:,.2f}" if info['mark_price'] is not None else "Mark: N/A",
        f"Index: {info['index_price']:,.2f}" if info['index_price'] is not None else "Index: N/A",
        f"Last funding: {_pct(lr, 4)}",
        f"≈ {_pct(daily, 4)} daily",
        f"≈ {_pct(annual, 2)} annualized",
        "",
        f"Next funding: {nxt_str}",
    ]

    text = "<pre>" + escape("\n".join(lines)) + "</pre>"
    await update.message.chat.send_message(text, parse_mode="HTML")


@require_auth
async def cbfundhist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cbfundhist [coin|instrument] [days<=20]
    /cbfundhist <coin|instrument> <start> - <end|now>

    Mirrors your Binance /bfundhist:
      • Mode 1: prints last N calendar days (default 10, max 20)
      • Mode 2: absolute date range → cumulative + annualized
    """
    parts = context.args or []
    user_arg = parts[0] if parts else "BTC"

    # --- Mode 2: absolute range (replace your current block) ---
    if len(parts) == 4 and parts[2] == "-":
        start_str, end_str = parts[1], parts[3]

        def parse_date_to_dt(s: str) -> datetime:
            if s.lower() == "now":
                return datetime.now(timezone.utc)
            fmts = ["%d%b%y", "%d%b%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]
            for fmt in fmts:
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue
            raise ValueError(f"Unrecognized date: {s}")

        try:
            dt0 = parse_date_to_dt(start_str)
            dt1 = parse_date_to_dt(end_str)
        except Exception as e:
            await update.message.chat.send_message(f"Date parse error: {e}")
            return

        if dt0 > dt1:
            dt0, dt1 = dt1, dt0  # ensure start <= end

        # paginate until we cover the range or hit a safety max
        all_rows: list[dict] = []
        offset = 0
        PAGE = 100
        SAFETY_MAX = 5000
        while len(all_rows) < SAFETY_MAX:
            batch = cb_fhist(user_arg, limit=PAGE, offset=offset)
            if not batch:
                break
            all_rows.extend(batch)
            # stop early if earliest fetched already precedes dt0
            earliest_dt = iso_to_dt_utc(batch[0]["event_time"])
            if earliest_dt <= dt0:
                break
            offset += PAGE

        if not all_rows:
            await update.message.chat.send_message(f"No funding data for {user_arg}.")
            return

        # ensure global chronological order (across pages), then filter
        all_rows.sort(key=lambda r: r["event_time"])
        rows = [r for r in all_rows if dt0 <= iso_to_dt_utc(r["event_time"]) <= dt1]
        if not rows:
            await update.message.chat.send_message(f"No funding data for {user_arg} in range.")
            return

        total = sum(float(r["funding_rate"]) for r in rows)  # cumulative decimal
        total_pct = total * 100.0
        count = len(rows)

        # time bounds (ordered)
        t_first = iso_to_dt_utc(rows[0]["event_time"])
        t_last  = iso_to_dt_utc(rows[-1]["event_time"])

        # interval (seconds), for robust span fallback
        info = cb_details(user_arg) or {}
        fi_sec = float(info.get("funding_interval_seconds") or 3600.0)
        interval_h = fi_sec / 3600.0

        # robust wall-clock span: use the larger of (t_last - t_first) and (count * interval)
        span_sec = max((t_last - t_first).total_seconds(), count * fi_sec)
        wall_days = span_sec / 86400.0

        apr_simple = total * (365.0 / wall_days)

        def fmt_dt(dt: datetime) -> str:
            return dt.strftime("%d-%b-%Y %H:%M UTC")

        msg = (
            f"{user_arg} funding (Coinbase {info.get('instrument', user_arg)})\n"
            f"Range: {fmt_dt(t_first)} → {fmt_dt(t_last)}\n"
            f"Observations: {count}: {int(round(interval_h))}h interval\n"
            f"Cumulative funding: {total_pct:.4f}%\n"
            f"Days: {wall_days:.2f}\n"
            f"Annualized (simple): {apr_simple*100:.2f}%"
        )
        text = "<pre>" + escape(msg) + "</pre>"
        await update.message.chat.send_message(text, parse_mode="HTML")
        return

    # --- Mode 1: rolling daily history (default path, mirrors Binance handler) ---
    try:
        limit_days = int(parts[1]) if len(parts) > 1 else 10
    except Exception:
        limit_days = 10
    limit_days = max(1, min(limit_days, 20))

    rows = cb_fhist(user_arg, limit=limit_days * 24)
    if not rows:
        await update.message.chat.send_message(f"No funding history for {user_arg}.")
        return

    daily_rates = defaultdict(float)
    for r in rows:
        try:
            dt = iso_to_dt_utc(r["event_time"])
        except Exception:
            continue
        date_str = dt.strftime("%Y-%m-%d")
        try:
            daily_rates[date_str] += float(r["funding_rate"])
        except Exception:
            continue

    if not daily_rates:
        await update.message.chat.send_message(f"No funding history for {user_arg}.")
        return

    sorted_days = sorted(daily_rates.keys(), reverse=True)[:limit_days]
    lines = [f"{user_arg.upper()} daily funding history:", ""]
    lines.append("Date        Daily Rate   Annualized")
    lines.append("-----------------------------------")
    for d in sorted_days:
        dr = daily_rates[d]
        ann = dr * 365
        lines.append(f"{d}   {dr*100:.4f}%   {ann*100:.2f}%")

    text_block = "\n".join(lines)
    await update.message.chat.send_message(f"<pre>{escape(text_block)}</pre>", parse_mode="HTML")
