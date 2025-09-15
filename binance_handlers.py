# binance_handlers.py
from html import escape
from telegram import Update
from telegram.ext import ContextTypes
from providers.binance import (
    get_spot_price as bz_spot,
    get_mark_and_funding as bz_funding,
    get_funding_history as bz_funding_hist,
    ms_to_dt_utc as bz_ms_to_dt,
)
from auth import require_auth
from collections import defaultdict

@require_auth
async def bspot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = context.args or []
    coin = (parts[0].upper() if parts else "BTC")
    px = bz_spot(coin)
    if px is None:
        await update.message.chat.send_message(f"Binance {coin} spot unavailable.")
        return
    await update.message.chat.send_message(f"Binance {coin} spot ≈ {px:,.2f} USDT")

@require_auth
async def bfund_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = context.args or []
    coin = (parts[0].upper() if parts else "BTC")
    snap = bz_funding(coin)
    if not snap:
        await update.message.chat.send_message(f"Binance {coin} funding unavailable.")
        return
    nxt = bz_ms_to_dt(snap["nextFundingTime"])
    lr = float(snap['lastFundingRate'])
    daily = lr * 3
    annual = lr * 3 * 365

    txt = (
        f"{snap['symbol']} funding (USDⓈ-M)\n"
        f"Mark: {snap['markPrice']:,.2f}\n"
        f"Index: {snap['indexPrice']:,.2f}\n"
        f"Last funding: {snap['lastFundingRate']*100:.4f}%\n"
        f"≈ {daily*100:.4f}% daily\n"
        f"≈ {annual*100:.2f}% annualized\n"
        f"\n"
        f"Next funding: {nxt.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    await update.message.chat.send_message(f"<pre>{escape(txt)}</pre>", parse_mode="HTML")

@require_auth
async def bfundhist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Two modes:
    1) /bfundhist [coin] [days<=20]
       → show daily funding history (default 10 days, like before)

    2) /bfundhist <coin> <start> - <end|now>
       → calculate cumulative funding % between two dates
    """

    parts = context.args or []
    coin = (parts[0].upper() if parts else "BTC")

    # === Mode 2: absolute cost if "<start> - <end>" present ===
    if len(parts) == 4 and parts[2] == "-":
        start_str, end_str = parts[1], parts[3]

        from datetime import datetime, timezone
        import requests

        def parse_date_to_ms(s: str) -> int:
            if s.lower() == "now":
                return int(datetime.now(timezone.utc).timestamp() * 1000)
            fmts = ["%d%b%y", "%d%b%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
                except Exception:
                    continue
            raise ValueError(f"Unrecognized date: {s}")

        try:
            start_ms = parse_date_to_ms(start_str)
            end_ms = parse_date_to_ms(end_str)
        except Exception as e:
            await update.message.chat.send_message(f"Date parse error: {e}")
            return

        if start_ms > end_ms:
            start_ms, end_ms = end_ms, start_ms

        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        sym = f"{coin}USDT"
        params = {"symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1000}

        rows = []
        while True:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                await update.message.chat.send_message(f"Binance API error {r.status_code}")
                return
            data = r.json()
            if not data:
                break
            rows.extend(data)
            if len(data) < 1000:
                break
            params["startTime"] = data[-1]["fundingTime"] + 1

        if not rows:
            await update.message.chat.send_message(f"No funding data for {coin}.")
            return

        total = sum(float(r["fundingRate"]) for r in rows)
        total_pct = total * 100
        count = len(rows)

        def fmt(ms):
            return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%d-%b-%Y %H:%M UTC")

        msg = (
            f"{coin} funding (Binance {sym})\n"
            f"Range: {fmt(rows[0]['fundingTime'])} → {fmt(rows[-1]['fundingTime'])}\n"
            f"Observations: {count}\n"
            f"Cumulative funding: {total_pct:.4f}%"
        )
        await update.message.chat.send_message(f"<pre>{escape(msg)}</pre>", parse_mode="HTML")
        return

    # === Mode 1: default daily history (old behavior) ===
    try:
        limit_days = int(parts[1]) if len(parts) > 1 else 10
    except Exception:
        limit_days = 10
    limit_days = max(1, min(limit_days, 20))

    rows = bz_funding_hist(coin, limit=limit_days * 3)
    if not rows:
        await update.message.chat.send_message(f"No funding history for {coin}.")
        return

    from collections import defaultdict
    daily_rates = defaultdict(float)
    for r in rows:
        dt = bz_ms_to_dt(r["fundingTime"])
        date_str = dt.strftime("%Y-%m-%d")
        daily_rates[date_str] += float(r["fundingRate"])

    sorted_days = sorted(daily_rates.keys(), reverse=True)[:limit_days]

    lines = [f"{coin} daily funding history:", ""]
    lines.append("Date        Daily Rate   Annualized")
    lines.append("-----------------------------------")
    for d in sorted_days:
        dr = daily_rates[d]
        ann = dr * 365
        lines.append(f"{d}   {dr*100:.4f}%   {ann*100:.2f}%")

    text_block = "\n".join(lines)
    await update.message.chat.send_message(f"<pre>{escape(text_block)}</pre>", parse_mode="HTML")
