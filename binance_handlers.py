# binance_handlers.py
from html import escape
from telegram import Update
from telegram.ext import ContextTypes
from providers.binance import (
    get_spot_price as bz_spot,
    get_mark_and_funding as bz_funding,
    get_funding_history as bz_funding_hist,
    ms_to_dt_utc as bz_ms_to_dt,
    get_funding_series as bz_funding_series,
    get_funding_info as bz_funding_info,
    get_book_ticker as bz_book,
)
from auth import require_auth
from collections import defaultdict, Counter

import math
from statistics import mean
from typing import Tuple
import os

def _infer_interval_hours(times_ms: list[int]) -> float:
    """
    Infer Binance funding interval (in hours) from consecutive fundingTime stamps.
    We bin to nearest among common Binance values {1, 4, 8} with a safe fallback.
    """
    diffs_h = [(t2 - t1) / 3_600_000.0 for t1, t2 in zip(times_ms, times_ms[1:]) if t2 > t1]
    if not diffs_h:
        return 8.0  # sensible default
    CANDIDATES = [1.0, 4.0, 8.0]
    binned = [min(CANDIDATES, key=lambda c: abs(x - c)) for x in diffs_h]
    most_common, _ = Counter(binned).most_common(1)[0]
    return float(most_common)


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
        f"Mark: {float(snap['markPrice']):,.2f}\n"
        f"Index: {float(snap['indexPrice']):,.2f}\n"
        f"Last funding: {float(snap['lastFundingRate'])*100:.4f}%\n"
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
       → calculate cumulative funding % between two dates, infer the symbol's funding interval,
         and annualize based on that interval.
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

        total = sum(float(r["fundingRate"]) for r in rows)   # cumulative (decimal)
        total_pct = total * 100.0
        count = len(rows)

        def fmt(ms):
            return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%d-%b-%Y %H:%M UTC")

        # --- event-range (wall-clock) span shown to users ---
        t0_ms = rows[0]['fundingTime']
        t1_ms = rows[-1]['fundingTime']
        wall_days = (t1_ms - t0_ms) / 86_400_000.0
        if wall_days <= 0:
            wall_days = 1e-9  # guard against divide-by-zero if API returns a single timestamp

        # --- infer interval just to display it (1h/4h/8h typical on Binance) ---
        times = [r["fundingTime"] for r in rows]
        interval_h = _infer_interval_hours(times)

        # --- annualize using the same denominator as the "Days" line ---
        apr_simple = total * (365.0 / wall_days)

        msg = (
            f"{coin} funding (Binance {sym})\n"
            f"Range: {fmt(t0_ms)} → {fmt(t1_ms)}\n"
            f"Observations: {count}: {int(interval_h)}h interval\n"
            f"Cumulative funding: {total_pct:.4f}%\n"
            f"Days: {wall_days:.2f}\n"
            f"Annualized (simple): {apr_simple*100:.2f}%"
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
        dr = daily_rates[d]           # sum of windows that day (decimal)
        ann = dr * 365                # simple annualization from daily total
        lines.append(f"{d}   {dr*100:.4f}%   {ann*100:.2f}%")

    text_block = "\n".join(lines)
    await update.message.chat.send_message(f"<pre>{escape(text_block)}</pre>", parse_mode="HTML")


def _parse_tenor_days(s: str) -> int:
    """
    Accepts '30d', '2w', '1m' (30d), '90D', etc. Default 30d if not parsable.
    """
    if not s:
        return 30
    s = s.strip().lower()
    num = ""
    unit = "d"
    for ch in s:
        if ch.isdigit():
            num += ch
        else:
            unit = ch
            break
    try:
        n = int(num) if num else 30
    except Exception:
        n = 30
    if unit == "w":
        return n * 7
    if unit == "m":
        return n * 30
    return n  # default days


def _ewma_per_interval(vals: list[float], half_life_days: float, periods_per_day: float) -> float:
    """
    EWMA over per-interval funding rates.
    vals must be chronological; use last value as 'current'.
    """
    if not vals:
        return 0.0
    hl_steps = max(1.0, half_life_days * periods_per_day)
    lam = math.exp(-math.log(2.0) / hl_steps)
    a = 1.0 - lam
    m = vals[0]
    for x in vals[1:]:
        m = lam * m + a * x
    return m


def _rmse(pred: list[float], y: list[float]) -> float:
    if not pred or len(pred) != len(y):
        return float("inf")
    se = [(p - yy) ** 2 for p, yy in zip(pred, y)]
    return math.sqrt(mean(se)) if se else float("inf")


def _blend_weights_from_backtest(series: list[float], periods_per_day: float) -> Tuple[float, float, float]:
    """
    Compute inverse-RMSE weights for predictors: NOW (lag-1), 7d-EWMA, 30d-EWMA.
    Backtest one-step-ahead on the last ~180 days (cap by series length).
    Fallback to 0.50/0.30/0.20 if insufficient data.
    """
    n = len(series)
    if n < 60:  # need some history
        return (0.50, 0.30, 0.20)

    # build features at t-1 predicting f_t
    preds_now, preds_7, preds_30, actual = [], [], [], []
    # precompute rolling EWMAs
    m7 = [series[0]]
    m30 = [series[0]]
    lam7 = math.exp(-math.log(2.0) / max(1.0, 7 * periods_per_day))
    lam30 = math.exp(-math.log(2.0) / max(1.0, 30 * periods_per_day))
    a7, a30 = 1 - lam7, 1 - lam30

    for i in range(1, n):
        # update EWMAs up to i-1 (so they don't peek at f_i)
        m7.append(lam7 * m7[-1] + a7 * series[i - 1])
        m30.append(lam30 * m30[-1] + a30 * series[i - 1])

        preds_now.append(series[i - 1])
        preds_7.append(m7[-1])
        preds_30.append(m30[-1])
        actual.append(series[i])

    rmse_now = _rmse(preds_now, actual)
    rmse_7 = _rmse(preds_7, actual)
    rmse_30 = _rmse(preds_30, actual)

    eps = 1e-9
    inv = [1.0 / (rmse_now ** 2 + eps), 1.0 / (rmse_7 ** 2 + eps), 1.0 / (rmse_30 ** 2 + eps)]
    s = sum(inv)
    if s <= 0 or not math.isfinite(s):
        return (0.50, 0.30, 0.20)
    w_now, w7, w30 = [x / s for x in inv]

    # regime boost: if current deviates > 2σ of last 30d, up-weight NOW modestly
    last_30 = series[-int(min(n, 30 * periods_per_day)):]
    if last_30:
        mu = mean(last_30)
        sigma = (mean([(x - mu) ** 2 for x in last_30]) ** 0.5) if len(last_30) > 1 else 0.0
        if sigma > 0 and abs(series[-1] - mu) > 2.0 * sigma:
            w_now *= 1.5
            s2 = w_now + w7 + w30
            w_now, w7, w30 = w_now / s2, w7 / s2, w30 / s2

    return (w_now, w7, w30)

C_MARGIN_ANN = float(os.getenv("BINANCE_MARGIN_ANN", "0.00"))  # annualized decimal, e.g. "0.02" for 2%

@require_auth
async def bcurve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /bcurve <coin> <tenor>
    Example: /bcurve sei 30d
    Shows: current, 7d EWMA, 30d EWMA funding (per interval + annualized),
           blended rate, worst/best 30D from Binance caps (if available),
           spot index and spot bid/ask.
    """
    parts = context.args or []
    coin = (parts[0].upper() if parts else "BTC")
    tenor_days = _parse_tenor_days(parts[1]) if len(parts) > 1 else 30
    T_years = tenor_days / 365.0

    # --- data pulls ---
    snap = bz_funding(coin)            # mark/index/lastFundingRate + nextFundingTime
    if not snap:
        await update.message.chat.send_message(f"Binance {coin} data unavailable.")
        return

    series = bz_funding_series(coin, limit=1000)  # per-interval funding series (chronological)
    if not series:
        await update.message.chat.send_message(f"No funding series for {coin}.")
        return

    times = [r["fundingTime"] for r in series]
    frates = [float(r["fundingRate"]) for r in series]  # per-interval decimals

    # infer interval (1h/4h/8h typical)
    interval_h = _infer_interval_hours(times)
    periods_per_day = max(1.0, 24.0 / float(interval_h))

    # compute EWMAs (per interval)
    f_now = float(snap.get("lastFundingRate", 0.0))           # last settled per-interval
    f_7 = _ewma_per_interval(frates, half_life_days=7.0, periods_per_day=periods_per_day)
    f_30 = _ewma_per_interval(frates, half_life_days=30.0, periods_per_day=periods_per_day)

    # blend weights (inverse-RMSE on one-step-ahead)
    w_now, w7, w30 = _blend_weights_from_backtest(frates, periods_per_day)
    f_blend = w_now * f_now + w7 * f_7 + w30 * f_30

    # annualized (simple) for intuition
    ann = lambda per_int: per_int * periods_per_day * 365.0
    f_now_ann, f7_ann, f30_ann, fblend_ann = map(ann, [f_now, f_7, f_30, f_blend])

    # expected cumulative over tenor (linear comp ok for small rates)
    Phi_expected = f_blend * periods_per_day * float(tenor_days)

    # worst/best from fundingInfo if available
    finfo = bz_funding_info(coin) or {}
    cap = finfo.get("fundingRateCap", None)
    floor = finfo.get("fundingRateFloor", None)
    fi_interval_h = finfo.get("fundingIntervalHours", None)
    wc_txt = "N/A"
    bc_txt = "N/A"
    if cap is not None and floor is not None:
        use_h = float(fi_interval_h) if fi_interval_h else float(interval_h)
        ppd = max(1.0, 24.0 / use_h)
        N = int(round(ppd * tenor_days))
        # short-perp hedge: worst-case is paying at floor (negative), best-case receiving at cap
        Phi_worst = float(floor) * N
        Phi_best = float(cap) * N
        wc_txt = f"{Phi_worst*100:+.2f}%"
        bc_txt = f"{Phi_best*100:+.2f}%"

    # spot + book
    index_px = float(snap.get("indexPrice", 0.0))
    ba = bz_book(coin) or {}
    bid = ba.get("bid", None)
    ask = ba.get("ask", None)

    F_mid = index_px * math.exp(-Phi_expected + C_MARGIN_ANN * T_years)

    # pretty print
    lines = []
    lines.append(f"{coin} {tenor_days}D Funding Curve (Hedge: Binance USDⓈ-M Perp)")
    lines.append("")
    lines.append(f"Spot Index: {index_px:,.6f} USDT")

    lines.append("")
    lines.append(f"Funding per interval (≈ every {int(interval_h)}h):")
    lines.append(f"• Current (last settled): {f_now*100:+.4f}%   | Ann ≈ {f_now_ann*100:+.2f}%")
    lines.append(f"• 7D EWMA:               {f_7*100:+.4f}%   | Ann ≈ {f7_ann*100:+.2f}%")
    lines.append(f"• 30D EWMA:              {f_30*100:+.4f}%   | Ann ≈ {f30_ann*100:+.2f}%")
    lines.append(f"→ Blended:               {f_blend*100:+.4f}%   | Ann ≈ {fblend_ann*100:+.2f}%")
    lines.append("")
    # worst/best using adjusted caps if present; otherwise stays N/A (symbol had no adjustments)
    if cap is not None and floor is not None:
        lines.append("Symbol guardrails (Binance, adjusted):")
        lines.append(f"• Cap/Floor per settlement: {cap*100:+.2f}% / {floor*100:+.2f}%")
        lines.append(f"• Settlement interval:       {int(fi_interval_h or interval_h)}h")
        lines.append("")
    lines.append(f"{tenor_days}D cumulative funding (short-perp):")
    lines.append(f"• Expected (blended): {Phi_expected*100:+.2f}%")
    lines.append(f"• Worst-case (pay):   {wc_txt}")
    lines.append(f"• Best-case (receive):{bc_txt}")
    lines.append("")
    lines.append(f"{tenor_days}D Forward Mid (c_margin={C_MARGIN_ANN*100:.2f}% p.a.): {F_mid:,.6f} USDT")

    txt = "\n".join(lines)
    await update.message.chat.send_message(f"<pre>{escape(txt)}</pre>", parse_mode="HTML")
