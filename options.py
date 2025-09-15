# options.py
from html import escape
from datetime import datetime, timezone
from typing import List
from config import STRIKES_PER_EXPIRY, MIN_COIN_PREMIUM, MAX_LEN, EXPIRIES_PER_MESSAGE
from formatters import _fmt2
from providers.deribit import (
    get_index_price, get_option_summaries, get_option_instruments_map, ms_to_dt_utc
)

def render_option_chain_sections(coin: str, side: str, opt_type: str, spread: float) -> List[str]:
    spot = get_index_price(coin)
    if spot is None:
        return [f"Couldn't fetch {coin} spot."]

    summaries = get_option_summaries(coin)
    now = datetime.now(timezone.utc)
    inst_map = get_option_instruments_map(coin)

    grouped: dict[str, list[tuple[float, float, float, float, int]]] = {}

    for s in summaries:
        name = s.get("instrument_name")
        if not name:
            continue
        exp_ms = inst_map.get(name)
        if not exp_ms:
            continue
        exp_dt = ms_to_dt_utc(exp_ms)
        dte = (exp_dt - now).days
        if dte <= 2 or exp_dt.weekday() != 4:
            continue

        parts = name.split("-")
        if len(parts) < 4:
            continue
        strike = float(parts[2])
        typ = parts[3].upper()
        if typ != opt_type:
            continue

        if typ == "C" and strike <= spot:
            continue
        if typ == "P" and strike >= spot:
            continue

        bid = s.get("bid_price")
        ask = s.get("ask_price")
        mark = s.get("mark_price")

        if side == "sell":
            ref_px = bid if bid is not None else mark
            if ref_px is None:
                continue
            coin_px = float(ref_px) * (1.0 - spread)
        else:
            ref_px = ask if ask is not None else mark
            if ref_px is None:
                continue
            coin_px = float(ref_px) * (1.0 + spread)

        coin_disp = round(float(coin_px), 4)
        if coin_disp < MIN_COIN_PREMIUM:
            continue

        usd_disp = round(coin_disp * float(spot), 2)
        ay_pct = (coin_disp / (dte / 365.0)) * 100.0 if (side == "sell" and dte > 0) else 0.0

        exp_str = exp_dt.strftime("%d-%b-%y")
        grouped.setdefault(exp_str, []).append((strike, ay_pct, usd_disp, coin_disp, int(dte)))

    label = "Call" if opt_type == "C" else "Put"
    header_lines = [
        f"Spot Ref: {spot:,.2f} (As of: {now.strftime('%Y-%m-%d %H:%M:%S %Z')})",
        f"Coin: {coin}",
        f"Direction: {side.capitalize()}",
        f"Option type: {label}",
    ]
    sections: list[str] = ["\n".join(header_lines)]

    if not grouped:
        sections[0] += "\nNo matching options found."
        return sections

    for exp in sorted(grouped.keys(), key=lambda x: datetime.strptime(x, "%d-%b-%y")):
        rows = grouped[exp]
        if opt_type == "C":
            rows.sort(key=lambda r: r[0])
        else:
            rows.sort(key=lambda r: -r[0])
        rows = rows[:STRIKES_PER_EXPIRY]
        if not rows:
            continue

        sec_lines: list[str] = []
        if side == "sell":
            sec_lines.append(f"{exp} (Implied Ann. Yield | Price Per {coin})")
        else:
            sec_lines.append(f"{exp} (Price Per {coin})")
        sec_lines.append(f"Expiring in: {rows[0][4]} Days")

        for strike, ay_pct, usd_px, coin_px, _ in rows:
            if side == "sell":
                sec_lines.append(f"{int(strike)} {label}: {ay_pct:.2f}% | {_fmt2(usd_px)} USD | {coin_px:.4f}")
            else:
                sec_lines.append(f"{int(strike)} {label}: {_fmt2(usd_px)} USD | {coin_px:.4f}")

        sections.append("\n".join(sec_lines))

    return sections

def pack_sections_into_messages(sections: list[str]) -> list[str]:
    if not sections:
        return []
    header, expiries = sections[0], sections[1:]

    messages: list[str] = []
    current = header
    count_in_current = 0

    def flush():
        nonlocal current, count_in_current
        if current:
            messages.append(current)
        current = ""
        count_in_current = 0

    for sec in expiries:
        if len(sec) > MAX_LEN:
            if current:
                flush()
            for i in range(0, len(sec), MAX_LEN):
                messages.append(sec[i:i+MAX_LEN])
            continue

        cap = EXPIRIES_PER_MESSAGE
        proposed = f"{current}\n\n{sec}" if current else sec

        if (count_in_current >= cap) or (len(proposed) > MAX_LEN):
            flush()
            current = sec
            count_in_current = 1
        else:
            current = proposed
            count_in_current += 1

    if current:
        flush()
    return messages
