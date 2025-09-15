# futures.py
from html import escape
from datetime import datetime, timezone
from typing import List, Dict
from maths import days_between, annualized_basis
from formatters import _fmt0, _fmt_idx, _index_of
from providers.deribit import (
    get_futures_summaries, get_instruments_map, ms_to_dt_utc
)

def _futures_snapshot(coin: str) -> List[Dict]:
    summaries = get_futures_summaries(coin)
    inst_map = get_instruments_map(coin)
    out = []
    for s in summaries:
        name = s.get("instrument_name")
        mark = s.get("mark_price")
        exp_ms = s.get("expiration_timestamp") or inst_map.get(name)
        if not name or mark is None or not exp_ms:
            continue
        exp = ms_to_dt_utc(int(exp_ms))
        if exp.year < 2020 or name.upper().endswith("PERPETUAL"):
            continue
        out.append({"idx": _index_of(exp), "exp": exp, "mark_r": int(round(float(mark)))})
    uniq = {}
    for r in out:
        uniq[r["idx"]] = r
    rows = list(uniq.values())
    rows.sort(key=lambda x: x["exp"])
    return rows

def render_swap_table(coin: str, spot_int: int, side: str, spread: float) -> tuple[str, str | None]:
    summaries = get_futures_summaries(coin)
    inst_map = get_instruments_map(coin)
    now = datetime.now(timezone.utc)
    summaries.sort(key=lambda r: r.get("expiration_timestamp") or inst_map.get(r.get("instrument_name"), 0))

    rows, deltas = [], []
    for s in summaries:
        name = s.get("instrument_name")
        mark = s.get("mark_price")
        exp_ms = s.get("expiration_timestamp") or inst_map.get(name)
        if not name or mark is None or not exp_ms:
            continue
        exp = ms_to_dt_utc(int(exp_ms))
        if exp.year < 2020 or name.upper().endswith("PERPETUAL"):
            continue
        dte = max(0.0, days_between(now, exp))
        if dte <= 0:
            continue

        fut_r = int(round(float(mark)))
        spt_r = int(round(float(spot_int)))

        fut_adj = fut_r * (1.0 - spread) if side == "b" else fut_r * (1.0 + spread)
        fut_adj_r = int(round(fut_adj))

        apr_adj = annualized_basis(float(spt_r), float(fut_adj_r), dte)
        if apr_adj is None:
            continue

        rows.append({"index": _fmt_idx(exp), 
                     "basis": fut_adj_r - spt_r, 
                     "apr_pct": apr_adj * 100.0, 
                     "tenor": int(round(dte))})
        
        deltas.append({"index": _fmt_idx(exp), 
                       "delta_bps": abs(fut_r - fut_adj_r), 
                       "tenor": int(round(dte))})

    header = ["index", "Basis", "AnnRate(%)", "Tenor"]
    
    def row_to_list(r): return [r["index"], _fmt0(r["basis"]), f"{r['apr_pct']:.2f}", str(r["tenor"])]
    table_rows = [header] + [row_to_list(r) for r in rows]

    col_w = [0] * len(header)
    for rr in table_rows:
        for i, cell in enumerate(rr):
            col_w[i] = max(col_w[i], len(cell))

    def fmt_line(cells): return " | ".join(c.rjust(col_w[i]) for i, c in enumerate(cells))
    sep = "-+-".join("-" * w for w in col_w)

    lines = [f"{coin} spot ≈ {_fmt0(spot_int)} USD", ""]
    lines.append(fmt_line(header))
    lines.append(sep)
    for rr in table_rows[1:]:
        lines.append(fmt_line(rr))
    if len(lines) == 3:
        lines.append("(No futures with usable expiry/mark found.)")
    main_html = "<pre>" + escape("\n".join(lines)) + "</pre>"

    delta_html = None
    if spread != 0.0 and deltas:
        hdr = ["index", "Fwd Spd", "Tenor"]
        data2 = [[d["index"], f"{d['delta_bps']:.1f}", str(d["tenor"])] for d in deltas]
        col_w2 = [len(h) for h in hdr]
        for row in data2:
            for i, cell in enumerate(row):
                col_w2[i] = max(col_w2[i], len(cell))
        def fmt2(cells): return " | ".join(c.rjust(col_w2[i]) for i, c in enumerate(cells))
        sep2 = "-+-".join("-" * w for w in col_w2)
        lines2 = [f"{coin} spread impact (future leg): {int(round(spread*10000))} bps", ""]
        lines2.append(fmt2(hdr))
        lines2.append(sep2)
        for row in data2:
            lines2.append(fmt2(row))
        delta_html = "<pre>" + escape("\n".join(lines2)) + "</pre>"

    return main_html, delta_html

def fs_matrix_table(coin: str) -> str:
    rows = _futures_snapshot(coin)
    if len(rows) < 2:
        return "<pre>No enough futures to compute matrix.</pre>"

    headers = [""] + [r["idx"] for r in rows]
    matrix = []
    for far in rows:
        line = [far["idx"]]
        for near in rows:
            if far["exp"] <= near["exp"]:
                line.append("-")
                continue
            days = (far["exp"] - near["exp"]).total_seconds() / 86400.0
            apr = (far["mark_r"] / near["mark_r"] - 1.0) / (days / 365.0) * 100.0
            line.append(f"{apr:.2f}")
        matrix.append(line)

    table = [headers] + matrix
    col_w = [0] * len(headers)
    for rr in table:
        for i, cell in enumerate(rr):
            col_w[i] = max(col_w[i], len(cell))
    def fmt_line(cells): return " | ".join(c.rjust(col_w[i]) for i, c in enumerate(cells))
    sep = "-+-".join("-" * w for w in col_w)

    lines = [f"{coin} forward–forward (buy near / sell far)", ""]
    lines.append(fmt_line(headers))
    lines.append(sep)
    for rr in matrix:
        lines.append(fmt_line(rr))
    return "<pre>" + escape("\n".join(lines)) + "</pre>"
