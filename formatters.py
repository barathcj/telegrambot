# formatters.py
from datetime import datetime

def _fmt0(x: float | int) -> str:
    return f"{int(round(float(x))):,}"

def _fmt2(x: float) -> str:
    return f"{x:,.2f}"

def _fmt_idx(dt: datetime) -> str:
    return dt.strftime("%d%b%y").upper()

def _index_of(dt: datetime) -> str:
    # identical to _fmt_idx, kept for backward readability
    return dt.strftime("%d%b%y").upper()
