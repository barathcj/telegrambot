# providers/coinbase.py
import requests
from datetime import datetime, timezone
from typing import Optional, List, Dict

INTX_BASE = "https://api.international.coinbase.com"


def _get(url: str, params: Dict | None = None, timeout: int = 8):
    """Lightweight GET with basic error logging."""
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        try:
            print("Coinbase HTTPError:", e.response.status_code, e.response.text)
        except Exception:
            print("Coinbase HTTPError (no body)")
        return None
    except Exception as e:
        print("Coinbase request error:", e)
        return None


def iso_to_dt_utc(s: str) -> datetime:
    """Parse ISO8601 '...Z' into timezone-aware UTC datetime."""
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def _normalize_interval_seconds(val) -> float:
    """
    Normalize Coinbase INTX funding_interval to seconds.
    Heuristic by magnitude:
      >= 1e11  → nanoseconds
      >= 1e7   → microseconds
      >= 1e3   → milliseconds
      else     → seconds
    """
    if val is None:
        return 3600.0  # default 1 hour
    v = float(val)
    if v >= 1e11:   # ns
        return v / 1e9
    if v >= 1e7:    # µs
        return v / 1e6
    if v >= 1e3:    # ms
        return v / 1e3
    return v        # seconds

# ---------- Instrument resolution ----------

def resolve_instrument(user_arg: str | None) -> Optional[str]:
    """
    Resolve user input to a valid INTX instrument path token.
    Accepts: 'BTC', 'BTC-PERP', 'BTC-USDC' (name) — or falls back to listing.
    """
    s = (user_arg or "BTC").strip().upper()
    candidates: List[str] = []

    if "-" in s:
        # user already passed a name like BTC-PERP or BTC-USDC
        candidates.append(s)
    else:
        # try common perpetual names seen in docs
        candidates += [f"{s}-PERP", f"{s}-USDC"]

    # probe candidates quickly via funding endpoint (limit=1)
    for inst in candidates:
        if _has_funding(inst):
            return inst

    # fallback: list instruments; pick the PERP for this base (prefer USDC-quoted)
    lst = _get(f"{INTX_BASE}/api/v1/instruments")
    if isinstance(lst, list):
        # try an exact base match first
        for it in lst:
            try:
                if (it.get("type") == "PERP"
                        and (it.get("base_asset_name", "") or it.get("base_asset", "")).upper() == s):
                    sym = it.get("symbol")
                    if isinstance(sym, str) and sym:
                        return sym
                    # last resort use the instrument_id
                    iid = it.get("instrument_id")
                    if iid is not None:
                        return str(iid)
            except Exception:
                continue
        # otherwise any PERP starting with base name
        for it in lst:
            try:
                sym = it.get("symbol") or ""
                if it.get("type") == "PERP" and sym.upper().startswith(s + "-"):
                    return sym
            except Exception:
                continue

    # absolute last resort: first candidate
    return candidates[0] if candidates else None


def _has_funding(instrument: str) -> bool:
    """Return True if the instrument's funding endpoint yields at least one row."""
    res = _get(f"{INTX_BASE}/api/v1/instruments/{instrument}/funding", {"result_limit": 1})
    rows = _normalize_funding_rows(res)
    return bool(rows)


# ---------- Core fetchers ----------

def get_instrument_details(inst_or_coin: str = "BTC") -> Optional[dict]:
    """
    GET /api/v1/instruments/{instrument}
    Returns { instrument, index_price, mark_price, funding_interval_ms }.
    """
    instrument = resolve_instrument(inst_or_coin)
    if not instrument:
        return None
    res = _get(f"{INTX_BASE}/api/v1/instruments/{instrument}")
    if not isinstance(res, dict):
        return None
    try:
        q = res.get("quote") or {}
        fi_raw = res.get("funding_interval")
        out = {
            "instrument": instrument,
            "index_price": float(q["index_price"]) if q.get("index_price") is not None else None,
            "mark_price":  float(q["mark_price"])  if q.get("mark_price")  is not None else None,
            "funding_interval_seconds": _normalize_interval_seconds(fi_raw),
        }
        return out
    except Exception:
        return None


def _normalize_funding_rows(res) -> List[dict]:
    """
    Normalize the historical funding response to a list of rows:
    [{event_time, funding_rate, mark_price}] (chronological ascending).
    """
    if isinstance(res, list):
        raw = res
    elif isinstance(res, dict):
        # try common wrappers
        raw = None
        for k in ("funding_rates", "data", "items", "results"):
            if isinstance(res.get(k), list):
                raw = res[k]
                break
        if raw is None:
            # single-object case (seen in some examples)
            if all(k in res for k in ("funding_rate", "event_time")):
                raw = [res]
            else:
                return []
    else:
        return []

    out: List[dict] = []
    for row in raw:
        try:
            out.append({
                "event_time": row["event_time"],                 # ISO8601
                "funding_rate": float(row["funding_rate"]),      # per-interval (decimal)
                "mark_price": float(row["mark_price"]),
            })
        except Exception:
            continue

    out.sort(key=lambda r: r["event_time"])
    return out


def get_funding_history(inst_or_coin: str = "BTC", limit: int = 30, offset: int = 0) -> List[dict]:
    """
    GET /api/v1/instruments/{instrument}/funding?result_limit=&result_offset=
    Returns a chronological list of funding rows (oldest → newest).
    """
    instrument = resolve_instrument(inst_or_coin)
    if not instrument:
        return []
    limit = max(1, min(limit, 100))
    params = {"result_limit": limit}
    if offset > 0:
        params["result_offset"] = int(offset)
    res = _get(f"{INTX_BASE}/api/v1/instruments/{instrument}/funding", params)
    return _normalize_funding_rows(res)
