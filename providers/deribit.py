# deribit.py
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

BASE_URL = "https://www.deribit.com/api/v2"

logger = logging.getLogger(__name__)

_LAST_HTTP_ERROR: Optional[Dict] = None

class InstrumentNotOpenError(Exception):
    """
    Raised when Deribit returns `instrument is not open` for a funding history request.
    """
    def __init__(self, instrument_name: str, message: str | None = None):
        self.instrument_name = instrument_name
        super().__init__(message or f"{instrument_name} not open on Deribit")

def get_last_http_error() -> Optional[Dict]:
    return _LAST_HTTP_ERROR

def _get(endpoint: str, params: Dict) -> Optional[dict]:
    """
    Small helper around requests.get that returns the 'result' dict/list
    or None on error (and prints a brief message).
    """
    url = f"{BASE_URL}{endpoint}"
    global _LAST_HTTP_ERROR
    _LAST_HTTP_ERROR = None
    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        _LAST_HTTP_ERROR = None
        return r.json().get("result")
    except requests.HTTPError as e:
        info: Dict = {
            "endpoint": endpoint,
            "params": dict(params),
            "status": getattr(e.response, "status_code", None),
            "body": None,
        }
        # Helpful debug output if Deribit rejects a request
        try:
            body = e.response.text
            info["body"] = body
            payload = e.response.json()
            err = payload.get("error", {}) if isinstance(payload, dict) else {}
            data = err.get("data") if isinstance(err.get("data"), dict) else {}
            info["reason"] = data.get("reason")
            info["param"] = data.get("param")
            info["message"] = err.get("message")
            print("Deribit HTTPError:", e.response.status_code, body)
        except Exception:
            print("Deribit HTTPError (no body)")
        _LAST_HTTP_ERROR = info
        logger.warning(
            "Deribit HTTPError endpoint=%s status=%s reason=%s param=%s params=%s",
            endpoint, info.get("status"), info.get("reason"), info.get("param"), info.get("params")
        )
        return None
    except Exception as e:
        print("Deribit request error:", e)
        _LAST_HTTP_ERROR = {
            "endpoint": endpoint,
            "params": dict(params),
            "status": None,
            "message": str(e),
        }
        logger.exception("Deribit request error on %s with params=%s", endpoint, params)
        return None

def get_index_price(currency: str = "BTC") -> Optional[float]:
    """
    Returns Deribit index price for <currency>_usd (all lowercase required).
    """
    index_name = f"{currency.lower()}_usd"               # <-- force lowercase
    res = _get("/public/get_index_price", {"index_name": index_name})
    if not res:
        return None
    price = res.get("index_price")
    return float(price) if price is not None else None

def get_futures_summaries(currency: str = "BTC") -> List[dict]:
    """
    Fast summary for all active futures (mark_price included).
    """
    res = _get(
        "/public/get_book_summary_by_currency",
        {"currency": currency.upper(), "kind": "future"}
    )
    return res or []

def get_instruments_map(currency: str = "BTC") -> Dict[str, int]:
    """
    Map: instrument_name -> expiration_timestamp(ms) for active futures.
    Excludes perpetual futures (expiry=0).
    """
    res = _get(
        "/public/get_instruments",
        {"currency": currency.upper(), "kind": "future", "expired": "false"}
    )
    if not res:
        return {}

    return {
        row["instrument_name"]: int(row["expiration_timestamp"])
        for row in res
        if row.get("expiration_timestamp", 0) > 0   # <-- skip perps
    }

def ms_to_dt_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)



def get_option_summaries(currency: str = "BTC") -> list[dict]:
    """
    Fast summaries for all active options of a currency.
    We need: instrument_name, bid_price, ask_price, underlying_price.
    """
    res = _get(
        "/public/get_book_summary_by_currency",
        {"currency": currency.upper(), "kind": "option"}
    )
    return res or []

def get_option_instruments_map(currency: str = "BTC") -> dict[str, int]:
    """
    Map: instrument_name -> expiration_timestamp(ms) for active options.
    """
    res = _get(
        "/public/get_instruments",
        {"currency": currency.upper(), "kind": "option", "expired": "false"}
    )
    if not res:
        return {}
    return {
        row["instrument_name"]: int(row["expiration_timestamp"])
        for row in res
        if row.get("expiration_timestamp", 0) > 0
    }

def _summary_by_instrument(instrument_name: str) -> Optional[dict]:
    res = _get("/public/get_book_summary_by_instrument", {"instrument_name": instrument_name})
    if isinstance(res, list):
        return res[0] if res else None
    if isinstance(res, dict):
        data = res.get("data")
        if isinstance(data, list) and data:
            return data[0]
        return res
    return None

def get_perpetual_summary(instrument_name: str) -> Optional[dict]:
    """
    Return book summary for the provided perpetual instrument name.
    """
    if not instrument_name:
        return None
    inst = instrument_name.upper()
    # First try a targeted lookup (fast for all instruments)
    row = _summary_by_instrument(inst)
    if row:
        return row
    # Fallback: attempt currency sweep if instrument resembles BASE[-/_QUOTE]-PERPETUAL
    base = inst.split("-")[0]
    summaries = get_futures_summaries(base) or []
    for r in summaries:
        if str(r.get("instrument_name", "")).upper() == inst:
            return r
    return None

def get_funding_rate_history(
    instrument_name: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    count: int | None = None
) -> list[dict]:
    """
    Fetch historical funding prints for the perpetual via /public/get_funding_rate_history.
    Returns chronological rows [{timestamp, funding_rate}]
    """
    inst = (instrument_name or "").upper()
    if not inst:
        return []
    params: Dict[str, int | str] = {"instrument_name": inst}
    if start_ms is not None:
        params["start_timestamp"] = int(start_ms)
    if end_ms is not None:
        params["end_timestamp"] = int(end_ms)
    elif start_ms is not None:
        # Deribit now rejects requests with start but no end, so default to "now".
        params["end_timestamp"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    if count is not None and start_ms is None and end_ms is None:
        # Deribit rejects combos of count with explicit timestamps, so only use count when
        # the caller relies on the API to page from the latest observations.
        params["count"] = max(1, min(int(count), 500))
    res = _get("/public/get_funding_rate_history", params)
    if res is None:
        err = get_last_http_error()
        reason = (err or {}).get("reason")
        if reason == "instrument is not open":
            raise InstrumentNotOpenError(inst, (err or {}).get("message"))
        return []

    if isinstance(res, list):
        raw = res
    elif isinstance(res, dict):
        raw = res.get("data") or res.get("entries") or []
    else:
        raw = []

    if not raw:
        return []

    out: list[dict] = []
    for row in raw:
        ts = row.get("timestamp") or row.get("time") or row.get("event_timestamp")
        fr = (
            row.get("funding_rate")
            or row.get("value")
            or row.get("interest_8h")
            or row.get("interest_rate")
        )
        if ts is None or fr is None:
            continue
        try:
            out.append({"timestamp": int(ts), "funding_rate": float(fr)})
        except Exception:
            continue

    if not out:
        return []

    out.sort(key=lambda r: r["timestamp"])
    return out
