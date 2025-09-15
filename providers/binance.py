# binance.py
import requests
from datetime import datetime, timezone
from typing import Optional, List, Dict

SPOT_BASE = "https://api.binance.com"
FUT_BASE  = "https://fapi.binance.com"   # USDⓈ-M Futures (USDT perpetuals)

def _get(url: str, params: Dict | None = None) -> Optional[dict | list]:
    try:
        r = requests.get(url, params=params or {}, timeout=8)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        try:
            print("Binance HTTPError:", e.response.status_code, e.response.text)
        except Exception:
            print("Binance HTTPError (no body)")
        return None
    except Exception as e:
        print("Binance request error:", e)
        return None

def _sym(coin: str) -> str:
    """Map 'BTC' -> 'BTCUSDT', 'ETH' -> 'ETHUSDT' (uppercased)."""
    return f"{coin.upper()}USDT"

def get_spot_price(coin: str = "BTC") -> Optional[float]:
    """
    GET /api/v3/ticker/price?symbol=BTCUSDT
    Spot last price (simple and fast).
    """
    sym = _sym(coin)
    res = _get(f"{SPOT_BASE}/api/v3/ticker/price", {"symbol": sym})
    if not res or "price" not in res:
        return None
    try:
        return float(res["price"])
    except Exception:
        return None

def get_mark_and_funding(coin: str = "BTC") -> Optional[dict]:
    """
    GET /fapi/v1/premiumIndex?symbol=BTCUSDT
    Returns mark price, index price, instantaneous funding rate (8h), and next funding time (ms).
    """
    sym = _sym(coin)
    res = _get(f"{FUT_BASE}/fapi/v1/premiumIndex", {"symbol": sym})
    if not res:
        return None
    try:
        return {
            "symbol": res.get("symbol"),
            "markPrice": float(res["markPrice"]),
            "indexPrice": float(res["indexPrice"]),
            "lastFundingRate": float(res.get("lastFundingRate", 0.0)),
            "nextFundingTime": int(res.get("nextFundingTime", 0)),  # ms epoch
        }
    except Exception:
        return None

def get_funding_history(coin: str = "BTC", limit: int = 10) -> List[dict]:
    """
    GET /fapi/v1/fundingRate?symbol=BTCUSDT&limit=10
    Recent historical funding rates (each ~8h).
    """
    sym = _sym(coin)
    limit = max(1, min(limit, 100))
    res = _get(f"{FUT_BASE}/fapi/v1/fundingRate", {"symbol": sym, "limit": limit})
    if not isinstance(res, list):
        return []
    out = []
    for row in res:
        try:
            out.append({
                "fundingTime": int(row["fundingTime"]),            # ms
                "fundingRate": float(row["fundingRate"]),
            })
        except Exception:
            continue
    return out

def ms_to_dt_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
