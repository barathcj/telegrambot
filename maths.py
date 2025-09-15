from datetime import datetime, timezone

def days_between(now: datetime, later: datetime) -> float:
    return (later - now).total_seconds() / 86400.0

def annualized_basis(spot: float, fut: float, days_to_expiry: float) -> float | None:
    if spot is None or fut is None or spot <= 0 or days_to_expiry <= 0:
        return None
    T = days_to_expiry / 365.0
    return (fut / spot - 1.0) / T  # simple annualization
