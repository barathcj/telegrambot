# providers/talos_rest.py
import base64, hashlib, hmac, datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlencode
import requests

OPEN_STATUSES_DEFAULT = ["PendingNew", "New", "PartiallyFilled", "PendingReplace"]

def _now_iso() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _sign_headers(host: str, method: str, path: str, query: str, key: str, secret: str) -> dict:
    ts = _now_iso()
    parts = [method.upper(), ts, host, path]
    if query:
        parts.append(query)
    payload = "\n".join(parts)
    sig = base64.urlsafe_b64encode(
        hmac.new(secret.encode("ascii"), payload.encode("ascii"), hashlib.sha256).digest()
    ).decode()
    return {"TALOS-KEY": key, "TALOS-TS": ts, "TALOS-SIGN": sig, "Accept": "application/json"}

def _fmt_px(p):
    try:
        x = float(p)
        return f"{x:,.6f}" if abs(x) < 1 else f"{x:,.2f}"
    except Exception:
        return str(p) if p is not None else "-"

def _fmt_qty(q):
    try:
        return f"{float(q):.8f}"
    except Exception:
        return str(q) if q is not None else "-"

def _oid_short(s: Optional[str], n: int = 8) -> str:
    if not s: return "-"
    return s[:n] + "…" if len(s) > n else s

@dataclass
class TalosRestClient:
    host: str                 # e.g. "tal-59.prod.talostrading.com"
    api_key: str
    api_secret: str
    path: str = "/v1/orders"  # list orders endpoint
    timeout: int = 15
    page_limit: int = 500

    def list_open_orders(
        self,
        statuses: Optional[Iterable[str]] = None,
        subaccounts: Optional[Iterable[str]] = None,
        exclude_users: Optional[Iterable[str]] = None,
    ) -> list[dict]:
        """Return de-duped 'live' orders after server-side filtering."""
        statuses = list(statuses or OPEN_STATUSES_DEFAULT)
        params_base = {"Statuses": ",".join(statuses), "limit": str(self.page_limit)}
        if subaccounts:
            params_base["SubAccounts"] = ",".join(subaccounts)

        all_rows: list[dict] = []
        after = None
        sess = requests.Session()

        while True:
            params = dict(params_base)
            if after:
                params["after"] = after
            query = urlencode(sorted(params.items()))
            url = f"https://{self.host}{self.path}?{query}"
            hdr = _sign_headers(self.host, "GET", self.path, query, self.api_key, self.api_secret)

            r = sess.get(url, headers=hdr, timeout=self.timeout)
            if r.status_code == 404:
                raise RuntimeError(f"Talos REST 404 on {self.path} — confirm your list-orders path.")
            r.raise_for_status()
            payload = r.json()

            if isinstance(payload, dict):
                rows = payload.get("data", [])
                all_rows.extend(rows)
                after = payload.get("next") or payload.get("after")
                if not after or not rows:
                    break
            elif isinstance(payload, list):
                all_rows.extend(payload)
                break
            else:
                break

        # De-dup by OrderID (keep last appearance)
        by_oid: dict[str, dict] = {}
        for o in all_rows:
            oid = o.get("OrderID") or o.get("orderId") or o.get("Id")
            if not oid:
                continue
            by_oid[oid] = o

        # Client-side filters
        excl = {e.upper() for e in (exclude_users or [])}
        def is_excluded(o: dict) -> bool:
            u = (o.get("RequestUser") or o.get("CustomerUser") or o.get("User") or "").upper()
            return u in excl

        def is_live(o: dict) -> bool:
            st = (o.get("OrdStatus") or "").strip()
            if st not in statuses:
                return False
            try:
                leaves = float(o.get("LeavesQty") or 0)
            except Exception:
                leaves = 0.0
            return leaves > 0.0

        live = [o for o in by_oid.values() if not is_excluded(o) and is_live(o)]
        live.sort(key=lambda o: (str(o.get("AccountName") or o.get("SubAccount") or ""),
                                 str(o.get("Symbol") or ""), str(o.get("Side") or "")))
        return live

    @staticmethod
    def format_block(rows: list[dict], header: str = "Open orders", max_rows: int = 200) -> str:
        """Build a compact, readable text block (no Telegram markup)"""
        lines: list[str] = [f"{header} — {len(rows)}"]
        current_acct = None
        shown = 0
        for o in rows:
            if shown >= max_rows:
                lines.append(f"... and {len(rows) - shown} more")
                break
            acct = o.get("AccountName") or o.get("SubAccount") or "-"
            if acct != current_acct:
                current_acct = acct
                lines.append(f"\n* {acct}")
            sym  = o.get("Symbol") or "-"
            side = o.get("Side") or "-"
            typ  = o.get("OrdType") or "-"
            px   = _fmt_px(o.get("Price") or o.get("LimitPx") or o.get("Px"))
            oq   = _fmt_qty(o.get("OrderQty"))
            cq   = _fmt_qty(o.get("CumQty"))
            lq   = _fmt_qty(o.get("LeavesQty"))
            oid  = o.get("OrderID") or "-"
            lines.append(f"- {sym} · {typ} · {side} @ {px} | Qty {oq}  Cum {cq}  Leaves {lq} | OID {_oid_short(oid)}")
            shown += 1
        return "\n".join(lines)
