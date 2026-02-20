from datetime import UTC, datetime
from random import Random

from fastapi import FastAPI, Query

app = FastAPI(title="DRP Data Generator API", version="0.1.0")
_rng = Random(42)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "api-generator"}


@app.get("/v1/orders")
def get_orders(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, object]:
    orders: list[dict[str, object]] = []
    for idx in range(limit):
        orders.append(
            {
                "order_id": f"ord_{idx + 1:06d}",
                "customer_id": f"cus_{_rng.randint(1, 200):05d}",
                "amount": round(_rng.uniform(5, 500), 2),
                "created_at": datetime.now(UTC).isoformat(),
            }
        )
    return {"records": orders, "count": len(orders)}
