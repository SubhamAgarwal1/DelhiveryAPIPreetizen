"""
csv_to_order_json.py
~~~~~~~~~~~~~~~~~~~~~

Convert an orders CSV into the Delhivery create-order JSON payload.

Usage:
  python csv_to_order_json.py --csv path/to/orders.csv \
      --pickup "MainWarehouse" \
      --default-hsn 610910 \
      --select PZ10861Q120250910WED,PZ10860Q120250910WED \
      --out order_payload.json

If --pickup is omitted, the script tries to infer it from the
"Pickup Location Name" column. If multiple values are present, it will
use the first one unless you pass --pickup explicitly.

The script expects column headers similar to the provided sample CSV,
including (not exhaustive):
  - Sale Order Number
  - Customer Name, Customer Phone
  - Shipping Address Line1, Shipping City, Shipping Pincode, Shipping State
  - Payment Mode (COD/Prepaid)
  - Item Sku Name, Total Price, Quantity Ordered, Weight (gm)

Outputs a JSON object with keys: shipments (list) and pickup_location (name).
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _get(row: Dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in row and row[k] is not None:
            val = str(row[k]).strip()
            if val:
                return val
    return default


def _to_float(s: str, default: float = 0.0) -> float:
    try:
        # Remove commas and spaces
        return float(str(s).replace(",", "").strip())
    except Exception:
        return default


def _payment_mode(val: str) -> str:
    v = (val or "").strip().lower()
    if v in {"prepaid", "paid", "online"}:
        return "Prepaid"
    if v in {"cod", "cash on delivery"}:
        return "COD"
    if v in {"pickup", "pick-up"}:
        return "Pickup"
    return "Prepaid"


def build_shipments(
    rows: List[Dict[str, Any]],
    default_hsn: Optional[str] = None,
    default_country: str = "India",
) -> List[Dict[str, Any]]:
    shipments: List[Dict[str, Any]] = []
    for row in rows:
        order_no = _get(row, "Sale Order Number", "*Order ID")
        if not order_no:
            # Skip rows that don't have an order identifier
            continue

        # Amounts
        qty = _to_float(_get(row, "Quantity Ordered"), 1.0)
        total_price = _to_float(_get(row, "Total Price", "*Total Amount"))
        if total_price == 0:
            unit_price = _to_float(_get(row, "Unit Item Price", "Subtotal"))
            total_price = unit_price * max(qty, 1.0)

        weight_gm = _get(row, "Weight (gm)") or _get(row, "Weight")
        weight_str = f"{weight_gm}gm" if weight_gm else ""

        shipment: Dict[str, Any] = {
            "add": _get(row, "Shipping Address Line1", "*Street Address"),
            "phone": _get(row, "Customer Phone", "*Phone"),
            "payment_mode": _payment_mode(_get(row, "Payment Mode", "*Payment Status")),
            "name": _get(row, "Customer Name", "*First Name"),
            "pin": _get(row, "Shipping Pincode", "*Postal Code"),
            "state": _get(row, "Shipping State"),
            "city": _get(row, "Shipping City", "*City"),
            "country": default_country,
            "order": order_no,
            "cosignee_gst_amount": "0",
            "integrated_gst_amount": "0",
            "gst_cess_amount": "0",
            "ewbn": "",
            "cosignee_gst_tin": "",
            "hsn_code": default_hsn or "",
            "total_amount": round(total_price, 2),
            "weight": weight_str,
            "product_desc": _get(row, "Item Sku Name", "Translated Name"),
        }
        shipments.append(shipment)

    return shipments


def main() -> None:
    ap = argparse.ArgumentParser(description="Convert orders CSV to Delhivery order JSON")
    ap.add_argument("--csv", required=True, help="Path to orders CSV file")
    ap.add_argument("--pickup", help="Pickup location name (warehouse)")
    ap.add_argument("--default-hsn", dest="default_hsn", help="Default HSN code to include")
    ap.add_argument(
        "--select",
        help="Comma-separated list of order ids (Sale Order Number or *Order ID) to include",
    )
    ap.add_argument("--out", help="Path to write JSON output (defaults to stdout)")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    # Read CSV rows
    rows: List[Dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    # Filter selection if provided
    if args.select:
        wanted = {s.strip() for s in args.select.split(",") if s.strip()}
        rows = [
            r
            for r in rows
            if _get(r, "Sale Order Number", "*Order ID") in wanted
        ]

    if not rows:
        raise SystemExit("No rows after filtering; check CSV and --select filter")

    # Determine pickup location
    pickup_name = args.pickup
    if not pickup_name:
        # Collect unique values
        candidates = []
        for r in rows:
            v = _get(r, "Pickup Location Name")
            if v and v not in candidates:
                candidates.append(v)
        pickup_name = candidates[0] if candidates else "MainWarehouse"

    shipments = build_shipments(rows, default_hsn=args.default_hsn)
    payload = {
        "shipments": shipments,
        "pickup_location": {"name": pickup_name},
    }

    data = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(data, encoding="utf-8")
    else:
        print(data)


if __name__ == "__main__":
    main()

