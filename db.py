from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database
from bson import ObjectId


MONGODB_URL = os.getenv("MongoDB_URL") or os.getenv("MONGODB_URL") or os.getenv("MONGO_URL")
if not MONGODB_URL:
    # Fallback to local if not provided. Users provided MongoDB_URL in .env
    MONGODB_URL = "mongodb://localhost:27017"

DB_NAME = os.getenv("MONGODB_DB", "delhivery")

_client: Optional[MongoClient] = None
_db: Optional[Database] = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URL)
    return _client


def get_db() -> Database:
    global _db
    if _db is None:
        _db = get_client()[DB_NAME]
    return _db


def init_db() -> None:
    db = get_db()
    # Ensure indexes
    db.orders.create_index([("sale_order_number", ASCENDING)], unique=True)
    db.orders.create_index([("waybill", ASCENDING)])
    db.manifest_logs.create_index([("created_at", DESCENDING)])
    db.manifest_logs.create_index([("sale_order_number", ASCENDING)])
    db.manifest_batches.create_index([("created_at", DESCENDING)])


def upsert_order_from_row(db: Database, row: Dict[str, Any]):
    """Insert or update an order document using a CSV/JSON row.

    Minimal required key: 'Sale Order Number' or '*Order ID'.
    Stores the whole row into `raw` for flexible frontend rendering.
    """
    sale_order_number = str(row.get("Sale Order Number") or row.get("*Order ID") or "").strip()
    if not sale_order_number:
        return None

    update_doc: Dict[str, Any] = {
        "sale_order_number": sale_order_number,
        "pickup_location_name": str(row.get("Pickup Location Name") or "").strip() or None,
        "payment_mode": str(row.get("Payment Mode") or row.get("*Payment Status") or "").strip() or None,
        "customer_name": str(row.get("Customer Name") or row.get("*First Name") or "").strip() or None,
        "customer_phone": str(row.get("Customer Phone") or row.get("*Phone") or "").strip() or None,
        "shipping_address_line1": str(row.get("Shipping Address Line1") or row.get("*Street Address") or "").strip() or None,
        "shipping_city": str(row.get("Shipping City") or row.get("*City") or "").strip() or None,
        "shipping_pincode": str(row.get("Shipping Pincode") or row.get("*Postal Code") or "").strip() or None,
        "shipping_state": str(row.get("Shipping State") or "").strip() or None,
        "item_sku_name": str(row.get("Item Sku Name") or row.get("Translated Name") or "").strip() or None,
        "raw": row,
        "updated_at": datetime.now(timezone.utc),
    }

    db.orders.update_one(
        {"sale_order_number": sale_order_number},
        {"$set": update_doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    return db.orders.find_one({"sale_order_number": sale_order_number})


def extract_waybills_from_response(resp: Dict[str, Any]) -> Dict[str, str]:
    """Try to extract a mapping of {order_id -> waybill} from various response shapes."""
    result: Dict[str, str] = {}

    # Common: resp.packages: [{waybill, order/order_id/reference}]
    packages = []
    if isinstance(resp, dict):
        if isinstance(resp.get("packages"), list):
            packages = resp["packages"]
        elif isinstance(resp.get("response"), dict) and isinstance(resp["response"].get("packages"), list):
            packages = resp["response"]["packages"]

    for p in packages:
        try:
            wb = str(p.get("waybill") or p.get("wbn") or p.get("awb") or "").strip()
            ord_id = str(p.get("order") or p.get("order_id") or p.get("reference") or "").strip()
            if ord_id and wb:
                result[ord_id] = wb
        except Exception:
            continue

    # Fallbacks: shipments array
    shipments = []
    if not result and isinstance(resp, dict):
        if isinstance(resp.get("shipments"), list):
            shipments = resp["shipments"]
        elif isinstance(resp.get("response"), dict) and isinstance(resp["response"].get("shipments"), list):
            shipments = resp["response"]["shipments"]

    for s in shipments:
        try:
            wb = str(s.get("waybill") or s.get("wbn") or s.get("awb") or "").strip()
            ord_id = str(s.get("order") or s.get("order_id") or s.get("reference") or "").strip()
            if ord_id and wb:
                result[ord_id] = wb
        except Exception:
            continue

    return result
