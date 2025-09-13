from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # Default useful for local docker-compose
    "postgresql+psycopg2://postgres:postgres@localhost:5432/delhivery",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    sale_order_number = Column(String(100), unique=True, nullable=False, index=True)

    pickup_location_name = Column(String(255))
    payment_mode = Column(String(50))
    customer_name = Column(String(255))
    customer_phone = Column(String(50))
    shipping_address_line1 = Column(Text)
    shipping_city = Column(String(255))
    shipping_pincode = Column(String(20))
    shipping_state = Column(String(255))
    item_sku_name = Column(Text)
    quantity_ordered = Column(Integer)
    unit_item_price = Column(Numeric(12, 2))
    weight_gm = Column(Integer)

    raw = Column(JSON)  # full CSV row or additional details

    waybill = Column(String(64), index=True)
    manifest_status = Column(String(50))
    manifested_at = Column(DateTime(timezone=True))

    logs = relationship("ManifestLog", back_populates="order", cascade="all, delete-orphan")


class ManifestBatch(Base):
    __tablename__ = "manifest_batches"

    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    pickup_location_name = Column(String(255))
    total_count = Column(Integer)
    status = Column(String(50))

    logs = relationship("ManifestLog", back_populates="batch", cascade="all, delete-orphan")


class ManifestLog(Base):
    __tablename__ = "manifest_logs"

    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    batch_id = Column(Integer, ForeignKey("manifest_batches.id"))
    sale_order_number = Column(String(100), index=True)
    operation = Column(String(50))  # create, edit, cancel, track, etc.
    request_payload = Column(JSON)
    response_payload = Column(JSON)
    waybill = Column(String(64), index=True)

    order_id = Column(Integer, ForeignKey("orders.id"))
    order = relationship("Order", back_populates="logs")
    batch = relationship("ManifestBatch", back_populates="logs")


def init_db() -> None:
    """Create tables if they do not exist."""
    Base.metadata.create_all(engine)


def upsert_order_from_row(db, row: Dict[str, Any]):
    """Insert or update an order using a CSV/JSON row.

    Minimal required key: 'Sale Order Number'.
    Stores the whole row into `raw` for flexible frontend rendering.
    """
    sale_order_number = str(row.get("Sale Order Number") or row.get("*Order ID") or "").strip()
    if not sale_order_number:
        return None

    order = (
        db.query(Order)
        .filter(Order.sale_order_number == sale_order_number)
        .one_or_none()
    )
    if order is None:
        order = Order(sale_order_number=sale_order_number)

    # Map common fields used during manifestation
    order.pickup_location_name = str(row.get("Pickup Location Name") or "").strip() or order.pickup_location_name
    order.payment_mode = str(row.get("Payment Mode") or row.get("*Payment Status") or "").strip() or order.payment_mode
    order.customer_name = str(row.get("Customer Name") or row.get("*First Name") or "").strip() or order.customer_name
    order.customer_phone = str(row.get("Customer Phone") or row.get("*Phone") or "").strip() or order.customer_phone
    order.shipping_address_line1 = (
        str(row.get("Shipping Address Line1") or row.get("*Street Address") or "").strip()
        or order.shipping_address_line1
    )
    order.shipping_city = str(row.get("Shipping City") or row.get("*City") or "").strip() or order.shipping_city
    order.shipping_pincode = str(row.get("Shipping Pincode") or row.get("*Postal Code") or "").strip() or order.shipping_pincode
    order.shipping_state = str(row.get("Shipping State") or "").strip() or order.shipping_state
    order.item_sku_name = str(row.get("Item Sku Name") or row.get("Translated Name") or "").strip() or order.item_sku_name

    try:
        order.quantity_ordered = int(str(row.get("Quantity Ordered") or row.get("Quantity") or "0").strip() or 0)
    except Exception:
        pass
    try:
        # numeric cleanup
        price_raw = str(row.get("Unit Item Price") or row.get("Total Price") or "").replace(",", "").strip()
        order.unit_item_price = (None if not price_raw else float(price_raw))
    except Exception:
        pass
    try:
        w_raw = str(row.get("Weight (gm)") or row.get("Weight") or "").replace(",", "").strip()
        order.weight_gm = (None if not w_raw else int(float(w_raw)))
    except Exception:
        pass

    # Always store full row for frontend
    order.raw = row

    db.add(order)
    return order


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

