"""
main.py
~~~~~~~

A simple FastAPI application exposing select Delhivery API operations.
This microservice wraps the functionality of the `DelhiveryClient` class
defined in `delhivery_client.py` and demonstrates how you might expose
the Delhivery API as RESTful endpoints in your own infrastructure.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query, Body, Depends, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import requests  # for handling HTTP errors

from delhivery_client import DelhiveryClient
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import func

from db import SessionLocal, init_db, Order, ManifestBatch, ManifestLog, upsert_order_from_row, extract_waybills_from_response
import logging
import json as _json


def get_client() -> DelhiveryClient:
    """Dependency injection helper that constructs a client per request.

    FastAPI will call this function on each request.  The returned
    client reads the API token and mode from environment variables.
    """
    try:
        return DelhiveryClient()
    except Exception as exc:
        # Convert errors into HTTP exceptions for consistent error handling
        raise HTTPException(status_code=500, detail=str(exc))


load_dotenv()

app = FastAPI(title="Delhivery Python API Wrapper")

# Allow CORS for local frontend (Next.js default dev server on 3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def on_startup():
    # Ensure tables exist before handling requests
    init_db()
    # Configure logging level
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    # Ensure root handler exists so custom loggers propagate to console
    root = logging.getLogger()
    if not root.handlers:
        root_handler = logging.StreamHandler()
        root_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        root_handler.setFormatter(root_formatter)
        root.addHandler(root_handler)
    root.setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("uvicorn").setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("uvicorn.error").setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("uvicorn.access").setLevel(getattr(logging, level, logging.INFO))
    # Ensure delhivery logger propagates
    dlog = logging.getLogger("delhivery")
    dlog.setLevel(getattr(logging, level, logging.INFO))
    dlog.propagate = True
    logger.info("Startup complete. DB initialized. Log level=%s", level)

    # Announce pickup and GST constants for visibility
    logger.info(
        "Pickup fixed: name=%s city=%s pin=%s country=%s",
        os.getenv("PICKUP_NAME", "Preetizen Lifestyle"),
        os.getenv("PICKUP_CITY", "Kolkata"),
        os.getenv("PICKUP_PIN", "700107"),
        os.getenv("PICKUP_COUNTRY", "India"),
    )
    logger.info(
        "GST constants: consignee_gst_amount=%s integrated_gst_amount=%s gst_cess_amount=%s consignee_gst_tin=%s hsn_code=%s",
        os.getenv("CONSIGNEE_GST_AMOUNT", "150.00"),
        os.getenv("INTEGRATED_GST_AMOUNT", "275.50"),
        os.getenv("GST_CESS_AMOUNT", "35.25"),
        os.getenv("CONSIGNEE_GST_TIN", "27ABCDE1234F1Z5"),
        os.getenv("HSN_CODE", "851770"),
    )
    logger.info("Dry-run mode: %s", os.getenv("DRY_RUN", "false"))


@app.get("/pincode")
async def pincode_serviceability(filter_codes: str = Query(..., description="Comma separated PIN codes"),
                                 client: DelhiveryClient = Depends(get_client)) -> Dict[str, Any]:
    """Check whether one or more pincodes are serviceable by Delhivery.

    You can pass a single code or a comma‑separated list of codes in
    the `filter_codes` query parameter.
    """
    try:
        return client.pincode_serviceability(filter_codes)
    except requests.HTTPError as exc:  # type: ignore
        # If the upstream API returns a 4xx/5xx we propagate as 400
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/orders")
async def create_order(
    order_details: Dict[str, Any] = Body(..., description="Order payload as per Delhivery docs"),
    client: DelhiveryClient = Depends(get_client),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Create and manifest a new order.

    See Delhivery’s order creation API documentation for the structure of
    the request body.  This endpoint simply forwards your JSON to
    Delhivery and returns their response【550105068546031†L346-L370】.
    """
    try:
        # If dry-run, do not create batch or call external API; just echo payload
        if os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}:
            logger.info("[MANIFEST][DRY_RUN] Skipping Delhivery call. Returning provided payload")
            return {"dry_run": True, "payload": order_details}

        # Create manifest batch
        pickup_name = None
        try:
            pickup_name = (order_details.get("pickup_location") or {}).get("name")
        except Exception:
            pickup_name = None

        batch = ManifestBatch(pickup_location_name=pickup_name, total_count=len(order_details.get("shipments", []) or []), status="pending")
        db.add(batch)
        db.flush()

        # Log request
        db.add(ManifestLog(batch_id=batch.id, operation="create", request_payload=order_details))
        db.commit()
        db.refresh(batch)

        # Console log — sanitized
        _payload_log = _json.dumps(_redact_tokens(order_details), ensure_ascii=False)
        logger.info("[MANIFEST][batch=%s] Request payload to Delhivery: %s", batch.id, _payload_log)

        # Call upstream
        resp = client.create_order(order_details)

        # Persist response log
        log = ManifestLog(batch_id=batch.id, operation="create", response_payload=resp)
        db.add(log)

        # Console log response
        _resp_log = _json.dumps(_redact_tokens(resp), ensure_ascii=False)
        logger.info("[MANIFEST][batch=%s] Response from Delhivery: %s", batch.id, _resp_log)

        # Attempt to map waybills back to orders and update
        mapping = extract_waybills_from_response(resp)
        for shp in order_details.get("shipments", []) or []:
            ord_id = str(shp.get("order") or shp.get("order_id") or shp.get("reference") or "").strip()
            if not ord_id:
                continue
            wb = mapping.get(ord_id)
            # Create per-order log row
            entry = ManifestLog(
                batch_id=batch.id,
                sale_order_number=ord_id,
                operation="create",
                request_payload=shp,
                response_payload=None,
                waybill=wb,
            )
            db.add(entry)

            # Update order if exists
            o = db.query(Order).filter(Order.sale_order_number == ord_id).one_or_none()
            if o:
                o.waybill = wb
                o.manifest_status = "manifested" if wb else o.manifest_status
                o.manifested_at = func.now() if wb else o.manifested_at
                db.add(o)

        batch.status = "completed"
        db.add(batch)
        db.commit()

        return resp
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/orders/edit")
async def edit_order(order_details: Dict[str, Any] = Body(...), client: DelhiveryClient = Depends(get_client)) -> Dict[str, Any]:
    """Edit an existing order【550105068546031†L361-L369】."""
    try:
        return client.edit_order(order_details)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/orders/cancel")
async def cancel_order(waybill: str = Body(..., embed=True), client: DelhiveryClient = Depends(get_client)) -> Dict[str, Any]:
    """Cancel an order by its waybill number【550105068546031†L374-L379】."""
    try:
        return client.cancel_order(waybill)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/orders/track/{waybill}")
async def track_order(waybill: str, client: DelhiveryClient = Depends(get_client)) -> Dict[str, Any]:
    """Track an order’s status by its waybill【550105068546031†L379-L389】."""
    try:
        return client.track_order(waybill)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/waybill/bulk")
async def bulk_waybill(count: int = Body(..., embed=True), client: DelhiveryClient = Depends(get_client)) -> Dict[str, Any]:
    """Generate a batch of waybill numbers.【550105068546031†L327-L343】"""
    if count <= 0:
        raise HTTPException(status_code=400, detail="count must be positive")
    try:
        return client.bulk_waybill(count)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Additional API endpoints
#
# The following routes wrap the remaining Delhivery services exposed by the
# Laravel SDK and documented in Delhivery’s API specification.  They
# demonstrate how to forward query parameters and JSON bodies from FastAPI
# through to the underlying `DelhiveryClient`.


@app.get("/invoice/charges")
async def invoice_charges(
    md: str = Query(None, description="Billing mode of shipment (E or S)"),
    cgm: int = Query(None, description="Chargeable weight in grams"),
    o_pin: str = Query(None, description="Origin pin code"),
    d_pin: str = Query(None, description="Destination pin code"),
    ss: str = Query(None, description="Shipment status (Delivered, RTO, DTO)"),
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Calculate approximate shipping charges for a prospective shipment.

    This endpoint proxies Delhivery’s invoice (shipping charge) API.  Any
    combination of supported query parameters may be supplied.  See
    Delhivery’s documentation for required and optional parameters.  Only
    parameters that are not ``None`` will be forwarded.
    """
    try:
        # Build a dictionary of non-null parameters
        params: Dict[str, Any] = {}
        if md is not None:
            params["md"] = md
        if cgm is not None:
            params["cgm"] = cgm
        if o_pin is not None:
            params["o_pin"] = o_pin
        if d_pin is not None:
            params["d_pin"] = d_pin
        if ss is not None:
            params["ss"] = ss
        return client.invoice_locations(params)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/packing-slip/{waybill}")
async def packing_slip(
    waybill: str,
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Generate a packing slip for a specific waybill.

    The response from Delhivery must be rendered into a PDF or HTML on
    the client side; this endpoint simply relays the JSON payload.
    """
    try:
        return client.print_packing_slip(waybill)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/pickup-request")
async def pickup_request(
    pickup_details: Dict[str, Any] = Body(..., description="Pickup request payload as per Delhivery docs"),
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Create a new pickup request with Delhivery.
    """
    try:
        return client.schedule_pickup(pickup_details)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/warehouse/create")
async def warehouse_create(
    details: Dict[str, Any] = Body(..., description="Client warehouse details as per Delhivery docs"),
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Register a new client warehouse with Delhivery.
    """
    try:
        return client.create_warehouse(details)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/warehouse/edit")
async def warehouse_edit(
    details: Dict[str, Any] = Body(..., description="Updated warehouse details, including warehouse code"),
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Update an existing client warehouse.
    """
    try:
        return client.edit_warehouse(details)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ----------------------
# DB-backed order routes
# ----------------------

@app.post("/orders/import")
async def import_orders(
    payload: Dict[str, Any] = Body(..., description="{ rows: Array<Record<string, any>> }"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    rows = payload.get("rows") or []
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="Body must include 'rows' as a list")

    total = len(rows)
    created = 0
    updated = 0
    logger.info("[IMPORT] Received %d rows for import", total)
    for row in rows:
        if not isinstance(row, dict):
            continue
        existing = None
        sale_order_number = str(row.get("Sale Order Number") or row.get("*Order ID") or "").strip()
        if sale_order_number:
            existing = (
                db.query(Order).filter(Order.sale_order_number == sale_order_number).one_or_none()
            )

        order = upsert_order_from_row(db, row)
        if order is None:
            continue
        if existing is None:
            created += 1
        else:
            updated += 1

    db.commit()
    logger.info("[IMPORT] Completed. created=%d updated=%d", created, updated)
    return {"received": total, "created": created, "updated": updated}


@app.get("/orders")
async def list_orders(db: Session = Depends(get_db)) -> Dict[str, Any]:
    items = (
        db.query(Order)
        .order_by(Order.id.desc())
        .all()
    )
    data = []
    for o in items:
        row = dict(o.raw or {})
        row["Sale Order Number"] = o.sale_order_number
        row["Pickup Location Name"] = o.pickup_location_name
        row["Waybill"] = o.waybill
        data.append(row)
    return {"count": len(data), "items": data}


# ----------------------
# Utilities
# ----------------------

logger = logging.getLogger("app")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _redact_tokens(obj: Any):
    """Return a deep-copied object with sensitive tokens redacted for logging."""
    try:
        if isinstance(obj, dict):
            red = {}
            for k, v in obj.items():
                kl = str(k).lower()
                if kl in {"token", "authorization", "auth", "api_key", "apikey"}:
                    red[k] = "***REDACTED***"
                else:
                    red[k] = _redact_tokens(v)
            return red
        if isinstance(obj, list):
            return [_redact_tokens(v) for v in obj]
        return obj
    except Exception:
        return obj


# ----------------------
# Debug endpoints (to inspect last payloads)
# ----------------------

@app.get("/debug/last-manifest")
def debug_last_manifest(db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = (
        db.query(ManifestLog)
        .filter(ManifestLog.operation == "create")
        .order_by(ManifestLog.id.desc())
        .first()
    )
    if not row:
        return {"message": "no manifest logs yet"}
    return {
        "id": row.id,
        "created_at": str(row.created_at),
        "batch_id": row.batch_id,
        "sale_order_number": row.sale_order_number,
        "request_payload": row.request_payload,
        "response_payload": row.response_payload,
        "waybill": row.waybill,
    }

@app.get("/debug/batch/{batch_id}")
def debug_batch(batch_id: int, db: Session = Depends(get_db)) -> Dict[str, Any]:
    logs = (
        db.query(ManifestLog)
        .filter(ManifestLog.batch_id == batch_id)
        .order_by(ManifestLog.id.asc())
        .all()
    )
    return {
        "batch_id": batch_id,
        "count": len(logs),
        "logs": [
            {
                "id": l.id,
                "created_at": str(l.created_at),
                "operation": l.operation,
                "sale_order_number": l.sale_order_number,
                "waybill": l.waybill,
                "request_payload": l.request_payload,
                "response_payload": l.response_payload,
            }
            for l in logs
        ],
    }


# ----------------------
# Manifest payload builder from DB (preview and execute)
# ----------------------

def _row_get(row: Dict[str, Any], keys: List[str], default: str = "") -> str:
    """Fetch first non-empty value for any of the provided keys.

    Handles variants with leading '*' that appear in CSV headers and trims whitespace.
    """
    for k in keys:
        candidates = []
        k_stripped = k.strip()
        candidates.append(k_stripped)
        # Try star-prefixed or non-star variant
        if k_stripped.startswith("*"):
            candidates.append(k_stripped.lstrip("*"))
        else:
            candidates.append("*" + k_stripped)
        # Try exact, and also trim BOM-like chars
        for cand in candidates:
            if cand in row and row[cand] is not None:
                s = str(row[cand]).strip()
                if s:
                    return s
    return default


def _to_int(val: Any, default: int = 0) -> int:
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def _to_num(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _normalize_payment(s: str) -> str:
    v = (s or "").lower()
    if v in {"prepaid", "paid", "online"}:
        return "Prepaid"
    if v in {"cod", "cash on delivery"}:
        return "COD"
    if v in {"pickup", "pick-up"}:
        return "Pickup"
    return "Prepaid"


def build_shipment_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    # Fixed constants from env
    CONSIGNEE_GST_AMOUNT = os.getenv("CONSIGNEE_GST_AMOUNT", "150.00")
    INTEGRATED_GST_AMOUNT = os.getenv("INTEGRATED_GST_AMOUNT", "275.50")
    GST_CESS_AMOUNT = os.getenv("GST_CESS_AMOUNT", "35.25")
    CONSIGNEE_GST_TIN = os.getenv("CONSIGNEE_GST_TIN", "27ABCDE1234F1Z5")
    HSN_CODE = os.getenv("HSN_CODE", "851770")

    qty = _to_int(_row_get(row, ["Quantity Ordered", "Quantity"])) or 1
    unit_price = _to_num(_row_get(row, ["Unit Item Price", "*Unit Item Price"]))
    total_price_str = _row_get(row, ["Total Amount", "*Total Amount", "Total Price", "*Total Price"]) or None
    total_amount = 0.0
    try:
        if total_price_str:
            total_amount = float(str(total_price_str).replace(",", "").strip())
        else:
            total_amount = round((unit_price or 0.0) * max(qty, 1), 2)
    except Exception:
        total_amount = 0.0

    weight_gm = _to_int(_row_get(row, ["Weight (gm)", "Weight", "*Weight"]))
    length_cm = _to_int(_row_get(row, ["Length (cm)"]))
    breadth_cm = _to_int(_row_get(row, ["Breadth (cm)"]))
    height_cm = _to_int(_row_get(row, ["Height (cm)"]))

    shipping_mode = _row_get(row, ["Transport Mode", "*Transport Mode"]).lower()
    shipping_mode = "Express" if shipping_mode == "express" else "Surface"

    # Build rich product description: name + size + colour
    name = _row_get(row, ["Item Sku Name", "Translated Name", "*Translated Name", "Item Name", "prd", "product_desc", "products_desc", "Item Sku Code"])  # fallbacks
    size = _row_get(row, ["Size", "*Size"]).strip()
    colour = _row_get(row, ["Color", "*Color", "Colour"]).strip()
    parts: List[str] = []
    if size:
        parts.append(f"Size: {size}")
    if colour:
        parts.append(f"Colour: {colour}")
    product = name
    if parts:
        product = f"{name} - " + " - ".join(parts) if name else " - ".join(parts)

    shipment: Dict[str, Any] = {
        "add": _row_get(row, ["Shipping Address Line1", "*Street Address", "add"]),
        "address_type": "home",
        "phone": _row_get(row, ["Customer Phone", "*Phone", "phone"]),
        "payment_mode": _normalize_payment(_row_get(row, ["Payment Mode", "*Payment Status", "payment_mode"])),
        "name": (lambda fn, ln: (fn + " " + ln).strip())(_row_get(row, ["Customer Name", "*First Name", "name"]), _row_get(row, ["*Last Name", "Last Name", ""])) ,
        "pin": _to_int(_row_get(row, ["Shipping Pincode", "*Postal Code", "pin"])),
        "order": _row_get(row, ["Sale Order Number", "*Order ID", "order"]),

        # Fixed GST fields
        "consignee_gst_amount": CONSIGNEE_GST_AMOUNT,
        "integrated_gst_amount": INTEGRATED_GST_AMOUNT,
        "ewbn": "",
        "consignee_gst_tin": CONSIGNEE_GST_TIN,
        "hsn_code": HSN_CODE,
        "gst_cess_amount": GST_CESS_AMOUNT,

        # Optional and recommended
        "city": _row_get(row, ["Shipping City", "*City", "city"]),
        "state": _row_get(row, ["Shipping State", "state"]),
        "country": "India",
        "weight": weight_gm,
        "shipment_height": height_cm,
        "shipment_width": breadth_cm,
        "shipment_length": length_cm,
        "shipping_mode": shipping_mode,
        "quantity": qty,
        "total_amount": round(total_amount, 2),
        "product_desc": product,
        "products_desc": product,
    }

    return shipment


def build_manifest_payload(db: Session, sale_order_numbers: List[str]) -> Dict[str, Any]:
    pickup = {
        "name": os.getenv("PICKUP_NAME", "Preetizen Lifestyle"),
        "city": os.getenv("PICKUP_CITY", "Kolkata"),
        "pin": os.getenv("PICKUP_PIN", "700107"),
        "country": os.getenv("PICKUP_COUNTRY", "India"),
    }

    shipments: List[Dict[str, Any]] = []
    for ord_id in sale_order_numbers:
        o = db.query(Order).filter(Order.sale_order_number == str(ord_id).strip()).one_or_none()
        if not o:
            continue
        row = dict(o.raw or {})
        row.setdefault("Sale Order Number", o.sale_order_number)
        shipments.append(build_shipment_from_row(row))

    payload = {"shipments": shipments, "pickup_location": pickup}
    logger.info("[BUILD_MANIFEST] Built payload with %d shipments: %s", len(shipments), _json.dumps(payload, ensure_ascii=False))
    return payload


@app.post("/orders/build-manifest")
async def api_build_manifest(body: Dict[str, Any] = Body(...), db: Session = Depends(get_db)) -> Dict[str, Any]:
    sale_order_numbers: List[str] = body.get("sale_order_numbers") or []
    if not isinstance(sale_order_numbers, list) or not sale_order_numbers:
        raise HTTPException(status_code=400, detail="Provide 'sale_order_numbers' as a non-empty list")
    payload = build_manifest_payload(db, sale_order_numbers)
    return payload


@app.post("/orders/manifest-from-db")
async def api_manifest_from_db(body: Dict[str, Any] = Body(...), client: DelhiveryClient = Depends(get_client), db: Session = Depends(get_db)) -> Dict[str, Any]:
    sale_order_numbers: List[str] = body.get("sale_order_numbers") or []
    if not isinstance(sale_order_numbers, list) or not sale_order_numbers:
        raise HTTPException(status_code=400, detail="Provide 'sale_order_numbers' as a non-empty list")

    payload = build_manifest_payload(db, sale_order_numbers)

    # Dry-run short-circuit: do not call external API or write batch
    if os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}:
        logger.info("[MANIFEST][DRY_RUN] Built payload for %d orders; skipping API call", len(payload.get("shipments", [])))
        return {"dry_run": True, "payload": payload}

    # Create a batch and logs similar to /orders
    batch = ManifestBatch(pickup_location_name=payload["pickup_location"]["name"], total_count=len(payload["shipments"]), status="pending")
    db.add(batch)
    db.flush()
    db.add(ManifestLog(batch_id=batch.id, operation="create", request_payload=payload))
    db.commit()
    db.refresh(batch)

    resp = client.create_order(payload)
    db.add(ManifestLog(batch_id=batch.id, operation="create", response_payload=resp))

    # Map waybills
    mapping = extract_waybills_from_response(resp)
    for shp in payload["shipments"]:
        ord_id = str(shp.get("order") or "").strip()
        wb = mapping.get(ord_id)
        db.add(ManifestLog(batch_id=batch.id, sale_order_number=ord_id, operation="create", request_payload=shp, response_payload=None, waybill=wb))
        o = db.query(Order).filter(Order.sale_order_number == ord_id).one_or_none()
        if o and wb:
            o.waybill = wb
            o.manifest_status = "manifested"
            o.manifested_at = func.now()
            db.add(o)

    batch.status = "completed"
    db.add(batch)
    db.commit()
    return resp


@app.post("/ndr/update")
async def ndr_update(
    data: Dict[str, Any] = Body(..., description="NDR action payload: waybill, act and optional fields"),
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Submit an asynchronous NDR action for a given package.
    """
    try:
        return client.ndr_update(data)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/ndr/status/{upl}")
async def ndr_status(
    upl: str,
    client: DelhiveryClient = Depends(get_client),
) -> Dict[str, Any]:
    """Retrieve the status of a previously submitted NDR action using its UPL.
    """
    try:
        return client.ndr_status(upl)
    except requests.HTTPError as exc:  # type: ignore
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

        


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 
