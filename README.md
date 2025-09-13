# Delhivery Python API Wrapper

This project provides a basic Python‑based API that wraps some of the
functionality offered by the Delhivery express courier service.  It is
modelled after the Laravel SDK described in the `nguyendachuy/laravel‑delhivery‑api`
repository, but is implemented in Python using [FastAPI](https://fastapi.tiangolo.com/)
and the `requests` HTTP client.  You can use it to integrate
Delhivery’s serviceability lookup, waybill generation and order
management into your own applications or expose them as HTTP endpoints.

## Features

This wrapper exposes several commonly used Delhivery endpoints:

* **Pincode serviceability** – check if a given postal code is serviceable by
  Delhivery.
* **Order creation** – create and manifest a new order with shipments and
  pickup details.  The API forwards your payload directly to
  Delhivery’s `cmu/create.json` endpoint【550105068546031†L346-L370】.
* **Order tracking and cancellation** – cancel or track an order by its
  waybill number【550105068546031†L374-L389】.
* **Waybill generation** – generate bulk waybills (with a method stub for
  fetching single waybills).
* **Invoice calculation** – obtain approximate shipping charges for a
  prospective shipment by supplying billing mode, weight and pincodes
  via the invoice API【123006354569356†L1-L4】.
* **Packing slip printing** – generate a packing slip for a given waybill,
  which can be rendered into a shipping label【40241588985681†L115-L133】.
* **Pickup scheduling** – create a pickup request for a specific warehouse
  and time【112589501909948†L115-L139】.
* **Warehouse management** – register new pickup locations (warehouses)
  or edit existing ones【923132388160142†L117-L130】【398867434661020†L115-L124】.
* **NDR actions** – take actions on non‑delivery packages (defer delivery,
  edit consignee details or request a reattempt) and query the status of
  asynchronous requests【268787320008269†L115-L179】【715084579276354†L115-L119】.

Each endpoint is exposed through a simple HTTP route in the FastAPI
application.  You can extend the client further by following the
existing patterns.

## Getting started

1. **Install dependencies**.  The API uses FastAPI, Uvicorn and
   Requests.  From the project root run:

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables**.  You need to supply your
   Delhivery API token and choose the mode (`staging` or `live`):

   ```bash
   export DELHIVERY_TOKEN="your_api_token"
   export DELHIVERY_MODE="staging"  # or "live"
   ```

   These variables are read by `DelhiveryClient`.  You can also pass
   them directly when instantiating the client.

3. **Run the API server**:

   ```bash
   uvicorn main:app --reload
   ```

   This starts a development server on `http://localhost:8000`.  You can
   use tools like Postman or cURL to exercise the endpoints:

   ```bash
   # Check serviceability for PIN 400064
   curl -X GET "http://localhost:8000/pincode?filter_codes=400064"

   # Create a shipment
   curl -X POST "http://localhost:8000/orders" \
        -H "Content-Type: application/json" \
        -d @example_order.json
   ```

5. **Run the frontend (Next.js)**.

   A minimal React/Next.js frontend lives under `frontend/`. It lets you
   import the sample orders CSV, select orders, and create a manifest
   by calling the FastAPI `/orders` endpoint. In a separate terminal:

   ```bash
   cd frontend
   cp .env.local.example .env.local   # adjust API base if needed
   npm install
   npm run dev
   ```

   Visit `http://localhost:3000`, upload your CSV (like
   `delhivery_manifest_ (6) - delhivery_manifest_ (7).csv`), select rows,
   choose a pickup location, and click "Create Manifest". When the API
   returns, click "Export Waybills CSV" to download the waybills.

6. **Optional: Convert CSV to order JSON via script**.

   You can also generate the Delhivery order payload directly from CSV
   using the provided Python script:

   ```bash
   python csv_to_order_json.py --csv "delhivery_manifest_ (6) - delhivery_manifest_ (7).csv" \
     --pickup "Preetizen Lifestyle" \
     --default-hsn 610910 \
     --select PZ10861Q120250910WED,PZ10860Q120250910WED \
     --out order_payload.json
   ```
   Then POST `order_payload.json` to `POST /orders`.

4. **Extend the client**.  The file `delhivery_client.py` defines a
   `DelhiveryClient` class with helper methods.  You can add methods
   to call other Delhivery endpoints (e.g. invoice management, pickup
   scheduling, warehouse creation) by following the patterns shown
   inside `create_order`, `cancel_order`, etc.

## Security note

This code is provided as an example.  Do **not** expose your
Delhivery token to untrusted clients.  In production you should
implement proper authentication on the FastAPI endpoints and secure
your environment variables.
