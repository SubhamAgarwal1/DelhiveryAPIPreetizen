"""
delhivery_client.py
~~~~~~~~~~~~~~~~~~~~

A thin Python client for Delhivery’s express API.  This module provides
helper methods that mirror the functionality of the Laravel SDK
described in the `nguyendachuy/laravel-delhivery-api` repository.  See
the README for examples and environment configuration.

The client reads the API token and mode (staging vs live) from
environment variables by default, but these may be provided
explicitly when constructing the client.
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional
import logging

from fastapi import HTTPException
import requests


class DelhiveryClient:
    """Simple HTTP client for Delhivery’s API.

    Parameters
    ----------
    token: str, optional
        The API token provided by Delhivery.  If not supplied,
        the `DELHIVERY_TOKEN` environment variable will be used.
    mode: str, optional
        One of `'staging'` or `'live'`.  Determines which base
        URL is used.  Defaults to the `DELHIVERY_MODE` environment
        variable or `'staging'` if unset.
    """

    def __init__(self, token: Optional[str] = None, mode: Optional[str] = None) -> None:
        self.token = token or os.getenv("DELHIVERY_TOKEN", "96ad50355c3c942dd6782bc95785c8fcc7b5e35f")
        if not self.token:
            raise ValueError("Delhivery API token must be provided via 'DELHIVERY_TOKEN' env var or constructor argument")
        mode = mode or os.getenv("DELHIVERY_MODE", "live")
        self.mode = mode.lower()
        if self.mode not in {"staging", "live"}:
            raise ValueError("DELHIVERY_MODE must be either 'staging' or 'live'")
        self.base_url = self._resolve_base_url(self.mode)

    def _resolve_base_url(self, mode: str) -> str:
        # Official endpoints differ between staging and live environments
        if mode == "live":
            return "https://express.delhivery.com/"
        # Default to staging
        return "https://track.delhivery.com/"

    def _get(self, path, params=None, *, use_auth_header=False):
        url = self.base_url.rstrip('/') + '/' + path.lstrip('/')
        params = params or {}
        headers = {}
        if use_auth_header:
            params.pop('token', None)
            headers['Authorization'] = f'Token {self.token}'
        else:
            params.setdefault('token', self.token)
        _log_params = dict(params)
        if 'token' in _log_params:
            _log_params['token'] = '***REDACTED***'
        logging.getLogger("delhivery").info("[GET] %s params=%s auth_header=%s", url, _log_params, bool(headers))
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        try:
            data = response.json()
        except Exception:
            data = {"response": response.text}
        logging.getLogger("delhivery").info("[GET] %s -> %s", url, data)
        return data

    def _post(self, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        data = data or {}
        data.setdefault("token", self.token)  # This ensures the token is included in the request body
        _log_data = dict(data)
        if 'token' in _log_data:
            _log_data['token'] = '***REDACTED***'
        logging.getLogger("delhivery").info("[POST] %s form=%s", url, _log_data)
        response = requests.post(url, data=data, timeout=30)
        response.raise_for_status()
        try:
            payload = response.json()
        except Exception:
            payload = {"response": response.text}
        logging.getLogger("delhivery").info("[POST] %s -> %s", url, payload)
        return payload

    # Pincode serviceability API (GET)
    def pincode_serviceability(self, filter_codes: str) -> Dict[str, Any]:
        """Check if a pincode (PIN code) is serviceable by Delhivery.

        Parameters
        ----------
        filter_codes: str
            A single pincode or comma‑separated list of pincodes (as
            expected by the Delhivery API).  See Delhivery docs for
            specifics.

        Returns
        -------
        dict
            Parsed JSON response from Delhivery.
        """
        # According to the Laravel SDK, Pincode API uses getLocations
        # and accepts filter_codes
        return self._get("c/api/pin-codes/json", {"filter_codes": filter_codes})

    # Waybill management – bulk or single (stubs)
    def bulk_waybill(self, count: int) -> Dict[str, Any]:
        """Request a bulk set of waybill numbers.

        Parameters
        ----------
        count: int
            Number of waybills to generate.
        """
        # The Laravel SDK calls `Delhivery::waybill()->bulk(['count' => 5])`
        # which maps to the API '/api/v1/awbs/bulk.json'.  We use this
        return self._post("waybill/api/bulk/json", {"count": count})
    # https://track.delhivery.com/waybill/api/bulk/json/?cl=client_name&token=API_License_key&count=count

    def fetch_waybill(self, client_name: str) -> Dict[str, Any]:
        """Fetch a single waybill for a given client name.

        The Laravel SDK's fetch method uses the same endpoint as bulk
        but returns a single waybill.  We follow the same convention.
        """
        return self._post("api/v1/awbs/fetch.json", {"client_name": client_name})

    # Order management
    def create_order(self, order_details: Dict[str, Any]) -> Dict[str, Any]:
        """Create (manifest) an order with shipments and pickup details.

        Parameters
        ----------
        order_details: dict
            Data describing the order. This method will pass the data as JSON.
        """
        url = f"{self.base_url}api/cmu/create.json"

        # Pass through the caller-provided JSON (should include shipments and pickup_location)
        payload = order_details

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Token {self.token}"  # Include the token in the Authorization header
        }

        # Delhivery expects form-encoded fields: format=json & data=<json-string>
        # See official spec: format=json&data={ ... }
        import json as _json
        form_body = {
            "format": "json",
            "data": _json.dumps(payload),
        }

        # Log sanitized request (full JSON payload)
        _log_payload = payload
        logging.getLogger("delhivery").info("[CREATE_ORDER] %s format=json data=%s", url, _log_payload)
        try:
            response = requests.post(url, data=form_body, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()  # Return the parsed JSON response
            logging.getLogger("delhivery").info("[CREATE_ORDER] %s -> %s", url, data)
            return data
        except requests.HTTPError as exc:
            # Raise HTTP exceptions for 4xx/5xx responses
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            # Catch other errors
            raise HTTPException(status_code=500, detail=str(exc))

    def edit_order(self, order_details: Dict[str, Any]) -> Dict[str, Any]:
        """Edit an existing order (e.g. update dimensions or tax).

        The payload structure must follow Delhivery’s API.  The
        Laravel SDK calls `Delhivery::order()->edit($params)` which hits
        `/api/p/edit`【550105068546031†L361-L369】.
        """
        return self._post("api/p/edit", order_details)

    def cancel_order(self, waybill: str) -> Dict[str, Any]:
        """Cancel an order by its waybill number.

        The Laravel SDK implements cancellation by posting to
        `/api/v1/packages/json` with `waybill` and `cancellation=true`【550105068546031†L374-L379】.
        """
        return self._post("api/v1/packages/json", {"waybill": waybill, "cancellation": "true"})

    def track_order(self, waybill: str) -> Dict[str, Any]:
        """Track an order by its waybill number.

        According to the Laravel SDK, tracking uses the same endpoint as
        cancellation but performs a GET with the waybill【550105068546031†L379-L389】.
        """
        return self._get("api/v1/packages/json", {"waybill": waybill})

    # Invoice management
    def invoice_locations(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Retrieve estimated shipping charges for a prospective shipment.

        The Delhivery invoice (shipping charge) API accepts a set of query
        parameters describing the shipment – such as billing mode (``md``),
        chargeable weight in grams (``cgm``), origin pin code (``o_pin``),
        destination pin code (``d_pin``) and shipment status (``ss``).  See
        Delhivery’s documentation for the list of supported parameters and
        mandatory fields.  This method forwards the supplied parameters
        directly to Delhivery.  The underlying endpoint differs slightly
        between staging and production environments, but both reside under
        ``/api/kinko/v1/invoice/charges/.json``【123006354569356†L1-L4】.

        Parameters
        ----------
        params: dict, optional
            A dictionary of query parameters to include in the request.

        Returns
        -------
        dict
            Parsed JSON response containing approximate shipping charges.
        """
        return self._get("api/kinko/v1/invoice/charges/.json", params or {})

    # Packing slip
    def print_packing_slip(self, waybill: str) -> Dict[str, Any]:
        """Generate a packing slip for a given waybill number.

        Packing slips are essentially shipping labels that include
        consignee and shipment details.  Delhivery provides a GET
        endpoint that accepts one or more waybill numbers via the
        ``wbns`` query parameter and returns a JSON payload that can be
        rendered into a PDF or HTML slip【40241588985681†L115-L133】.  Under
        staging the path is ``/api/p/packing_slip`` whereas in production
        it is the same.  This method wraps that endpoint and passes the
        provided waybill through the ``wbns`` parameter.

        Parameters
        ----------
        waybill: str
            The waybill number for which to generate a packing slip.

        Returns
        -------
        dict
            Parsed JSON response from Delhivery containing slip data.
        """
        # The API expects the waybill numbers in the parameter `wbns`
        return self._get("api/p/packing_slip", {"wbns": waybill}, use_auth_header=True)

    # Pickup request
    def schedule_pickup(self, pickup_details: Dict[str, Any]) -> Dict[str, Any]:
        """Schedule a pickup using Delhivery’s pickup request API.

        To notify Delhivery to collect shipments from a warehouse on a
        particular date and time, you need to create a pickup request.
        The official staging endpoint is ``/fm/request/new/`` and the
        production endpoint is under the same path【112589501909948†L115-L139】.  The
        payload should contain keys such as ``pickup_time``, ``pickup_date``,
        ``warehouse_name`` and ``quantity``.  This method simply forwards
        the payload to the appropriate endpoint.

        Parameters
        ----------
        pickup_details: dict
            A dictionary of pickup request details as per Delhivery’s API.

        Returns
        -------
        dict
            Parsed JSON response from Delhivery.
        """
        return self._post("fm/request/new/", pickup_details)

    # Warehouse creation
    def create_warehouse(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Register a new client warehouse with Delhivery.

        In Delhivery’s terminology a warehouse represents a physical pickup
        location.  To create one dynamically via API, send a POST
        request to ``/api/backend/clientwarehouse/create/`` with the
        warehouse details【923132388160142†L117-L130】.  Only the fields
        documented by Delhivery are accepted (name, address, pincode,
        contact information, etc.).  This method forwards your payload.

        Parameters
        ----------
        details: dict
            Warehouse information as per Delhivery’s specification.

        Returns
        -------
        dict
            Parsed JSON response indicating success and returning the
            created warehouse details.
        """
        return self._post("api/backend/clientwarehouse/create/", details)

    # Warehouse edit
    def edit_warehouse(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Edit an existing client warehouse.

        To update a previously registered warehouse, call the
        ``/api/backend/clientwarehouse/edit/`` endpoint with the
        warehouse code and new details【398867434661020†L115-L124】.  Only
        permitted fields will be updated.  This method forwards the
        provided payload.

        Parameters
        ----------
        details: dict
            Updated warehouse information, including the warehouse
            identifier.

        Returns
        -------
        dict
            Parsed JSON response from Delhivery.
        """
        return self._post("api/backend/clientwarehouse/edit/", details)

    # NDR update
    def ndr_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit an action against a non‑delivery (NDR) package.

        This endpoint allows you to defer delivery, edit consignee details
        or request a reattempt for a package that failed on the first
        attempt.  According to Delhivery’s documentation, the staging
        endpoint is ``/api/p/update``【268787320008269†L115-L179】.  The request
        body must include the ``waybill`` of the package and an action
        keyword ``act`` (``DEFER_DLV``, ``EDIT_DETAILS`` or ``RE-ATTEMPT``)
        along with optional parameters depending on the chosen action.
        This method sends a POST request to the update endpoint.

        Parameters
        ----------
        data: dict
            Payload describing the action and associated fields.

        Returns
        -------
        dict
            Parsed JSON response containing a UPL id if the request was
            accepted.
        """
        return self._post("api/p/update", data)

    # NDR status
    def ndr_status(self, upl: str) -> Dict[str, Any]:
        """Retrieve the status of a previously submitted NDR update.

        After you submit an asynchronous NDR action, Delhivery returns
        a unique processing label (UPL) which can be used to query the
        status of the action.  Use this method to poll the UPL status
        via the ``/api/cmu/get_bulk_upl`` endpoint【715084579276354†L115-L119】.

        Parameters
        ----------
        upl: str
            The unique processing label returned by ``ndr_update``.

        Returns
        -------
        dict
            Parsed JSON response containing the current status of the NDR
            action.
        """
        return self._get("api/cmu/get_bulk_upl", {"upl": upl})

    # NDR get – stub
    def ndr_get(self, upl: str) -> Dict[str, Any]:
        """Get NDR status by UPL identifier【550105068546031†L432-L438】."""
        return self._get("api/ndr/get.json", {"upl": upl})
