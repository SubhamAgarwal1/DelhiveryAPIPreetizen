import React, { useMemo, useRef, useState } from 'react';
import Papa from 'papaparse';
import axios from 'axios';

type Row = Record<string, string>;

type Shipment = {
  add: string;
  address_type?: string;
  phone: string;
  payment_mode: string;
  name: string;
  pin: string;
  state: string;
  city: string;
  country: string;
  order: string;
  consignee_gst_amount: string;
  integrated_gst_amount: string;
  gst_cess_amount: string;
  ewbn: string;
  consignee_gst_tin: string;
  hsn_code?: string;
  total_amount: number;
  weight: number | string; // in grams as per API spec
  product_desc: string;
  products_desc?: string; // ewaybill optional
  shipping_mode?: string; // Surface/Express
  quantity?: number;
};

function getVal(row: Row, keys: string[], def = ''): string {
  for (const k of keys) {
    // Check both raw and trimmed-key access in case headers had spaces
    const raw = row[k];
    if (raw !== undefined && raw !== null && String(raw).trim() !== '') return String(raw).trim();
    const trimmedKey = k.trim();
    if (trimmedKey !== k) {
      const v = row[trimmedKey as keyof Row];
      if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim();
    }
  }
  return def;
}

function toFloat(s: any, def = 0): number {
  if (s === undefined || s === null) return def;
  const n = parseFloat(String(s).replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : def;
}

function normalizePaymentMode(s: string) {
  const v = (s || '').toLowerCase();
  if (['prepaid', 'paid', 'online'].includes(v)) return 'Prepaid';
  if (['cod', 'cash on delivery'].includes(v)) return 'COD';
  if (['pickup', 'pick-up'].includes(v)) return 'Pickup';
  return 'Prepaid';
}

function buildShipment(row: Row, defaultHSN?: string): Shipment {
  const qty = toFloat(row['Quantity Ordered'], 1);
  const totalPrice = toFloat(row['Total Price']) || toFloat(row['Unit Item Price']) * Math.max(qty, 1);
  const weightRaw = row['Weight (gm)'] || row['Weight'] || '';
  let weightGm: number | string = '';
  if (weightRaw) {
    const gm = parseFloat(String(weightRaw).replace(/,/g, '').trim());
    if (Number.isFinite(gm)) weightGm = Math.round(gm);
  }

  const tm = (row['Transport Mode'] || '').toString().trim().toLowerCase();
  const shippingMode = tm === 'express' ? 'Express' : 'Surface';

  const base: Shipment = {
    add: (row['Shipping Address Line1'] || row['*Street Address'] || '').trim(),
    address_type: 'home',
    phone: (row['Customer Phone'] || row['*Phone'] || '').trim(),
    payment_mode: normalizePaymentMode(row['Payment Mode'] || row['*Payment Status'] || ''),
    name: (row['Customer Name'] || row['*First Name'] || '').trim(),
    pin: String(row['Shipping Pincode'] || row['*Postal Code'] || '').trim(),
    state: (row['Shipping State'] || '').trim(),
    city: (row['Shipping City'] || row['*City'] || '').trim(),
    country: 'India',
    order: String(row['Sale Order Number'] || row['*Order ID'] || '').trim(),
    consignee_gst_amount: process.env.NEXT_PUBLIC_CONSIGNEE_GST_AMOUNT || '150.00',
    integrated_gst_amount: process.env.NEXT_PUBLIC_INTEGRATED_GST_AMOUNT || '275.50',
    gst_cess_amount: process.env.NEXT_PUBLIC_GST_CESS_AMOUNT || '35.25',
    ewbn: '',
    consignee_gst_tin: process.env.NEXT_PUBLIC_CONSIGNEE_GST_TIN || '27ABCDE1234F1Z5',
    total_amount: Math.round(totalPrice * 100) / 100,
    weight: weightGm,
    product_desc: (row['Item Sku Name'] || row['Translated Name'] || '').trim(),
    products_desc: (row['Item Sku Name'] || row['Translated Name'] || '').trim(),
    shipping_mode: shippingMode,
    quantity: Math.max(1, Math.round(qty)),
  };

  if (defaultHSN && defaultHSN.trim()) (base as any).hsn_code = defaultHSN.trim();
  return base;
}

function unparseCSV(rows: any[]): string {
  return Papa.unparse(rows, { quotes: true });
}

function download(filename: string, text: string) {
  const blob = new Blob([text], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export default function Home() {
  const [rows, setRows] = useState<Row[]>([]);
  const [headers, setHeaders] = useState<string[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const defaultPickupEnv = process.env.NEXT_PUBLIC_PICKUP_NAME || 'Preetizen Lifestyle';
  const defaultPickupCity = process.env.NEXT_PUBLIC_PICKUP_CITY || 'Kolkata';
  const defaultPickupPin = process.env.NEXT_PUBLIC_PICKUP_PIN || '700107';
  const defaultPickupCountry = process.env.NEXT_PUBLIC_PICKUP_COUNTRY || 'India';
  const defaultHSNEnv = process.env.NEXT_PUBLIC_HSN_CODE || '851770';

  const [pickup, setPickup] = useState<string>(defaultPickupEnv);
  const [resultCSV, setResultCSV] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [previewJSON, setPreviewJSON] = useState<any | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  // Use relative '/api' and let Next rewrites forward to backend
  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL || '/api';

  const uniquePickups = useMemo(() => {
    const s = new Set<string>();
    rows.forEach((r) => {
      const v = (r['Pickup Location Name'] || '').trim();
      if (v) s.add(v);
    });
    return Array.from(s);
  }, [rows]);

  const allSelected = rows.length > 0 && selected.size === rows.length;

  async function refreshFromDB() {
    try {
      const resp = await axios.get(`${apiBase}/orders`);
      const items: Row[] = resp.data?.items || [];
      setRows(items);
      const fields = Array.from(new Set(items.flatMap((r) => Object.keys(r))));
      setHeaders(fields);
      setSelected(new Set());
    } catch (e: any) {
      console.error('[ORDERS] fetch failed', e?.response?.data || e.message);
    }
  }

  React.useEffect(() => {
    refreshFromDB();
  }, []);

  function handleFile(file: File) {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (header: string) => header.replace(/^\uFEFF/, '').trim(),
      complete: (res: any) => {
        const rowsParsed: Row[] = Array.isArray(res?.data) ? (res.data as Row[]) : ([] as Row[]);
        const data: Row[] = rowsParsed.filter((r: Row) =>
          Boolean(r['Sale Order Number'] || r['*Order ID'] || getVal(r, ['Sale Order Number', '*Order ID']))
        );
        console.log('[CSV] Parsed rows:', rowsParsed.length);
        console.log('[CSV] Filtered orders:', data.length);
        setRows(data);
        // Persist rows to backend DB for tracking/waybills
        axios
          .post(`${apiBase}/orders/import`, { rows: data })
          .then((resp) => {
            console.log('[IMPORT] Imported to DB:', resp.data);
            refreshFromDB();
          })
          .catch((err: any) => {
            console.warn('[IMPORT] Import to DB failed:', err?.response?.data || err.message);
          });
        // Preserve CSV header order if provided by Papa
        const meta: any = res.meta || {};
        const fields: string[] = Array.isArray(meta.fields)
          ? meta.fields.map((h: string) => String(h).replace(/^\uFEFF/, '').trim())
          : Array.from(
              new Set(
                data.flatMap((r: Row) => Object.keys(r).map((k) => k.replace(/^\uFEFF/, '').trim()))
              )
            );
        setHeaders(fields);
        // Infer pickup
        // Pickup is fixed; prefer CSV value only if present; otherwise fixed default
        const csvPickup = data.find((r) => getVal(r, ['Pickup Location Name']))?.['Pickup Location Name'] || '';
        setPickup(csvPickup || defaultPickupEnv);
        setSelected(new Set());
        setResultCSV(null);
      },
      error: (err: any) =>  {
        console.error('[CSV] Parse error:', err);
        alert(`Failed to parse CSV: ${err.message}`);
      },
    });
  }

  function toggleAll(next: boolean) {
    if (next) setSelected(new Set(rows.map((r) => String(r['Sale Order Number'] || r['*Order ID']))));
    else setSelected(new Set());
  }

  function toggleOne(id: string, next: boolean) {
    const s = new Set(selected);
    if (next) s.add(id); else s.delete(id);
    setSelected(s);
  }

  async function handleCreateManifest() {
    const chosen = rows.filter((r) => selected.has(String(r['Sale Order Number'] || r['*Order ID'])));
    if (!chosen.length) {
      alert('Please select at least one order');
      return;
    }
    const sale_order_numbers = chosen.map((r) => String(r['Sale Order Number'] || r['*Order ID']));
    const payload = { sale_order_numbers };
    console.log('[MANIFEST] Requesting manifest-from-db for:', sale_order_numbers);

    setBusy(true);
    try {
      const resp = await axios.post(`${apiBase}/orders/manifest-from-db`, payload, { headers: { 'Content-Type': 'application/json' } });
      const data = resp.data || {};
      console.log('[MANIFEST] Response from backend:', data);

      // Try to extract waybills from various likely shapes
      const results: Array<{ order: string; waybill: string; status?: string }> = [];

      const byOrder = new Map<string, string>();

      // Common: data.packages as array of {waybill: ...}
      if (Array.isArray(data.packages)) {
        for (const p of data.packages) {
          const wb = String(p.waybill || p.wbn || p.awb || '').trim();
          const ord = String(p.order || p.order_id || '').trim();
          if (ord && wb) byOrder.set(ord, wb);
        }
      }

      // Fallback: if data.shipments is array
      if (Array.isArray(data.shipments)) {
        for (const p of data.shipments) {
          const wb = String(p.waybill || p.wbn || p.awb || '').trim();
          const ord = String(p.order || p.order_id || '').trim();
          if (ord && wb) byOrder.set(ord, wb);
        }
      }

      // Build rows using selected orders as source of truth
      for (const ord of sale_order_numbers) {
        results.push({ order: ord, waybill: byOrder.get(ord) || '' });
      }

      const csv = unparseCSV(results);
      setResultCSV(csv);
    } catch (err: any) {
      console.error(err);
      alert(`Manifest API failed: ${err?.response?.data?.detail || err.message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, sans-serif' }}>
      <h1>Delhivery Orders - CSV Import & Manifest</h1>
      <div style={{ marginTop: 16, marginBottom: 16 }}>
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,text/csv"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) handleFile(f);
          }}
        />
      </div>

      {rows.length > 0 && (
        <div>
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginBottom: 12 }}>
            <div style={{ fontSize: 14, color: '#444' }}>
              Pickup: {defaultPickupEnv}, {defaultPickupCity} {defaultPickupPin}, {defaultPickupCountry}
            </div>

            <button onClick={handleCreateManifest} disabled={busy}>
              {busy ? 'Creating…' : 'Create Manifest'}
            </button>

            {resultCSV && (
              <button onClick={() => download('manifest_waybills.csv', resultCSV)}>Export Waybills CSV</button>
            )}
            <button
              onClick={async () => {
                const chosen = rows.filter((r) => selected.has(String(r['Sale Order Number'] || r['*Order ID'])));
                if (!chosen.length) return alert('Select orders first');
                const sale_order_numbers = chosen.map((r) => String(r['Sale Order Number'] || r['*Order ID']));
                try {
                  const resp = await axios.post(`${apiBase}/orders/build-manifest`, { sale_order_numbers });
                  setPreviewJSON(resp.data);
                  console.log('[PREVIEW] Manifest payload:', resp.data);
                } catch (e: any) {
                  alert(`Preview failed: ${e?.response?.data?.detail || e.message}`);
                }
              }}
              disabled={busy}
            >
              Preview JSON
            </button>
          </div>

          <div style={{ overflow: 'auto', maxHeight: 500, border: '1px solid #ddd' }}>
            <table style={{ borderCollapse: 'collapse', width: '100%', whiteSpace: 'nowrap' as const }}>
              <thead>
                <tr>
                  <th style={{ borderBottom: '1px solid #ccc', padding: 8 }}>
                    <input type="checkbox" checked={allSelected} onChange={(e) => toggleAll(e.target.checked)} />
                  </th>
                  {headers.map((h) => (
                    <th key={h} style={{ borderBottom: '1px solid #ccc', padding: 8 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const id = String(r['Sale Order Number'] || r['*Order ID']);
                  return (
                    <tr key={id}>
                      <td style={{ borderBottom: '1px solid #eee', padding: 8, textAlign: 'center' }}>
                        <input
                          type="checkbox"
                          checked={selected.has(id)}
                          onChange={(e) => toggleOne(id, e.target.checked)}
                        />
                      </td>
                      {headers.map((h) => (
                        <td key={h} style={{ borderBottom: '1px solid #eee', padding: 8 }}>
                          {r[h] ?? ''}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {previewJSON && (
        <div style={{ marginTop: 16 }}>
          <h3>Preview Manifest JSON</h3>
          <pre style={{ maxHeight: 300, overflow: 'auto', background: '#f6f8fa', padding: 12 }}>
            {JSON.stringify(previewJSON, null, 2)}
          </pre>
          <button onClick={() => navigator.clipboard.writeText(JSON.stringify(previewJSON, null, 2))}>Copy</button>
          <button onClick={() => download('manifest_payload.json', JSON.stringify(previewJSON, null, 2))}>Download</button>
        </div>
      )}
    </div>
  );
}


