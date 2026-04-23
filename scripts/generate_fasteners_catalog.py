"""Generate a self-contained HTML catalog from the Firestore fasteners data.

Reads from the `steel-sections` Firestore database (populated by
`load_fasteners.py`) and writes a single-file HTML catalog to
`catalog.html` at the project root.

Features
--------
- Summary KPIs (totals, inventory value, ordered qty)
- Fastener master table with live search + type/thread/material filters
- Vendor pricelist browser (TPC pivot, Abpon flat)
- Config assembly reference
- Total-order rollup + purchase order archive
- Orders drawer per fastener (click a row)

Everything is embedded as inline JSON — no network calls at runtime.
"""
from __future__ import annotations

import html
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import firestore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("catalog")

GCP_PROJECT_ID = "ai-agents-go"
FIRESTORE_DATABASE = "steel-sections"


def _credentials_path() -> str:
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if env and Path(env).exists():
        return env
    creds_dir = Path("C:/Users/Eukrit/OneDrive/Documents/Claude Code/Credentials Claude Code")
    candidates = sorted(creds_dir.glob("ai-agents-go-*.json"))
    if candidates:
        chosen = str(candidates[-1]).replace("\\", "/")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = chosen
        return chosen
    raise FileNotFoundError("ai-agents-go SA key not found")


def fetch_collection(db: firestore.Client, name: str) -> list[dict]:
    return [{"_id": d.id, **d.to_dict()} for d in db.collection(name).stream()]


def fetch_fasteners_with_orders(db: firestore.Client) -> list[dict]:
    """Fetch every fastener doc plus its orders subcollection."""
    out = []
    for d in db.collection("fasteners").stream():
        data = d.to_dict() or {}
        data["_id"] = d.id
        orders = [o.to_dict() | {"_id": o.id} for o in d.reference.collection("orders").stream()]
        orders.sort(key=lambda o: o.get("column_index", 0))
        data["orders"] = orders
        out.append(data)
    out.sort(key=lambda x: (x.get("type_code") or "", x.get("model") or ""))
    return out


def _json_default(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def render(
    fasteners: list[dict],
    types: list[dict],
    threads: list[dict],
    config: list[dict],
    fittings: list[dict],
    pricelist: list[dict],
    vendor_pricelists: list[dict],
    orders_meta: list[dict],
    total_orders: list[dict],
    purchase_orders: list[dict],
    sanko_products: list[dict],
    sanko_prices: list[dict],
    sanko_quotations: list[dict],
    sanko_purchase_orders: list[dict],
    sanko_documents: list[dict],
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Summary KPIs
    total_models = len(fasteners)
    total_order_qty = sum((f.get("total_ordered") or 0) for f in fasteners)
    total_order_lines = sum(len(f.get("orders") or []) for f in fasteners)
    priced_models = sum(1 for f in fasteners if (f.get("cost_thb") or 0) > 0)
    inventory_value = sum(
        (f.get("cost_thb") or 0) * (f.get("total_ordered") or 0) for f in fasteners
    )

    # Derive dropdown options
    type_options = sorted({f.get("type_code", "") for f in fasteners if f.get("type_code")})
    thread_options = sorted(
        {f.get("thread", "") for f in fasteners if f.get("thread")},
        key=lambda t: (len(t), t),
    )
    # Material lookup by type_code
    mat_by_type = {t["type_code"]: t.get("material", "") for t in types}
    desc_by_type = {t["type_code"]: t.get("description_th", "") for t in types}
    material_options = sorted({v for v in mat_by_type.values() if v})

    # Enrich fastener rows with material
    for f in fasteners:
        f["material"] = mat_by_type.get(f.get("type_code", ""), "")
        f["type_description_th"] = desc_by_type.get(f.get("type_code", ""), "")

    # Sanko-specific dropdown options
    sanko_categories = sorted({p.get("category", "") for p in sanko_products if p.get("category")})

    # Payload for JS
    payload = {
        "generated_at": now,
        "fasteners": fasteners,
        "types": types,
        "sanko_products": sanko_products,
        "sanko_prices": sanko_prices,
        "sanko_quotations": sanko_quotations,
        "sanko_purchase_orders": sanko_purchase_orders,
        "sanko_documents": sanko_documents,
        "threads": threads,
        "config": config,
        "fittings": fittings,
        "pricelist": pricelist,
        "vendor_pricelists": vendor_pricelists,
        "orders_meta": orders_meta,
        "total_orders": total_orders,
        "purchase_orders": purchase_orders,
    }
    # JSON inside <script> is raw text in HTML5 — entities are NOT decoded.
    # Only thing we must neutralize is the literal closing tag '</script>'
    # (and defensively '<!--' / '-->' which can also terminate scripts in some parsers).
    payload_json = json.dumps(payload, default=_json_default, ensure_ascii=False)
    payload_json = (
        payload_json
        .replace("</", "<\\/")
        .replace("<!--", "<\\!--")
        .replace("-->", "--\\>")
    )

    type_opts_html = "".join(f'<option value="{html.escape(t)}">{html.escape(t)}</option>' for t in type_options)
    thread_opts_html = "".join(f'<option value="{html.escape(t)}">{html.escape(t)}</option>' for t in thread_options)
    material_opts_html = "".join(f'<option value="{html.escape(m)}">{html.escape(m)}</option>' for m in material_options)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Fasteners Catalog — GO Corporation</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
  :root {{
    --bg:#0b1021; --panel:#111731; --ink:#e5e7eb; --muted:#94a3b8;
    --accent:#60a5fa; --accent-2:#22d3ee; --border:#1f2a4a;
    --chip:#1e293b; --chip-ink:#cbd5e1;
    --ok:#34d399; --warn:#f59e0b; --err:#f87171;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--ink);line-height:1.4}}
  header{{padding:1.5rem 2rem;border-bottom:1px solid var(--border);background:linear-gradient(180deg,#0f1530,#0b1021)}}
  header h1{{margin:0;font-size:1.6rem;letter-spacing:-0.01em}}
  header .sub{{color:var(--muted);font-size:0.9rem;margin-top:0.25rem}}
  .wrap{{max-width:1400px;margin:0 auto;padding:1.5rem 2rem}}
  .tabs{{display:flex;gap:0.25rem;margin-bottom:1rem;border-bottom:1px solid var(--border);flex-wrap:wrap}}
  .tab{{padding:0.6rem 1rem;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;font-size:0.9rem;white-space:nowrap}}
  .tab.active{{color:var(--accent);border-bottom-color:var(--accent);font-weight:600}}
  .tab:hover{{color:var(--ink)}}
  .panel{{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:1.25rem;margin-bottom:1rem}}
  .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:0.75rem;margin-bottom:1rem}}
  .kpi{{background:var(--panel);border:1px solid var(--border);padding:1rem;border-radius:10px}}
  .kpi .label{{color:var(--muted);font-size:0.8rem;text-transform:uppercase;letter-spacing:0.05em}}
  .kpi .value{{font-size:1.7rem;font-weight:700;margin-top:0.25rem}}
  .kpi .hint{{color:var(--muted);font-size:0.8rem;margin-top:0.25rem}}
  .filters{{display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem;align-items:center}}
  .filters input, .filters select{{background:var(--panel);border:1px solid var(--border);color:var(--ink);padding:0.5rem 0.75rem;border-radius:8px;font-size:0.9rem}}
  .filters input:focus, .filters select:focus{{outline:none;border-color:var(--accent)}}
  .filters input[type=search]{{flex:1;min-width:240px}}
  .count{{color:var(--muted);font-size:0.85rem}}
  table{{width:100%;border-collapse:collapse;font-size:0.88rem}}
  th{{text-align:left;padding:0.6rem 0.75rem;background:#0e1430;color:var(--muted);font-weight:600;font-size:0.78rem;text-transform:uppercase;letter-spacing:0.03em;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:1}}
  td{{padding:0.6rem 0.75rem;border-bottom:1px solid var(--border);vertical-align:top}}
  tr:hover td{{background:#131a3a}}
  .num{{text-align:right;font-variant-numeric:tabular-nums}}
  .chip{{display:inline-block;padding:1px 8px;border-radius:999px;background:var(--chip);color:var(--chip-ink);font-size:0.75rem;margin-right:4px;white-space:nowrap}}
  .chip.acc{{background:#172554;color:#bfdbfe}}
  .chip.ok{{background:#064e3b;color:#6ee7b7}}
  .clickable{{cursor:pointer}}
  .table-wrap{{max-height:640px;overflow:auto;border:1px solid var(--border);border-radius:10px}}
  dialog{{background:var(--panel);color:var(--ink);border:1px solid var(--border);border-radius:12px;padding:0;max-width:800px;width:90%}}
  dialog::backdrop{{background:rgba(0,0,0,0.6)}}
  .dialog-head{{padding:1rem 1.25rem;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}}
  .dialog-body{{padding:1rem 1.25rem;max-height:70vh;overflow:auto}}
  .close-btn{{background:transparent;color:var(--muted);border:none;font-size:1.2rem;cursor:pointer}}
  .tab-pane{{display:none}}
  .tab-pane.active{{display:block}}
  h2{{margin:0 0 0.75rem;font-size:1.1rem}}
  .pivot{{overflow:auto}}
  .pivot th:first-child, .pivot td:first-child{{position:sticky;left:0;background:#0e1430;z-index:2}}
  .pivot td:first-child{{background:#111731}}
  .muted{{color:var(--muted)}}
  details summary{{cursor:pointer;color:var(--accent);margin:0.5rem 0}}
  pre{{background:#0a0f24;padding:0.5rem 0.75rem;border-radius:6px;overflow:auto;font-size:0.78rem}}
  .empty{{padding:2rem;text-align:center;color:var(--muted)}}
</style>
</head>
<body>
<header>
  <h1>Fasteners Catalog</h1>
  <div class="sub">GO Corporation · Fasteners Schedule Ver. 2024 · generated {html.escape(now)}</div>
</header>

<div class="wrap">

  <div class="kpi-grid">
    <div class="kpi"><div class="label">Models</div><div class="value">{total_models:,}</div><div class="hint">{priced_models:,} with cost</div></div>
    <div class="kpi"><div class="label">Total ordered qty</div><div class="value">{int(total_order_qty):,}</div><div class="hint">{total_order_lines:,} order lines</div></div>
    <div class="kpi"><div class="label">Inventory value</div><div class="value">฿{inventory_value:,.0f}</div><div class="hint">cost × total ordered</div></div>
    <div class="kpi"><div class="label">Vendors priced</div><div class="value">{len(vendor_pricelists)}</div><div class="hint">TPC · Abpon</div></div>
    <div class="kpi"><div class="label">Purchase orders</div><div class="value">{len(purchase_orders)}</div><div class="hint">SO21 / SO22 / SO23</div></div>
    <div class="kpi"><div class="label">Sanko products</div><div class="value">{len(sanko_products)}</div><div class="hint">{len(sanko_documents)} docs · {len(sanko_quotations)} QTPs</div></div>
  </div>

  <div class="tabs" id="tabs">
    <div class="tab active" data-tab="catalog">Catalog</div>
    <div class="tab" data-tab="vendors">Vendor Pricelists</div>
    <div class="tab" data-tab="sanko">Sanko</div>
    <div class="tab" data-tab="config">Assembly Config</div>
    <div class="tab" data-tab="fittings">Fittings</div>
    <div class="tab" data-tab="pricelist">Misc Pricelist</div>
    <div class="tab" data-tab="total-orders">Total Orders</div>
    <div class="tab" data-tab="pos">Purchase Orders</div>
    <div class="tab" data-tab="reference">Reference</div>
  </div>

  <section class="tab-pane active" id="tab-catalog">
    <div class="filters">
      <input id="q" type="search" placeholder="Search model, description, thread…"/>
      <select id="f-type"><option value="">All types</option>{type_opts_html}</select>
      <select id="f-thread"><option value="">All threads</option>{thread_opts_html}</select>
      <select id="f-material"><option value="">All materials</option>{material_opts_html}</select>
      <label class="muted"><input type="checkbox" id="f-priced"/> Priced only</label>
      <label class="muted"><input type="checkbox" id="f-ordered"/> Ordered only</label>
      <span class="count" id="catalog-count"></span>
    </div>
    <div class="table-wrap">
      <table id="catalog-tbl">
        <thead><tr>
          <th>Type</th><th>Model</th><th>Thread</th><th>Len (mm)</th><th>Material</th>
          <th>Description</th>
          <th class="num">List ฿</th><th class="num">Disc %</th><th class="num">Cost ฿</th>
          <th class="num">Ordered</th><th class="num">Orders</th><th>Price date</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="tab-pane" id="tab-vendors">
    <div class="filters">
      <select id="vendor-pick"></select>
    </div>
    <div id="vendor-content"></div>
  </section>

  <section class="tab-pane" id="tab-sanko">
    <div class="kpi-grid">
      <div class="kpi"><div class="label">Sanko products</div><div class="value">{len(sanko_products)}</div><div class="hint">{sum(1 for p in sanko_products if p.get('latest_unit_price_thb'))} priced</div></div>
      <div class="kpi"><div class="label">Pricelist rows</div><div class="value">{len(sanko_prices)}</div><div class="hint">drop-in + drill bits</div></div>
      <div class="kpi"><div class="label">Quotations</div><div class="value">{len(sanko_quotations)}</div><div class="hint">QTP* + ใบเสนอราคา</div></div>
      <div class="kpi"><div class="label">POs / invoices</div><div class="value">{len(sanko_purchase_orders)}</div><div class="hint">PO-* / PI-*</div></div>
      <div class="kpi"><div class="label">Documents</div><div class="value">{len(sanko_documents)}</div><div class="hint">PDFs · images · videos</div></div>
    </div>

    <div class="panel">
      <h2>Products</h2>
      <div class="filters">
        <input id="sanko-q" type="search" placeholder="Search code, family, description…"/>
        <select id="sanko-cat"><option value="">All categories</option>{"".join(f'<option value="{html.escape(c)}">{html.escape(c)}</option>' for c in sanko_categories)}</select>
        <label class="muted"><input type="checkbox" id="sanko-priced"/> Priced only</label>
        <span class="count" id="sanko-count"></span>
      </div>
      <div class="table-wrap">
        <table id="sanko-products-tbl">
          <thead><tr>
            <th>Code</th><th>Category</th><th>Family</th><th>Description</th>
            <th class="num">Latest ฿</th><th class="num">Min ฿</th><th class="num">Max ฿</th>
            <th class="num">Sources</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>Pricelist rows</h2>
      <div class="table-wrap" style="max-height:380px">
        <table id="sanko-prices-tbl">
          <thead><tr>
            <th>Code</th><th>Family</th><th>Description / Size</th>
            <th class="num">List ฿</th><th class="num">Net ฿</th><th class="num">Disc %</th><th>Date</th><th>Source</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>Quotations &amp; purchase orders</h2>
      <div class="filters">
        <select id="sanko-qpick"></select>
      </div>
      <div id="sanko-qcontent"></div>
    </div>

    <div class="panel">
      <h2>All Sanko documents</h2>
      <div class="filters">
        <input id="sanko-doc-q" type="search" placeholder="Filter file name / folder…"/>
      </div>
      <div class="table-wrap" style="max-height:380px">
        <table id="sanko-docs-tbl">
          <thead><tr>
            <th>Date</th><th>Folder</th><th>File</th><th>Category</th><th class="num">Size</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </section>

  <section class="tab-pane" id="tab-config">
    <div class="filters">
      <input id="config-q" type="search" placeholder="Filter config…"/>
    </div>
    <div class="table-wrap">
      <table id="config-tbl">
        <thead><tr>
          <th>Section</th><th>Type+Size</th><th>Size</th><th>Design</th>
          <th>Nut</th><th>Nut qty</th><th>Spring</th><th>Spring qty</th>
          <th>Washer</th><th>Washer qty</th><th>Bolt head</th><th>Thread pitch</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="tab-pane" id="tab-fittings">
    <div class="table-wrap">
      <table id="fittings-tbl">
        <thead><tr>
          <th>Section</th><th>Model</th><th>Project</th><th>Size</th><th>Length</th>
          <th>Qty/set</th><th>Stock</th><th>Order</th><th>Full label</th><th>Spacing</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="tab-pane" id="tab-pricelist">
    <div class="table-wrap">
      <table id="pricelist-tbl">
        <thead><tr>
          <th>Model</th><th>Description</th><th>Code</th>
          <th class="num">Unit cost ฿</th><th>Qty/set</th><th class="num">Set amount ฿</th><th class="num">Total ฿</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="tab-pane" id="tab-total-orders">
    <div class="filters">
      <input id="totord-q" type="search" placeholder="Filter…"/>
    </div>
    <div class="table-wrap">
      <table id="total-orders-tbl">
        <thead><tr>
          <th>Section</th><th>Full label</th><th>Description</th><th>Size</th><th>Length</th>
          <th class="num">Required</th><th class="num">Extra</th><th class="num">Stock</th><th class="num">Actual order</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="tab-pane" id="tab-pos">
    <div class="filters">
      <select id="po-pick"></select>
    </div>
    <div id="po-content"></div>
  </section>

  <section class="tab-pane" id="tab-reference">
    <div class="panel">
      <h2>Fastener Types</h2>
      <div class="table-wrap">
        <table id="types-tbl"><thead><tr><th>Type</th><th>Material</th><th>Description (TH)</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
    <div class="panel">
      <h2>Thread pitches</h2>
      <div class="table-wrap">
        <table id="threads-tbl"><thead><tr><th>Thread</th><th class="num">Pitch (mm)</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
    <div class="panel">
      <h2>Order metadata (Fasteners sheet columns)</h2>
      <div class="table-wrap">
        <table id="orders-meta-tbl"><thead><tr><th>Order</th><th>Col</th><th>Project</th><th>Date</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
  </section>

</div>

<dialog id="orders-dialog">
  <div class="dialog-head">
    <strong id="orders-dialog-title"></strong>
    <button class="close-btn" onclick="document.getElementById('orders-dialog').close()">✕</button>
  </div>
  <div class="dialog-body" id="orders-dialog-body"></div>
</dialog>

<script id="payload" type="application/json">{payload_json}</script>
<script>
const DATA = JSON.parse(document.getElementById('payload').textContent);

// ---------- tabs ----------
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => {{
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById('tab-' + t.dataset.tab).classList.add('active');
}}));

// ---------- helpers ----------
const fmt = (n, d=0) => (n == null || n === '' || isNaN(n)) ? '' : Number(n).toLocaleString('en-US', {{minimumFractionDigits:d, maximumFractionDigits:d}});
const esc = s => (s == null ? '' : String(s).replace(/[<>&"']/g, c => ({{'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}}[c])));

// ---------- catalog ----------
const $q = document.getElementById('q');
const $type = document.getElementById('f-type');
const $thread = document.getElementById('f-thread');
const $material = document.getElementById('f-material');
const $priced = document.getElementById('f-priced');
const $ordered = document.getElementById('f-ordered');
const $count = document.getElementById('catalog-count');
const $tbody = document.querySelector('#catalog-tbl tbody');

function renderCatalog() {{
  const q = ($q.value || '').toLowerCase().trim();
  const ft = $type.value, fth = $thread.value, fm = $material.value;
  const priced = $priced.checked, ordered = $ordered.checked;
  const rows = DATA.fasteners.filter(f => {{
    if (ft && f.type_code !== ft) return false;
    if (fth && f.thread !== fth) return false;
    if (fm && f.material !== fm) return false;
    if (priced && !(f.cost_thb > 0)) return false;
    if (ordered && !(f.total_ordered > 0)) return false;
    if (q) {{
      const hay = [f.model, f.description, f.thread, f.type_code, f.material].join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }}
    return true;
  }});
  $count.textContent = `${{rows.length}} of ${{DATA.fasteners.length}} models`;
  $tbody.innerHTML = rows.map(f => `
    <tr class="clickable" data-model="${{esc(f.model)}}">
      <td><span class="chip">${{esc(f.type_code || '')}}</span></td>
      <td><strong>${{esc(f.model)}}</strong></td>
      <td>${{esc(f.thread || '')}}</td>
      <td class="num">${{fmt(f.length_mm)}}</td>
      <td>${{esc(f.material || '')}}</td>
      <td>${{esc(f.description || '')}}</td>
      <td class="num">${{fmt(f.list_price_thb, 2)}}</td>
      <td class="num">${{f.discount_pct != null ? fmt(f.discount_pct, 0) + '%' : ''}}</td>
      <td class="num">${{fmt(f.cost_thb, 2)}}</td>
      <td class="num">${{fmt(f.total_ordered)}}</td>
      <td class="num">${{fmt(f.order_count)}}</td>
      <td>${{esc(f.last_price_date || '')}}</td>
    </tr>`).join('') || '<tr><td colspan="12" class="empty">No matches</td></tr>';
}}
[$q,$type,$thread,$material,$priced,$ordered].forEach(el => el.addEventListener('input', renderCatalog));
$tbody.addEventListener('click', e => {{
  const tr = e.target.closest('tr[data-model]');
  if (!tr) return;
  const model = tr.dataset.model;
  const f = DATA.fasteners.find(x => x.model === model);
  if (!f) return;
  const orders = f.orders || [];
  document.getElementById('orders-dialog-title').textContent = f.model + ' — ' + orders.length + ' order line(s)';
  const body = document.getElementById('orders-dialog-body');
  if (orders.length === 0) {{
    body.innerHTML = '<div class="empty">No orders recorded.</div>';
  }} else {{
    body.innerHTML = '<table><thead><tr><th>Order</th><th>Project</th><th>Date</th><th class="num">Qty</th></tr></thead><tbody>' +
      orders.map(o => `<tr><td>${{esc(o.order_label||'')}}</td><td>${{esc(o.project||'')}}</td><td>${{esc(o.date_raw||'')}}</td><td class="num">${{fmt(o.qty)}}</td></tr>`).join('') +
      '</tbody></table>';
  }}
  document.getElementById('orders-dialog').showModal();
}});
renderCatalog();

// ---------- vendor pricelists ----------
const vendors = DATA.vendor_pricelists || [];
const $vpick = document.getElementById('vendor-pick');
$vpick.innerHTML = vendors.map((v,i) => `<option value="${{i}}">${{esc(v.vendor)}} — ${{esc(v.product)}}</option>`).join('');
function renderVendor() {{
  const v = vendors[Number($vpick.value || 0)];
  if (!v) return;
  const c = document.getElementById('vendor-content');
  if (v.vendor === 'TPC') {{
    // Pivot: length × size
    const prices = v.prices || [];
    const lengths = Array.from(new Set(prices.map(p => p.length)));
    const sizes = Array.from(new Set(prices.map(p => p.size)));
    const grid = {{}};
    for (const p of prices) grid[p.length + '|' + p.size] = p;
    let html = `<div class="panel"><h2>${{esc(v.vendor)}} — ${{esc(v.product)}}</h2>`;
    html += `<div class="muted" style="margin-bottom:0.75rem">${{esc(v.description||'')}} · ${{prices.length}} price points</div>`;
    html += `<div class="pivot table-wrap"><table><thead><tr><th>Length ↓ / Size →</th>`;
    html += sizes.map(s => `<th class="num">${{esc(s)}}</th>`).join('') + '</tr></thead><tbody>';
    for (const L of lengths) {{
      html += `<tr><td><strong>${{esc(L)}}</strong></td>`;
      for (const S of sizes) {{
        const p = grid[L+'|'+S];
        html += `<td class="num" title="${{p ? esc(p.pack||'') : ''}}">${{p ? fmt(p.unit_price_thb, 2) : ''}}</td>`;
      }}
      html += '</tr>';
    }}
    html += '</tbody></table></div></div>';
    c.innerHTML = html;
  }} else {{
    // Flat table
    const prices = v.prices || [];
    let html = `<div class="panel"><h2>${{esc(v.vendor)}} — ${{esc(v.product)}}</h2>`;
    html += '<div class="table-wrap"><table><thead><tr><th>Item (TH)</th><th>Material</th><th>Detail</th><th>Thread</th><th>Length</th><th>OD</th><th class="num">List ฿</th><th class="num">Disc %</th><th class="num">Net ฿</th><th>Unit</th></tr></thead><tbody>';
    html += prices.map(p => `<tr><td>${{esc(p.item_th||'')}}</td><td>${{esc(p.material||'')}}</td><td>${{esc(p.detail||'')}}</td><td>${{esc(p.thread_mm || p.thread_inch || '')}}</td><td>${{esc(p.length_mm || p.length_inch || '')}}</td><td>${{esc(p.od_mm||'')}}</td><td class="num">${{fmt(p.list_price_thb,2)}}</td><td class="num">${{fmt(p.discount_pct,0)}}%</td><td class="num">${{fmt(p.net_price_thb,2)}}</td><td>${{esc(p.unit||'')}}</td></tr>`).join('');
    html += '</tbody></table></div></div>';
    c.innerHTML = html;
  }}
}}
$vpick.addEventListener('change', renderVendor);
renderVendor();

// ---------- sanko ----------
const sankoProds = DATA.sanko_products || [];
const sankoPrices = DATA.sanko_prices || [];
const sankoQs = (DATA.sanko_quotations || []).concat(DATA.sanko_purchase_orders || []);
const sankoDocs = DATA.sanko_documents || [];

const $sq = document.getElementById('sanko-q');
const $scat = document.getElementById('sanko-cat');
const $spriced = document.getElementById('sanko-priced');
const $scount = document.getElementById('sanko-count');
function renderSankoProducts() {{
  const q = ($sq.value||'').toLowerCase().trim();
  const cat = $scat.value;
  const priced = $spriced.checked;
  const rows = sankoProds.filter(p => {{
    if (cat && p.category !== cat) return false;
    if (priced && !(p.latest_unit_price_thb > 0)) return false;
    if (q) {{
      const hay = [p.product_code, p.family, p.description, ...(p.descriptions||[])].join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }}
    return true;
  }});
  $scount.textContent = `${{rows.length}} of ${{sankoProds.length}} products`;
  document.querySelector('#sanko-products-tbl tbody').innerHTML = rows.map(p => `
    <tr>
      <td><strong>${{esc(p.product_code||'')}}</strong></td>
      <td><span class="chip">${{esc(p.category||'')}}</span></td>
      <td>${{esc(p.family||'')}}</td>
      <td class="muted">${{esc(p.description||'')}}</td>
      <td class="num">${{fmt(p.latest_unit_price_thb,2)}}</td>
      <td class="num">${{fmt(p.min_unit_price_thb,2)}}</td>
      <td class="num">${{fmt(p.max_unit_price_thb,2)}}</td>
      <td class="num">${{fmt(p.source_count)}}</td>
    </tr>`).join('') || '<tr><td colspan="8" class="empty">No matches</td></tr>';
}}
[$sq,$scat,$spriced].forEach(el => el.addEventListener('input', renderSankoProducts));
renderSankoProducts();

// Sanko prices table
document.querySelector('#sanko-prices-tbl tbody').innerHTML = sankoPrices.map(r => `
  <tr>
    <td><strong>${{esc(r.product_code||'')}}</strong></td>
    <td>${{esc(r.product_family_hint||'')}}</td>
    <td>${{esc(r.description || r.size || '')}}</td>
    <td class="num">${{fmt(r.list_price_thb,2)}}</td>
    <td class="num">${{fmt(r.net_price_thb,2)}}</td>
    <td class="num">${{r.discount_pct != null ? fmt(r.discount_pct,0)+'%' : ''}}</td>
    <td>${{esc(r.pricelist_date||'')}}</td>
    <td class="muted">${{esc(r.source||'')}}</td>
  </tr>`).join('');

// Sanko quotations / POs picker
const $sqp = document.getElementById('sanko-qpick');
const sankoQsSorted = [...sankoQs].sort((a,b) => (b.doc_date||'').localeCompare(a.doc_date||''));
$sqp.innerHTML = sankoQsSorted.map((q,i) =>
  `<option value="${{i}}">${{esc(q.doc_date||'')}} · ${{esc(q.doc_id||'')}} (${{esc(q.doc_type||'')}}, ${{q.item_count||0}} items)</option>`).join('');
function renderSankoDoc() {{
  const q = sankoQsSorted[Number($sqp.value||0)];
  if (!q) return;
  let h = `<div class="muted" style="margin-bottom:0.5rem">Source: ${{esc(q.source_file||'')}}</div>`;
  if ((q.items||[]).length === 0) {{
    h += '<div class="empty">No structured line items extracted (likely image-based PDF).</div>';
  }} else {{
    h += '<div class="table-wrap"><table><thead><tr><th>#</th><th>Code</th><th>Description</th><th class="num">Qty</th><th>Unit</th><th class="num">Unit ฿</th><th class="num">Discount ฿</th><th class="num">Amount ฿</th></tr></thead><tbody>';
    h += q.items.map(i => `<tr><td class="num">${{i.line_no}}</td><td><strong>${{esc(i.product_code||'')}}</strong></td><td>${{esc(i.description||'')}}</td><td class="num">${{fmt(i.qty)}}</td><td>${{esc(i.unit||'')}}</td><td class="num">${{fmt(i.unit_price_thb,2)}}</td><td class="num">${{fmt(i.discount_thb,2)}}</td><td class="num">${{fmt(i.amount_thb,2)}}</td></tr>`).join('');
    h += '</tbody></table></div>';
  }}
  document.getElementById('sanko-qcontent').innerHTML = h;
}}
$sqp.addEventListener('change', renderSankoDoc);
renderSankoDoc();

// Sanko docs
const $sdocQ = document.getElementById('sanko-doc-q');
function fmtSize(b) {{ if (!b) return ''; if (b<1024) return b+' B'; if (b<1048576) return (b/1024).toFixed(0)+' KB'; return (b/1048576).toFixed(1)+' MB'; }}
function renderSankoDocs() {{
  const q = ($sdocQ.value||'').toLowerCase().trim();
  const rows = sankoDocs.filter(d => !q || (d.relative_path||'').toLowerCase().includes(q) || (d.category||'').toLowerCase().includes(q));
  rows.sort((a,b) => (b.file_date||'').localeCompare(a.file_date||''));
  document.querySelector('#sanko-docs-tbl tbody').innerHTML = rows.map(d => `
    <tr>
      <td>${{esc(d.file_date||'')}}</td>
      <td class="muted">${{esc(d.folder||'.')}}</td>
      <td>${{esc(d.filename||'')}}</td>
      <td><span class="chip">${{esc(d.category||'')}}</span></td>
      <td class="num">${{fmtSize(d.size_bytes||0)}}</td>
    </tr>`).join('');
}}
$sdocQ.addEventListener('input', renderSankoDocs);
renderSankoDocs();

// ---------- config ----------
const $configQ = document.getElementById('config-q');
function renderConfig() {{
  const q = ($configQ.value||'').toLowerCase().trim();
  const rows = DATA.config.filter(c => !q || JSON.stringify(c).toLowerCase().includes(q));
  document.querySelector('#config-tbl tbody').innerHTML = rows.map(c => `
    <tr>
      <td>${{esc(c.section||'')}}</td><td>${{esc(c.type_and_size||'')}}</td><td>${{esc(c.size_mm||'')}}</td>
      <td>${{esc(c.design_pattern||'')}}</td>
      <td>${{esc(c.nut_size||'')}}</td><td class="num">${{fmt(c.nut_qty)}}</td>
      <td>${{esc(c.spring_thickness||'')}}</td><td class="num">${{fmt(c.spring_qty)}}</td>
      <td>${{esc(c.washer_thickness||'')}}</td><td class="num">${{fmt(c.washer_qty)}}</td>
      <td>${{esc(c.bolt_head||'')}}</td><td>${{esc(c.thread_pitch||'')}}</td>
    </tr>`).join('');
}}
$configQ.addEventListener('input', renderConfig);
renderConfig();

// ---------- fittings ----------
document.querySelector('#fittings-tbl tbody').innerHTML = DATA.fittings.map(f => `
  <tr>
    <td>${{esc(f.section||'')}}</td><td><strong>${{esc(f.model||'')}}</strong></td><td>${{esc(f.project||'')}}</td>
    <td>${{esc(f.size_mm||'')}}</td><td>${{esc(f.length_mm||'')}}</td>
    <td class="num">${{fmt(f.qty_per_set)}}</td><td class="num">${{fmt(f.stock)}}</td>
    <td class="num">${{fmt(f.order_actual)}}</td>
    <td>${{esc(f.full_label||'')}}</td><td>${{esc(f.spacing||'')}}</td>
  </tr>`).join('');

// ---------- misc pricelist ----------
document.querySelector('#pricelist-tbl tbody').innerHTML = DATA.pricelist.map(p => `
  <tr>
    <td><strong>${{esc(p.model||'')}}</strong></td><td>${{esc(p.description||'')}}</td><td>${{esc(p.code||'')}}</td>
    <td class="num">${{fmt(p.unit_cost_thb,2)}}</td><td>${{esc(p.qty_per_set||'')}}</td>
    <td class="num">${{fmt(p.set_amount_thb,2)}}</td><td class="num">${{fmt(p.total_cost_thb,2)}}</td>
  </tr>`).join('');

// ---------- total orders ----------
const $totOrdQ = document.getElementById('totord-q');
function renderTotalOrders() {{
  const q = ($totOrdQ.value||'').toLowerCase().trim();
  const rows = DATA.total_orders.filter(c => !q || JSON.stringify(c).toLowerCase().includes(q));
  document.querySelector('#total-orders-tbl tbody').innerHTML = rows.map(t => `
    <tr>
      <td>${{esc(t.section||'')}}</td><td>${{esc(t.full_label||'')}}</td><td>${{esc(t.description||'')}}</td>
      <td>${{esc(t.size||'')}}</td><td>${{esc(t.length||'')}}</td>
      <td class="num">${{fmt(t.required)}}</td><td class="num">${{fmt(t.extra_order)}}</td>
      <td class="num">${{fmt(t.stock)}}</td><td class="num">${{fmt(t.actual_order)}}</td>
    </tr>`).join('');
}}
$totOrdQ.addEventListener('input', renderTotalOrders);
renderTotalOrders();

// ---------- purchase orders ----------
const $poPick = document.getElementById('po-pick');
$poPick.innerHTML = DATA.purchase_orders.map((p,i) => `<option value="${{i}}">${{esc(p.po_id)}} — ${{p.item_count}} items</option>`).join('');
function renderPO() {{
  const p = DATA.purchase_orders[Number($poPick.value||0)];
  if (!p) return;
  let html = `<div class="panel"><h2>${{esc(p.po_id)}}</h2><div class="muted">${{esc(p.title||'')}} · ${{esc(p.subtitle||'')}}</div>`;
  html += '<div class="table-wrap" style="margin-top:1rem"><table><thead><tr><th>Type</th><th>Model</th><th>Description</th><th>Row values (first 10)</th></tr></thead><tbody>';
  html += p.items.map(i => `<tr><td>${{esc(i.col_0||'')}}</td><td><strong>${{esc(i.col_1||'')}}</strong></td><td>${{esc(i.col_2||'')}}</td><td class="muted">${{(i.values||[]).slice(3,13).map(esc).join(' · ')}}</td></tr>`).join('');
  html += '</tbody></table></div></div>';
  document.getElementById('po-content').innerHTML = html;
}}
$poPick.addEventListener('change', renderPO);
renderPO();

// ---------- reference ----------
document.querySelector('#types-tbl tbody').innerHTML = DATA.types.map(t =>
  `<tr><td><strong>${{esc(t.type_code||'')}}</strong></td><td>${{esc(t.material||'')}}</td><td>${{esc(t.description_th||'')}}</td></tr>`).join('');
document.querySelector('#threads-tbl tbody').innerHTML = DATA.threads.map(t =>
  `<tr><td>${{esc(t.thread||'')}}</td><td class="num">${{fmt(t.pitch_mm,2)}}</td></tr>`).join('');
document.querySelector('#orders-meta-tbl tbody').innerHTML = DATA.orders_meta.map(o =>
  `<tr><td>${{esc(o.order_label||'')}}</td><td class="num">${{esc(String(o.column_index))}}</td><td>${{esc(o.project||'')}}</td><td>${{esc(o.date_raw||'')}}</td></tr>`).join('');

</script>
</body>
</html>
"""


def main() -> int:
    _credentials_path()
    db = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)
    logger.info("reading collections from %s...", FIRESTORE_DATABASE)

    fasteners = fetch_fasteners_with_orders(db)
    types = fetch_collection(db, "fastener_types")
    threads = fetch_collection(db, "fastener_threads")
    config = fetch_collection(db, "fastener_config")
    fittings = fetch_collection(db, "fastener_fittings")
    pricelist = fetch_collection(db, "fastener_pricelist")
    vendor_pricelists = fetch_collection(db, "fastener_vendor_pricelists")
    orders_meta = fetch_collection(db, "fastener_orders")
    total_orders = fetch_collection(db, "fastener_total_orders")
    purchase_orders = fetch_collection(db, "fastener_purchase_orders")
    # Sanko vendor archive
    sanko_products = fetch_collection(db, "sanko_products")
    sanko_prices = fetch_collection(db, "sanko_prices")
    sanko_quotations = fetch_collection(db, "sanko_quotations")
    sanko_pos = fetch_collection(db, "sanko_purchase_orders")
    sanko_documents = fetch_collection(db, "sanko_documents")

    # Sort reference lists
    types.sort(key=lambda t: t.get("type_code", ""))
    threads.sort(key=lambda t: (len(t.get("thread", "")), t.get("thread", "")))
    orders_meta.sort(key=lambda o: o.get("column_index", 0))
    sanko_products.sort(key=lambda p: (p.get("category", ""), p.get("product_code", "")))

    logger.info(
        "counts: fasteners=%d types=%d threads=%d config=%d fittings=%d pricelist=%d "
        "vendors=%d orders_meta=%d total_orders=%d pos=%d | sanko_products=%d "
        "sanko_prices=%d sanko_quotations=%d sanko_pos=%d sanko_docs=%d",
        len(fasteners), len(types), len(threads), len(config), len(fittings),
        len(pricelist), len(vendor_pricelists), len(orders_meta),
        len(total_orders), len(purchase_orders),
        len(sanko_products), len(sanko_prices), len(sanko_quotations),
        len(sanko_pos), len(sanko_documents),
    )

    html_content = render(
        fasteners, types, threads, config, fittings, pricelist,
        vendor_pricelists, orders_meta, total_orders, purchase_orders,
        sanko_products, sanko_prices, sanko_quotations, sanko_pos, sanko_documents,
    )

    out_path = Path("catalog.html")
    out_path.write_text(html_content, encoding="utf-8")
    logger.info("wrote %s (%.1f KB)", out_path, out_path.stat().st_size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
