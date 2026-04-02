# Freight & Landed Cost Calculation Guide
**GO Corporation Co., Ltd. — Import Operations**
Last updated: 2026-04-02 | Source: Live order data via Shipping Agent

---

## Overview: GO Corporation's Freight Routes

| Route | Mode | Agent | Transit | Use for |
|---|---|---|---|---|
| China (Foshan/GZ) → Bangkok | Sea LCL | Gift Somlak (Murazame / Profreight) | 7–10 days | Large/heavy items, low urgency |
| China (Foshan/GZ) → Bangkok | Air LCL | DHL Global Forwarding | 3–5 days | Urgent, small items |
| Italy / Europe → Bangkok | Air LCL | Logimark International | 5–7 days | Confirmed on PO.1254 |
| Italy / Europe → Bangkok | Sea LCL | Logimark International | 25–35 days | Large furniture |

---

## 1. Chargeable Weight

Every freight quote is based on **chargeable weight (C.W.)** — the higher of actual vs volumetric.

### Air Freight
```
Volumetric weight (kg) = L(cm) × W(cm) × H(cm) ÷ 6,000
Chargeable weight      = max(actual_kg, volumetric_kg)
```

### Sea Freight (LCL)
```
Volume (CBM) = L(cm) × W(cm) × H(cm) ÷ 1,000,000
Billed by    = max(CBM, actual_kg ÷ 1,000)   ← revenue ton rule
```

---

## 2. Air Freight Rates (EXW, Door-to-Door to Bangkok)

### Logimark International — Confirmed from PO.1254 (Italy→Bangkok)
> Product: Glas Italia FOU03 glass table | C.W. 100 kg | March 2026

| Component | Rate | THB (@ 1 EUR = 37.40) |
|---|---|---|
| Air freight + clearance + last-mile (base) | All-in per C.W. kg | THB 18,237 / 100 kg = **182 THB/kg** |
| Fuel surcharge (WY airline, ad hoc) | EUR 0.70/kg | THB 26.18/kg |
| SCC surcharge (ad hoc) | EUR 0.10/kg | THB 3.74/kg |
| **Revised all-in with surcharges** | | **~214 THB/kg** |

**Note:** EUR/kg rates fluctuate. Always get a fresh quote. Surcharges can be added by the airline without notice.

### DHL Global Forwarding — Alternative (China/Italy→Bangkok)
- Quote obtained for Chandelier Khun Nan order (March 2026)
- Rate is per PDF quotation — request fresh quote per shipment
- Contact: Pakulkarn Haruthaiwichitchoke (Yokky) — pakulkarn.haruthaiwichitchoke@dhl.com

### Estimated Air Rate (China→Bangkok, guidance only)
China is ~40% shorter than Italy route. Expect **THB 100–140/kg all-in** for China air LCL.
Get fresh quote from Logimark or DHL before using.

---

## 3. Sea Freight Rates (EXW, China→Bangkok)

### Agent: Gift Somlak (Murazame) — Profreight Group
**China consolidation warehouse:**
> 广州市白云区太和镇和龙村云美西街2号
> Contact: 姜琼玉 — +86 137 9881 9454

**Tracking prefixes:**
- `TA-xxxx` = Sea freight
- `TB-xxxx` = Land freight (road, faster)

### Timeline (confirmed from Slack #shipping-china-thai)
| Stage | Duration |
|---|---|
| Factory → Guangzhou warehouse | 1–4 days (land transport) |
| Consolidation + loading | 2–3 days |
| Sea transit Guangzhou → Bangkok | **7–10 days** |
| Customs clearance Bangkok | 1–2 days |
| Last-mile Bangkok delivery | 1–3 days |
| **Total EXW → project site** | **~14–21 days** |

### Sea LCL Rate Reference
> Actual per-CBM rates are in Gift's freight invoices (PDF).
> Request current rate from Gift before quoting.

**Confirmed last-mile cost (from Slack):**
- Standard 4-wheel truck: ~THB 1,500–2,500
- **Oversized item, 6-wheel truck: THB 3,500** ✓ (confirmed)

---

## 4. Import Duty & Tax (Thailand)

### Formula
```
CIF value      = EXW price + freight cost + insurance premium
Import duty    = CIF × duty_rate
VAT base       = CIF + import duty
VAT            = VAT base × 7%
Total tax      = import duty + VAT
```

### HS Codes — Confirmed from GO Corporation Orders

| HS Code | Description | Import Duty | Notes |
|---|---|---|---|
| **9403** | Furniture (chairs, tables, shelving) | **20%** | Confirmed from PO.1254 |
| **7610** | Aluminum windows, doors, frames, structures | **~10–15%** | Verify with customs broker |
| **7308** | Iron/steel structures, doors, windows | **~10%** | Verify per product |
| **9405** | Luminaires / lighting fixtures | **~20%** | Common GO product |
| **3926 / 3925** | Plastic fittings, hardware | **~5–10%** | |

> **Always confirm HS code with freight forwarder before quoting.**
> HS 9403 furniture: 20% duty, NO import permit required (confirmed Logimark).

---

## 5. Cargo Insurance

### Rate (from Logimark / Chubb — confirmed PO.1254)
```
Insured value  = CIF × 110%
Premium        = insured_value × 0.45%   (minimum USD 40)
Stamp duty     = THB 4
Duplicate      = THB 1
VAT            = premium × 7%
Total insurance cost = premium + stamp + duplicate + VAT
```

### Example (PO.1254 actual)
| Item | Amount |
|---|---|
| EXW value | EUR 887.30 |
| CIF value (×110%) | EUR 976.03 = THB 36,493 |
| Premium (0.45%) | THB 1,000 |
| Stamp | THB 4 |
| Duplicate | THB 1 |
| VAT (7%) | THB 70.35 |
| **Total insurance** | **THB 1,075.35** |

### DHL Insurance Rate (for reference)
> 0.45% on CIF × 110%, minimum USD 40, + VAT 7%

---

## 6. Customs Clearance Service Fee

Charged by the freight forwarder — separate from duty/tax.

| Item | Typical Cost |
|---|---|
| Customs clearance service fee | THB 2,000–4,000 (per forwarder) |
| Actual import duty + VAT | Per HS code formula above |
| Port/terminal handling | Included in forwarder fee or charged separately |

**PO.1254 actual:** THB 14,076 total clearance payment (included duty + tax + clearance service for 100 kg air LCL).

---

## 7. Freight Request Template

When requesting quotes from Logimark/DHL/Gift, always provide:

```
Subject: [Agent-GO] Request for [Air/Sea] Freight Clearance and Delivery Cost
         under EXW Terms | [PO number] [Product] [Customer]

Body:
- Trade term: EXW [City, Country]
- Product: [Name in English and Thai]
- HS Code (if known): [XXXX]
- Dimensions (L × W × H cm): [dims]
- Actual weight (kg): [kg]
- No. of packages/cartons: [n]
- Declared value: [USD/EUR amount]
- Delivery address: [Full Bangkok address]
- Required by: [date]
- Include: Premium cargo insurance
- Include: HS code confirmation + import permit check
```

---

## 8. Full Landed Cost Formula

```
Landed cost = EXW price
            + freight (air or sea, door-to-door)
            + insurance
            + import duty (CIF × duty%)
            + VAT ((CIF + duty) × 7%)
            + [last-mile if not in freight quote]
```

**Areda margin rule:** Customer price ≥ landed cost × 1.20 (min 20% gross margin)

---

## 9. ED 70 Folding Door — China (Foshan) → Bangkok Estimate

**Product:** ED 70 Aluminum Folding Door, 2750 × 2750 mm, export wood crate
**Trade term:** EXW Foshan
**Recommended freight route:** Sea LCL via Gift Somlak (Profreight)

| Component | Estimate | Notes |
|---|---|---|
| Sea freight (LCL, Foshan→Bangkok) | THB 2,500–4,500/CBM | Request from Gift — ~2–3 CBM estimated |
| Customs clearance + port fees | THB 3,000–5,000 | Per Logimark/Profreight standard |
| Import duty (HS 7610, ~10%) | 10% × CIF value | CIF = EXW + freight + insurance |
| VAT | 7% × (CIF + duty) | — |
| Local 6-wheel truck (oversized) | THB 3,500 | Confirmed from Slack |
| Insurance (Chubb/DHL) | 0.45% × CIF × 110% | Min USD 40 |
| **Total estimate** | **THB 15,000–25,000** | Get firm quote from Gift first |

**Action:** Message Gift in Slack #shipping-china-thai with:
- Crate dimensions (request from supplier)
- Gross weight (request from supplier)
- EXW value in CNY/USD for duty calculation

---

## 10. Contacts

| Role | Name | Contact |
|---|---|---|
| China→Thailand Sea/Land freight | Gift Somlak (Murazame) | Slack: #shipping-china-thai |
| China warehouse contact | 姜琼玉 | +86 137 9881 9454 |
| Air freight (Italy/Europe) | Jakapon Patcharavisit (Pon) | p.jakapon@bkk.logimark-group.com |
| Air supervisor (Italy/Europe) | Prapaporn Thammachareerak | t.prapaporn@bkk.logimark-group.com |
| DHL Global Forwarding | Pakulkarn (Yokky) | pakulkarn.haruthaiwichitchoke@dhl.com |
| DHL (CC) | Thanawan Chaichana | thanawan.chaichana@dhl.com |
| Internal shipping | Niwat Samrit | niwat@goco.bz |
| Internal CC | Shipping GO | shipping@goco.bz |

---

## 11. Use `freight_calculator.py`

Run the Python script in the same folder to get a full landed cost calculation for any shipment:

```bash
python freight_calculator.py
```

Edit the `INPUT` block at the top of the script to set:
- EXW price (THB equivalent)
- Dimensions and weight
- Mode (air/sea)
- HS duty rate
- FX rate (if applicable)
