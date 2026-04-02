# -*- coding: utf-8 -*-
"""
freight_calculator.py
GO Corporation — Import Landed Cost Calculator
Adapted from real GO Corporation shipment data (2026)

Usage:
    Edit the INPUT block below, then run:
        python freight_calculator.py

All rates sourced from actual GO Corp shipments:
  - Air EXW Italy→BKK: Logimark International (PO.1254, March 2026)
  - Sea EXW China→BKK: Profreight / Gift Somlak (Slack #shipping-china-thai)
  - Insurance: Chubb via Logimark (confirmed PO.1254)
"""

# ──────────────────────────────────────────────
#  INPUT — edit this block for each quotation
# ──────────────────────────────────────────────

# Product info
PRODUCT_NAME        = "ED 70 Aluminum Folding Door"
HS_CODE             = "7610"           # See guide for common codes
HS_DUTY_RATE        = 0.10             # 10% for HS 7610 (aluminum structures)
                                       # 20% for HS 9403 (furniture) — confirmed

# EXW price (already in THB, or convert below)
EXW_PRICE_FOREIGN   = 0               # e.g. USD amount — set 0 if using THB directly
EXW_CURRENCY        = "USD"           # "USD", "EUR", "CNY"
FX_RATE_TO_THB      = 33.50           # THB per 1 unit of foreign currency
                                       # EUR: ~37.40 (confirmed March 2026)
                                       # USD: ~33–34 (check current rate)
                                       # CNY: ~4.60 (approximate)

EXW_PRICE_THB       = 150000          # Set directly in THB if known
                                       # Will override FX calculation if > 0

# Package dimensions and weight
PKG_LENGTH_CM       = 285             # crate length
PKG_WIDTH_CM        = 30              # crate width
PKG_HEIGHT_CM       = 280             # crate height
PKG_ACTUAL_KG       = 120             # gross weight of package

# Freight mode
MODE                = "sea"           # "air" or "sea"

# Air freight rate (THB per chargeable kg, door-to-door EXW)
# Source: Logimark PO.1254 Italy→BKK March 2026 = ~182 THB/kg (no surcharge)
# Revised with fuel+SCC surcharges = ~214 THB/kg
# China→BKK air estimate: 100–140 THB/kg (shorter route)
AIR_RATE_THB_PER_KG = 130             # REPLACE with actual forwarder quote

# Sea freight rate (THB per CBM, LCL consolidation)
# Source: Gift Somlak / Profreight #shipping-china-thai
# Typical China→BKK LCL: USD 35–60/CBM = THB ~1,175–2,010/CBM
# Closer estimate with all port fees: THB 2,500–4,500/CBM
SEA_RATE_THB_PER_CBM = 3500           # REPLACE with Gift's actual quote

# Customs clearance service fee (charged by forwarder, separate from duty)
# Logimark PO.1254 all-in clearance: THB 14,076 (included duty+tax+service)
# Service fee only (without duty): estimate THB 2,000–4,000
CLEARANCE_FEE_THB   = 3000            # REPLACE with forwarder's stated fee

# Last-mile delivery Bangkok
# Confirmed from Slack: oversized 6-wheel truck = THB 3,500
# Standard 4-wheel truck: THB 1,500–2,500
LAST_MILE_THB       = 3500

# Insurance
INSURE              = True
INSURANCE_RATE      = 0.0045          # 0.45% — confirmed from Chubb/Logimark
INSURANCE_MIN_USD   = 40              # minimum premium
USD_RATE            = 33.50           # THB per USD for insurance minimum

# ──────────────────────────────────────────────
#  CALCULATION ENGINE (do not edit below)
# ──────────────────────────────────────────────

def calc_volumetric_air(l, w, h):
    """Air volumetric weight: L×W×H (cm) / 6000"""
    return (l * w * h) / 6000

def calc_cbm(l, w, h):
    """Volume in CBM: L×W×H (cm) / 1,000,000"""
    return (l * w * h) / 1_000_000

def calc_chargeable_weight_air(actual_kg, vol_kg):
    """Chargeable weight for air = max(actual, volumetric)"""
    return max(actual_kg, vol_kg)

def calc_chargeable_sea(actual_kg, cbm):
    """Revenue ton for sea LCL = max(CBM, actual_kg/1000)"""
    return max(cbm, actual_kg / 1000)

def calc_insurance(cif_thb, rate=0.0045, min_usd=40, usd_rate=33.50):
    """Chubb cargo insurance via Logimark (confirmed PO.1254)"""
    insured_value = cif_thb * 1.10       # 110% of CIF
    premium = insured_value * rate
    min_premium_thb = min_usd * usd_rate
    premium = max(premium, min_premium_thb)
    stamp = 4
    duplicate = 1
    vat = premium * 0.07
    total = premium + stamp + duplicate + vat
    return {
        "insured_value_thb": round(insured_value, 2),
        "premium_thb": round(premium, 2),
        "stamp_thb": stamp,
        "duplicate_thb": duplicate,
        "vat_thb": round(vat, 2),
        "total_thb": round(total, 2),
    }

def calc_import_duty(cif_thb, duty_rate):
    """Thailand import duty + VAT"""
    duty = cif_thb * duty_rate
    vat_base = cif_thb + duty
    vat = vat_base * 0.07
    total_tax = duty + vat
    return {
        "cif_thb": round(cif_thb, 2),
        "duty_thb": round(duty, 2),
        "vat_base_thb": round(vat_base, 2),
        "vat_thb": round(vat, 2),
        "total_tax_thb": round(total_tax, 2),
    }

def run():
    import sys
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 55)
    print("  GO CORPORATION - LANDED COST CALCULATOR")
    print("=" * 55)
    print(f"  Product : {PRODUCT_NAME}")
    print(f"  HS Code : {HS_CODE}  |  Duty rate: {HS_DUTY_RATE*100:.0f}%")
    print(f"  Mode    : {MODE.upper()}")
    print()

    # ── EXW Price ──
    if EXW_PRICE_THB > 0:
        exw_thb = EXW_PRICE_THB
    else:
        exw_thb = EXW_PRICE_FOREIGN * FX_RATE_TO_THB
    print(f"  EXW price            : THB {exw_thb:>12,.2f}")

    # ── Dimensions ──
    vol_air_kg = calc_volumetric_air(PKG_LENGTH_CM, PKG_WIDTH_CM, PKG_HEIGHT_CM)
    cbm        = calc_cbm(PKG_LENGTH_CM, PKG_WIDTH_CM, PKG_HEIGHT_CM)
    cw_air     = calc_chargeable_weight_air(PKG_ACTUAL_KG, vol_air_kg)
    rev_ton    = calc_chargeable_sea(PKG_ACTUAL_KG, cbm)

    print(f"  Dimensions           : {PKG_LENGTH_CM} × {PKG_WIDTH_CM} × {PKG_HEIGHT_CM} cm")
    print(f"  Actual weight        : {PKG_ACTUAL_KG} kg")
    print(f"  Volumetric (air)     : {vol_air_kg:.1f} kg  (÷6,000)")
    print(f"  Volume (CBM)         : {cbm:.3f} CBM")
    print()

    # ── Freight ──
    if MODE == "air":
        cw = cw_air
        freight_thb = cw * AIR_RATE_THB_PER_KG
        print(f"  Chargeable wt (air)  : {cw:.1f} kg")
        print(f"  Air rate             : THB {AIR_RATE_THB_PER_KG}/kg")
        print(f"  Freight cost         : THB {freight_thb:>12,.2f}")
    else:  # sea
        cw = rev_ton
        freight_thb = cbm * SEA_RATE_THB_PER_CBM
        print(f"  Revenue ton (sea)    : {cw:.3f} RT  (max CBM vs actual/1000)")
        print(f"  Sea LCL rate         : THB {SEA_RATE_THB_PER_CBM:,.0f}/CBM")
        print(f"  Freight cost (sea)   : THB {freight_thb:>12,.2f}")

    # ── CIF value (for duty and insurance base) ──
    # Pre-insurance CIF estimate (insurance calculated on this)
    cif_pre = exw_thb + freight_thb + CLEARANCE_FEE_THB

    # ── Insurance ──
    if INSURE:
        ins = calc_insurance(cif_pre, INSURANCE_RATE, INSURANCE_MIN_USD, USD_RATE)
        insurance_total = ins["total_thb"]
        print(f"  Insurance (0.45%×110%): THB {insurance_total:>11,.2f}")
        print(f"    Insured value        : THB {ins['insured_value_thb']:>11,.2f}")
        print(f"    Premium              : THB {ins['premium_thb']:>11,.2f}")
        print(f"    VAT                  : THB {ins['vat_thb']:>11,.2f}")
    else:
        insurance_total = 0
        print(f"  Insurance            :         NONE")

    # ── True CIF ──
    cif_true = exw_thb + freight_thb + insurance_total

    # ── Import Duty + VAT ──
    tax = calc_import_duty(cif_true, HS_DUTY_RATE)
    print()
    print(f"  CIF value (true)     : THB {cif_true:>12,.2f}")
    print(f"  Import duty ({HS_DUTY_RATE*100:.0f}%)    : THB {tax['duty_thb']:>12,.2f}")
    print(f"  VAT base             : THB {tax['vat_base_thb']:>12,.2f}")
    print(f"  VAT (7%)             : THB {tax['vat_thb']:>12,.2f}")
    print(f"  Total import tax     : THB {tax['total_tax_thb']:>12,.2f}")
    print(f"  Clearance service fee: THB {CLEARANCE_FEE_THB:>12,.2f}")
    print(f"  Last-mile delivery   : THB {LAST_MILE_THB:>12,.2f}")

    # ── Total Landed Cost ──
    landed = (
        exw_thb
        + freight_thb
        + insurance_total
        + tax["total_tax_thb"]
        + CLEARANCE_FEE_THB
        + LAST_MILE_THB
    )
    print()
    print("─" * 55)
    print(f"  TOTAL LANDED COST    : THB {landed:>12,.2f}")
    print()

    # ── Areda Selling Price (min 20% margin) ──
    min_sell_price = landed / 0.80   # 20% gross margin
    margin_25 = landed / 0.75        # 25%
    margin_30 = landed / 0.70        # 30%
    print(f"  Min selling (20% GM) : THB {min_sell_price:>12,.2f}")
    print(f"  At 25% GM            : THB {margin_25:>12,.2f}")
    print(f"  At 30% GM            : THB {margin_30:>12,.2f}")
    print()

    # ── Cost Breakdown ──
    print("  COST BREAKDOWN:")
    items = [
        ("EXW product price", exw_thb),
        ("Freight", freight_thb),
        ("Insurance", insurance_total),
        ("Import duty", tax["duty_thb"]),
        ("VAT", tax["vat_thb"]),
        ("Clearance service fee", CLEARANCE_FEE_THB),
        ("Last-mile delivery", LAST_MILE_THB),
    ]
    for name, amt in items:
        pct = (amt / landed * 100) if landed > 0 else 0
        print(f"    {name:<24} THB {amt:>10,.0f}  ({pct:.1f}%)")
    print("─" * 55)
    print()

    # ── Rate reference note ──
    print("  NOTE: Rates are estimates based on GO Corp actual data.")
    print("  ALWAYS get a fresh quote from your freight agent before")
    print("  using in a customer quotation.")
    print()
    print("  Air rate source  : Logimark PO.1254, MXP→BKK, March 2026")
    print("  Sea rate source  : Profreight/Gift Somlak, China→BKK")
    print("  Insurance source : Chubb via Logimark, PO.1254")
    print("  HS 9403 duty 20% : Confirmed Logimark (furniture)")
    print("=" * 55)

    return landed

if __name__ == "__main__":
    run()
