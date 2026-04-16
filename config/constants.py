"""Constants for sheet IDs, URLs, and size lookup tables."""

# Google Sheets
SPREADSHEET_ID = "18wczPjTPic2GPh0cG_1hwalGXQK3tMzNvU-dmA3aSxk"
SOURCE_SHEET_NAME = "CHS JIS M"
OD_TABLE_SHEET_NAME = "CHS Table"
OUTPUT_SHEET_NAME = "CHS JIS Claude"

# OneStockHome
ONESTOCKHOME_BASE_URL = "https://www.onestockhome.com"
ONESTOCKHOME_TABLE_URL = (
    "https://www.onestockhome.com/th/product_categories/round-pipe/items_table"
)
SCRAPE_DELAY_SECONDS = 1.5

# Firestore collections
SECTIONS_COLLECTION = "sections"
VENDORS_COLLECTION = "vendors"
VENDOR_PRICES_COLLECTION = "vendor_prices"
SCRAPE_RUNS_COLLECTION = "scrape_runs"
PRICE_HISTORY_SUBCOLLECTION = "price_history"

# Inch to DN (Diameter Nominal) mapping
INCH_TO_DN: dict[str, int] = {
    "1/2": 15,
    "3/4": 20,
    "1": 25,
    "1 1/4": 32,
    "1 1/2": 40,
    "2": 50,
    "2 1/2": 65,
    "3": 80,
    "3 1/2": 90,
    "4": 100,
    "5": 125,
    "6": 150,
    "7": 175,
    "8": 200,
    "10": 250,
    "12": 300,
    "14": 350,
    "16": 400,
    "18": 450,
}

# DN to Outside Diameter (mm) — from TIS/JIS standards
DN_TO_OD: dict[int, float] = {
    15: 21.7,
    20: 27.2,
    25: 34.0,
    32: 42.7,
    40: 48.6,
    50: 60.5,
    65: 76.3,
    80: 89.1,
    90: 101.6,
    100: 114.3,
    125: 139.8,
    150: 165.2,
    175: 190.7,
    200: 216.3,
    250: 267.4,
    300: 318.5,
    350: 355.6,
    400: 406.4,
    450: 457.2,
}

# Inch to OD (convenience: inch string → OD mm)
INCH_TO_OD: dict[str, float] = {
    inch: DN_TO_OD[dn] for inch, dn in INCH_TO_DN.items() if dn in DN_TO_OD
}
