"""Constants: ATC columns, view names, and ATC code to full name mapping."""

# Columns in raw CSV that contain drug category (ATC) sales (wide format)
RAW_ATC_COLUMNS = [
    "M01AB",
    "M01AE",
    "N02BA",
    "N02BE",
    "N05B",
    "N05C",
    "R03",
    "R06",
]

# Required columns in raw CSV (besides ATC columns)
RAW_REQUIRED_COLUMNS = ["datum", "Year", "Month", "Hour", "Weekday Name"]

# Name of the temp view for SQL analytics
PHARMA_SALES_VIEW = "pharma_sales"

# Output column names after transform (long format)
COL_DATUM = "datum"
COL_DRUG_CATEGORY = "drug_category"
COL_SALES = "sales"
COL_YEAR = "year"
COL_QUARTER = "quarter"
COL_MONTH = "month"
COL_DAY_OF_WEEK = "day_of_week"
COL_IS_WEEKEND = "is_weekend"
COL_CATEGORY_NAME = "category_name"
COL_SEASON = "season"

# ATC code -> full category name (for display/reports)
ATC_CODE_TO_NAME = {
    "M01AB": "Anti-inflammatory/antirheumatic agents, acetic acid derivatives",
    "M01AE": "Anti-inflammatory/antirheumatic agents, COX-2 inhibitors",
    "N02BA": "Other analgesics and antipyretics, salicylic acid and derivatives",
    "N02BE": "Other analgesics and antipyretics, pyrazolones",
    "N05B": "Anxiolytics",
    "N05C": "Hypnotics and sedatives",
    "R03": "Drugs for obstructive airway diseases",
    "R06": "Antihistamines for systemic use",
}
