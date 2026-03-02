"""
DSR|RIECT — Query Normalizer
Handles short words, misspellings, abbreviations, and domain-specific aliases
so any user input — however informal — maps to the correct retail analytical intent.

Pipeline: raw_query → expand abbreviations → correct spelling → apply aliases → normalized
"""

import re
import logging

logger = logging.getLogger(__name__)

# ─── Retail Abbreviation Expansion ────────────────────────────────────────────
# Applied as whole-word token substitutions (word boundaries respected)

ABBREVIATIONS: dict[str, str] = {
    # Time periods
    "ytd": "year to date",
    "mtd": "month to date",
    "wtd": "week to date",
    "lw":  "last week",
    "lm":  "last month",
    "ly":  "last year",
    "ltl":  "like for like financial year",
    "fytd": "financial year to date",
    "ftd":  "financial year to date",
    "qtd":  "quarter to date",
    "aod":  "as on date",
    "asod": "as on date",
    "lfl": "like for like",
    "yoy": "year on year",
    "mom": "month on month",
    "wow": "week on week",
    "dod": "day on day",
    "yday": "yesterday",
    "tmrw": "tomorrow",
    # KPIs
    "spsf": "sales per square foot",
    "doi":  "days of inventory",
    "mbq":  "minimum baseline quantity",
    "soh":  "stock on hand",
    "git":  "goods in transit",
    "upt":  "units per transaction",
    "atv":  "average transaction value",
    "asp":  "average selling price",
    "grn":  "goods receipt note",
    "po":   "purchase order",
    # Products/Categories
    "sku":  "stock keeping unit",
    "div":  "division",
    "dept": "department",
    "sec":  "section",
    # Store/People
    "str":  "store",
    "strs": "stores",
    "mgr":  "manager",
    "zm":   "zone manager",
    "rm":   "regional manager",
    "hq":   "headquarters",
    # Finance/Operations
    "inv":  "inventory",
    "stk":  "stock",
    "sls":  "sales",
    "rev":  "revenue",
    "disc": "discount",
    "promo":"promotion",
    "ret":  "return",
    "rets": "returns",
    "pnl":  "profit and loss",
    # Analytics
    "avg":  "average",
    "pct":  "percentage",
    "vol":  "volume",
    "qty":  "quantity",
    "amt":  "amount",
    "cnt":  "count",
    "num":  "number",
    # Actions
    "shw":  "show",
    "lst":  "list",
    "chk":  "check",
    "anlys":"analysis",
    "anl":  "analyse",
    "cal":  "calculate",
    "calc": "calculate",
    "cmp":  "compare",
    # Loss/shrinkage domain
    "pilf":    "pilferage",
    "shrink":  "shrinkage",
    "mkdn":    "markdown",
    "mkdwn":   "markdown",
    "nmv":     "non moving inventory",
}

# ─── Spelling Corrections ─────────────────────────────────────────────────────
# Applied to individual tokens

SPELL_MAP: dict[str, str] = {
    # Analytics
    "analitcis":     "analytics",
    "analytcis":     "analytics",
    "analyitcs":     "analytics",
    "analyics":      "analytics",
    "analtics":      "analytics",
    "anlaytics":     "analytics",
    # Inventory
    "inventori":     "inventory",
    "inventary":     "inventory",
    "inventry":      "inventory",
    "invntory":      "inventory",
    "invetory":      "inventory",
    # Performance
    "performanc":    "performance",
    "performence":   "performance",
    "performnce":    "performance",
    "preformance":   "performance",
    # Discount
    "discound":      "discount",
    "discont":       "discount",
    "dicsount":      "discount",
    "diccount":      "discount",
    # Pilferage / shrinkage
    "pilferge":      "pilferage",
    "pilfrage":      "pilferage",
    "pilferege":     "pilferage",
    "pilferagr":     "pilferage",
    "pilferge":      "pilferage",
    "pilfrage":      "pilferage",
    "shrinkge":      "shrinkage",
    "shrinkge":      "shrinkage",
    "shrikage":      "shrinkage",
    # Returns
    "retun":         "return",
    "retuns":        "returns",
    "returnd":       "returns",
    "refnd":         "refund",
    # Revenue / Sales
    "revenu":        "revenue",
    "reveue":        "revenue",
    "slaes":         "sales",
    "saels":         "sales",
    "sal3s":         "sales",
    # Customer
    "custmer":       "customer",
    "cusomer":       "customer",
    "custommer":     "customer",
    "cutsomer":      "customer",
    # Category
    "categori":      "category",
    "catgory":       "category",
    "catagory":      "category",
    # Department
    "departmen":     "department",
    "departmnt":     "department",
    "dpartment":     "department",
    # Compliance
    "complianc":     "compliance",
    "compliace":     "compliance",
    "complinace":    "compliance",
    # Transaction
    "transacion":    "transaction",
    "transacton":    "transaction",
    "trasaction":    "transaction",
    # Store
    "stoer":         "store",
    "stoers":        "stores",
    "stre":          "store",
    # Stock
    "stocck":        "stock",
    "stcok":         "stock",
    "stoock":        "stock",
    # Threshold
    "threshhold":    "threshold",
    "treshold":      "threshold",
    "threshld":      "threshold",
    # Exception
    "excpetion":     "exception",
    "excepiton":     "exception",
    "expetion":      "exception",
    # Recommendation
    "recomendation": "recommendation",
    "recommandation":"recommendation",
    "recomend":      "recommend",
    "reccomend":     "recommend",
    # KPIs by name
    "sell-thru":     "sell-through",
    "sellthru":      "sell-through",
    "sellthrough":   "sell-through",
    "spsff":         "spsf",
}

# ─── Domain Alias Mapping ─────────────────────────────────────────────────────
# Phrase-level: map semantically equivalent phrases to canonical retail terms
# Applied AFTER abbreviation expansion and spell correction

PHRASE_ALIASES: dict[str, str] = {
    # Pilferage / shrinkage signals
    "theft":                    "pilferage shrinkage",
    "stolen":                   "pilferage shrinkage",
    "shoplifting":              "pilferage shrinkage",
    "pilferage":                "pilferage shrinkage",
    "leakage":                  "pilferage loss shrinkage",
    "internal fraud":           "pilferage discount fraud",
    "cashier fraud":            "pilferage discount fraud",
    "unauthorized discount":    "discount pilferage fraud",
    "bill integrity":           "discount pilferage fraud",
    "unaccounted loss":         "pilferage loss shrinkage",
    "stock leakage":            "pilferage shrinkage",
    # Loss
    "wastage":                  "loss shrinkage",
    "damage":                   "loss shrinkage",
    "spoilage":                 "loss shrinkage",
    "dead loss":                "loss",
    # Discounts
    "markdown":                 "discount markdown",
    "clearance":                "discount clearance",
    "clearence":                "discount clearance",
    "non promo discount":       "unauthorized discount",
    "non-promo discount":       "unauthorized discount",
    "extra discount":           "unauthorized discount",
    "manual discount":          "unauthorized discount",
    # Sales returns
    "sales return":             "sales returns",
    "sale return":              "sales returns",
    "credit note":              "sales returns",
    "refund":                   "sales returns",
    "refunds":                  "sales returns",
    "exchange return":          "sales returns",
    "product return":           "sales returns",
    # Inventory
    "overstock":                "excess inventory overstock",
    "over stock":               "excess inventory overstock",
    "deadstock":                "dead stock non moving",
    "dead stock":               "dead stock non moving",
    "slow mover":               "slow moving inventory",
    "slow movers":              "slow moving inventory",
    "slow moving":              "slow moving inventory",
    "slow mvr":                 "slow moving inventory",
    "fast mover":               "fast moving inventory",
    "fast movers":              "fast moving inventory",
    "non moving":               "non moving inventory",
    # Peak hours / traffic
    "high peak hours":          "peak hours analysis",
    "peak hour":                "peak hours analysis",
    "rush hours":               "peak hours analysis",
    "rush hour":                "peak hours analysis",
    "busy hours":               "peak hours analysis",
    "busiest time":             "peak hours analysis",
    "busiest hour":             "peak hours analysis",
    "hourly traffic":           "peak hours foot traffic",
    "hourly sales":             "peak hours sales",
    "store timing":             "peak hours store timing",
    "by hour":                  "peak hours hourly",
    # Performance terms
    "top store":                "top performing stores",
    "best store":               "top performing stores",
    "worst store":              "bottom performing stores",
    "poor store":               "bottom performing stores",
    "low store":                "bottom performing stores",
    "underperform":             "bottom performing",
    "outperform":               "top performing",
    # Basket
    "basket size":              "units per transaction upt",
    "basket value":             "average transaction value atv",
    "avg basket":               "average transaction value upt",
    "transaction size":         "units per transaction upt",
}

# Intents that pilferage/discount/return keywords should trigger
LOSS_PILFERAGE_KEYWORDS = {
    "pilferage", "shrinkage", "theft", "stolen", "leakage",
    "unauthorized discount", "bill integrity", "fraud", "loss",
}
RETURN_KEYWORDS = {
    "returns", "refund", "credit note", "sales returns", "exchange",
}
DISCOUNT_KEYWORDS = {
    "discount", "markdown", "clearance", "promo", "promotion",
    "non-promo", "unauthorized",
}
PEAK_HOURS_KEYWORDS = {
    "peak hour", "peak hours", "peak time", "peak times",
    "high peak", "rush hour", "rush hours", "busy hour", "busy hours",
    "hourly sales", "hourly revenue", "hourly traffic", "foot traffic hour",
    "hourly performance", "by hour", "per hour", "store timing",
    "busiest hour", "busiest time",
}

PRODUCT_ALIGNMENT_KEYWORDS = {
    "product alignment", "item alignment", "item master", "product master",
    "product hierarchy", "product catalog", "product catalogue",
    "sku master", "article master", "option code", "icode alignment",
    "cost price", "mrp alignment", "item description", "division section department",
    "article option", "cost mrp",
}

DATE_PERIOD_KEYWORDS = {
    "year to date", "financial year to date", "fytd", "ytd", "fy to date",
    "since april", "from april", "month to date", "mtd", "this month",
    "week to date", "wtd", "this week", "quarter to date", "qtd",
    "like for like financial year", "ltl", "prior fy", "previous fy",
    "same period last fy", "compare fy", "as on date", "till date",
    "week no", "week number",
}

# ─── Zone Mapping ──────────────────────────────────────────────────────────────
# DB ZONE column values (exact, case-sensitive): 'UP East', 'North', 'East/JHK', 'South', 'Bihar'
# Longest entries first so we always match the most specific phrase first.

ZONE_ALIASES: list[tuple[str, str]] = [
    # UP East (match before bare "up" or "east")
    ("up east zone",  "UP East"),
    ("up east",       "UP East"),
    ("up zone",       "UP East"),
    # North
    ("north zone",    "North"),
    ("north",         "North"),
    # East / JHK
    ("east/jhk zone", "East/JHK"),
    ("east/jhk",      "East/JHK"),
    ("east jhk zone", "East/JHK"),
    ("east jhk",      "East/JHK"),
    ("jhk zone",      "East/JHK"),
    ("jhk",           "East/JHK"),
    ("jharkhand",     "East/JHK"),
    ("east zone",     "East/JHK"),
    # South
    ("south zone",    "South"),
    ("south",         "South"),
    # Bihar
    ("bihar zone",    "Bihar"),
    ("bihar",         "Bihar"),
]

# Bare "up" is ambiguous — only match if followed by zone/east or standalone zone context
_UP_PATTERN = re.compile(r'\bup\s+(zone|east)\b', re.IGNORECASE)


def extract_zone(text: str) -> dict:
    """
    Extract zone filter from query text.
    Returns {'zone': 'UP East', 'sql': "ZONE = 'UP East'"} or {} if no zone found.
    Tries longest alias match first.
    """
    t = text.lower().strip()
    for phrase, zone_val in ZONE_ALIASES:
        if phrase in t:
            return {"zone": zone_val, "sql": f"ZONE = '{zone_val}'"}
    # Extra check: bare "up" with zone/east nearby
    if _UP_PATTERN.search(t):
        return {"zone": "UP East", "sql": "ZONE = 'UP East'"}
    return {}


_MONTH_MAP = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}
_MON_PAT = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may"
    r"|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?"
    r"|nov(?:ember)?|dec(?:ember)?"
)


def extract_target_date(text: str) -> str:
    """
    Extract a specific user-mentioned date from a query string.
    Returns 'YYYY-MM-DD' or '' if no specific date is mentioned.

    Handles:
      '25 Feb 2026', '25th February 2026', 'Feb 25 2026',
      '25/02/2026', '25-02-2026', '2026-02-25'
    """
    t = text.lower()

    # Pattern: DD [Mon] YYYY  e.g. "25 Feb 2026", "25th February 2026"
    m = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(' + _MON_PAT + r')\s+(\d{4})\b', t
    )
    if m:
        mon = MONTH_MAP_get(m.group(2))
        if mon:
            return f"{m.group(3)}-{mon}-{int(m.group(1)):02d}"

    # Pattern: Mon DD YYYY  e.g. "February 25 2026", "Feb 25, 2026"
    m = re.search(
        r'\b(' + _MON_PAT + r')\s+(\d{1,2})(?:st|nd|rd|th)?[,]?\s+(\d{4})\b', t
    )
    if m:
        mon = MONTH_MAP_get(m.group(1))
        if mon:
            return f"{m.group(3)}-{mon}-{int(m.group(2)):02d}"

    # Pattern: YYYY-MM-DD
    m = re.search(r'\b(20\d{2})-(\d{2})-(\d{2})\b', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Pattern: DD/MM/YYYY or DD-MM-YYYY
    m = re.search(r'\b(\d{2})[/\-](\d{2})[/\-](20\d{2})\b', text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Pattern: DD Mon (no year) — e.g. "25 Feb", "25th Feb"  → infer year 2026
    m = re.search(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(' + _MON_PAT + r')\b', t)
    if m:
        mon = MONTH_MAP_get(m.group(2))
        if mon:
            return f"2026-{mon}-{int(m.group(1)):02d}"

    # Pattern: Mon DD (no year) — e.g. "Feb 25", "February 25"  → infer year 2026
    m = re.search(r'\b(' + _MON_PAT + r')\s+(\d{1,2})(?:st|nd|rd|th)?\b', t)
    if m:
        mon = MONTH_MAP_get(m.group(1))
        if mon:
            return f"2026-{mon}-{int(m.group(2)):02d}"

    return ""


def MONTH_MAP_get(key: str) -> str:
    """Look up month abbreviation/name → zero-padded month number."""
    for k, v in _MONTH_MAP.items():
        if key.startswith(k):
            return v
    return ""


def normalize_query(raw: str) -> dict:
    """
    Normalize a user query: expand abbreviations, fix spelling, apply aliases.
    Also extracts a specific target date if the user mentions one.

    Returns:
        {
            "original":     original raw query,
            "normalized":   cleaned, expanded query for pipeline,
            "corrections":  list of (original_token, corrected_token) tuples,
            "target_date":  'YYYY-MM-DD' if user named a specific date, else '',
            "zone_filter":  {'zone': str, 'sql': str} or {},
            "date_period":  {'period': 'YTD'|'MTD'|'WTD'|'QTD'|'LTL'|'WEEK_NO'|'TILL_DATE'|None,
                             'week_no': int|None},
            "flags": {
                "has_pilferage":  bool,
                "has_returns":    bool,
                "has_discount":   bool,
                "has_loss":       bool,
            }
        }
    """
    corrections = []
    text = raw.strip()

    # ── 1. Expand abbreviations (whole-word) ───────────────────────────────
    for abbr, expansion in sorted(ABBREVIATIONS.items(), key=lambda x: -len(x[0])):
        pattern = r'\b' + re.escape(abbr) + r'\b'
        if re.search(pattern, text, re.IGNORECASE):
            text = re.sub(pattern, expansion, text, flags=re.IGNORECASE)
            corrections.append((abbr, expansion))

    # ── 2. Correct misspellings (token level) ─────────────────────────────
    tokens = text.split()
    corrected_tokens = []
    for token in tokens:
        clean = re.sub(r'[^a-zA-Z0-9\-]', '', token).lower()
        if clean in SPELL_MAP:
            corrected = SPELL_MAP[clean]
            corrections.append((token, corrected))
            # Preserve trailing punctuation
            punct = token[len(clean):]
            corrected_tokens.append(corrected + punct)
        else:
            corrected_tokens.append(token)
    text = " ".join(corrected_tokens)

    # ── 3. Apply phrase aliases ────────────────────────────────────────────
    for phrase, canonical in sorted(PHRASE_ALIASES.items(), key=lambda x: -len(x[0])):
        pattern = r'\b' + re.escape(phrase) + r'\b'
        if re.search(pattern, text, re.IGNORECASE):
            text = re.sub(pattern, canonical, text, flags=re.IGNORECASE)
            corrections.append((phrase, canonical))

    # ── 4. Detect semantic flags ───────────────────────────────────────────
    text_lower = text.lower()
    flags = {
        "has_pilferage":         any(k in text_lower for k in LOSS_PILFERAGE_KEYWORDS),
        "has_returns":           any(k in text_lower for k in RETURN_KEYWORDS),
        "has_discount":          any(k in text_lower for k in DISCOUNT_KEYWORDS),
        "has_loss":              any(k in text_lower for k in {"loss", "shrinkage", "leakage", "wastage"}),
        "has_peak_hours":        any(k in text_lower for k in PEAK_HOURS_KEYWORDS),
        "has_product_alignment": any(k in text_lower for k in PRODUCT_ALIGNMENT_KEYWORDS),
    }

    # ── 5. Extract specific target date (highest priority for SQL generation) ─
    target_date = extract_target_date(raw)   # check raw — preserves original case/format

    # ── 6. Extract zone filter ────────────────────────────────────────────────
    # Check normalized text first, then original (handles abbreviation-expanded forms)
    zone_filter = extract_zone(text) or extract_zone(raw)

    # ── 7. Detect FY date period ─────────────────────────────────────────────
    from pipeline.date_engine import detect_date_period as _detect_period
    date_period_info = _detect_period(text)

    if corrections:
        logger.info(f"Query normalized: '{raw}' → '{text}' | fixes={len(corrections)}")
    if target_date:
        logger.info(f"Query target_date extracted: {target_date}")
    if zone_filter:
        logger.info(f"Query zone_filter extracted: {zone_filter}")

    return {
        "original":    raw,
        "normalized":  text,
        "corrections": corrections,
        "target_date": target_date,
        "zone_filter": zone_filter,
        "date_period": date_period_info,   # {'period': 'YTD'|..., 'week_no': int|None}
        "flags":       flags,
    }


def correction_summary(result: dict) -> str:
    """Short human-readable summary of corrections made."""
    corrections = result.get("corrections", [])
    if not corrections:
        return ""
    unique = list(dict.fromkeys((o, c) for o, c in corrections if o.lower() != c.lower()))
    if not unique:
        return ""
    parts = [f"'{o}' → '{c}'" for o, c in unique[:5]]
    return "Understood as: " + "; ".join(parts)
