"""
DSR|RIECT — Financial Year Date Engine
Indian FY: April 1 – March 31
FY2025-26 = Apr 1 2025 – Mar 31 2026
"""
from datetime import date, timedelta

FY_START_MONTH = 4  # April
FY_START_DAY   = 1


def get_fy_year(d: date) -> int:
    """FY start year: Mar = prior year, Apr+ = this year."""
    return d.year if d.month >= FY_START_MONTH else d.year - 1


def get_fy_start(d: date) -> date:
    return date(get_fy_year(d), FY_START_MONTH, FY_START_DAY)


def get_fy_end(d: date) -> date:
    return date(get_fy_year(d) + 1, 3, 31)


def get_fy_label(d: date) -> str:
    y = get_fy_year(d)
    return f"FY{y}-{str(y + 1)[2:]}"


def get_fy_week_number(d: date) -> int:
    """Week 1 = first 7 days of April. Returns 1-52."""
    return (d - get_fy_start(d)).days // 7 + 1


def get_fy_week_range(d: date, week_no: int) -> tuple[date, date]:
    """(start, end) for given FY week number. End capped at d."""
    fy_s    = get_fy_start(d)
    w_start = fy_s + timedelta(weeks=week_no - 1)
    w_end   = min(w_start + timedelta(days=6), d)
    return w_start, w_end


def days_elapsed(start: date, end: date) -> int:
    return (end - start).days + 1


def get_ytd_range(d: date) -> tuple[date, date]:
    return get_fy_start(d), d


def get_mtd_range(d: date) -> tuple[date, date]:
    return date(d.year, d.month, 1), d


def get_wtd_range(d: date) -> tuple[date, date]:
    return d - timedelta(days=d.weekday()), d


def get_qtd_range(d: date) -> tuple[date, date]:
    """FY quarter: Apr-Jun, Jul-Sep, Oct-Dec, Jan-Mar."""
    fy_s = get_fy_start(d)
    months_in_fy = (d.year - fy_s.year) * 12 + d.month - fy_s.month
    q_offset = (months_in_fy // 3) * 3
    raw_m  = fy_s.month + q_offset
    q_year = fy_s.year + (raw_m - 1) // 12
    q_mon  = (raw_m - 1) % 12 + 1
    return date(q_year, q_mon, 1), d


def get_ltl_range(d: date, period: str = "MTD") -> dict:
    """
    LTL = same calendar period, prior FY year.
    Returns: {current_start, current_end, prior_start, prior_end, current_label, prior_label}
    """
    p = period.upper()
    if p == "MTD":
        cur_s, cur_e = get_mtd_range(d)
    elif p == "WTD":
        cur_s, cur_e = get_wtd_range(d)
    elif p == "YTD":
        cur_s, cur_e = get_ytd_range(d)
    elif p == "QTD":
        cur_s, cur_e = get_qtd_range(d)
    else:
        cur_s = cur_e = d

    # Prior = same calendar dates -1 year (handle Feb 29 leap year)
    def _prior(dt: date) -> date:
        try:
            return date(dt.year - 1, dt.month, dt.day)
        except ValueError:
            return date(dt.year - 1, dt.month, 28)

    pri_s = _prior(cur_s)
    pri_e = _prior(cur_e)

    return {
        "current_start": cur_s.isoformat(),
        "current_end":   cur_e.isoformat(),
        "prior_start":   pri_s.isoformat(),
        "prior_end":     pri_e.isoformat(),
        "current_label": f"{cur_s.strftime('%b %Y')} ({get_fy_label(d)})",
        "prior_label":   f"{pri_s.strftime('%b %Y')} ({get_fy_label(pri_e)})",
    }


def detect_date_period(text: str) -> dict:
    """
    Detect user's intended date period from normalized text.
    Returns: {'period': 'YTD'|'MTD'|'WTD'|'QTD'|'LTL'|'WEEK_NO'|'TILL_DATE'|None,
              'week_no': int|None}
    """
    import re
    t = text.lower()

    # LTL — check before YOY to avoid conflict
    if any(k in t for k in ("like for like financial year", "ltl", "compare fy",
                             "prior fy", "previous fy", "same period last fy",
                             "fy comparison", "year on year financial")):
        return {"period": "LTL", "week_no": None}

    # YTD / FYTD
    if any(k in t for k in ("financial year to date", "fytd", "fy to date",
                             "year to date", "ytd", "since april", "from april")):
        return {"period": "YTD", "week_no": None}

    # QTD
    if any(k in t for k in ("quarter to date", "qtd", "this quarter")):
        return {"period": "QTD", "week_no": None}

    # MTD
    if any(k in t for k in ("month to date", "mtd", "this month", "current month")):
        return {"period": "MTD", "week_no": None}

    # WTD
    if any(k in t for k in ("week to date", "wtd", "this week", "current week", "since monday")):
        return {"period": "WTD", "week_no": None}

    # Week number — "week 23", "week no 15", "week #7"
    m = re.search(r'\bweek\s*(?:no\.?\s*|number\s*|#\s*)?(\d{1,2})\b', t)
    if m:
        return {"period": "WEEK_NO", "week_no": int(m.group(1))}

    # Till date / As on date
    if any(k in t for k in ("as on date", "as on", "till date", "upto", "as of")):
        return {"period": "TILL_DATE", "week_no": None}

    return {"period": None, "week_no": None}


def build_fy_context(latest_sales_date_str: str, date_period: str = None, week_no: int = None) -> dict:
    """
    Compute full FY context from latest_sales_date string.
    Called from orchestrator.execute() after data freshness check.
    """
    if not latest_sales_date_str:
        return {}
    try:
        d = date.fromisoformat(latest_sales_date_str)
    except ValueError:
        return {}

    fy_s  = get_fy_start(d)
    fy_e  = get_fy_end(d)
    ytd_s, _ = get_ytd_range(d)
    mtd_s, _ = get_mtd_range(d)
    wtd_s, _ = get_wtd_range(d)
    qtd_s, _ = get_qtd_range(d)

    ctx = {
        "fy_start":         fy_s.isoformat(),
        "fy_end":           fy_e.isoformat(),
        "fy_label":         get_fy_label(d),
        "fy_week_no":       get_fy_week_number(d),
        "ytd_start":        ytd_s.isoformat(),
        "mtd_start":        mtd_s.isoformat(),
        "wtd_start":        wtd_s.isoformat(),
        "qtd_start":        qtd_s.isoformat(),
        "days_elapsed_fy":  days_elapsed(fy_s, d),
        "days_elapsed_mtd": days_elapsed(mtd_s, d),
        "days_elapsed_wtd": days_elapsed(wtd_s, d),
        "date_period":      date_period or "MTD",
        "week_start":       "",
        "week_end":         "",
        "week_label":       "",
    }

    # Week range (when week number is specified)
    if week_no:
        w_start, w_end = get_fy_week_range(d, week_no)
        ctx["week_start"] = w_start.isoformat()
        ctx["week_end"]   = w_end.isoformat()
        ctx["week_label"] = f"Week {week_no} ({w_start.strftime('%d %b')}–{w_end.strftime('%d %b %Y')})"

    # LTL auxiliary context: use MTD/WTD/QTD if those periods were requested;
    # for YTD, WEEK_NO, TILL_DATE, LTL, or None — default to MTD (most practical comparison)
    ltl_base = date_period if date_period in ("MTD", "WTD", "QTD") else "MTD"
    ltl = get_ltl_range(d, ltl_base)
    ctx.update({
        "ltl_current_start": ltl["current_start"],
        "ltl_current_end":   ltl["current_end"],
        "ltl_prior_start":   ltl["prior_start"],
        "ltl_prior_end":     ltl["prior_end"],
        "ltl_current_label": ltl["current_label"],
        "ltl_prior_label":   ltl["prior_label"],
    })

    return ctx
