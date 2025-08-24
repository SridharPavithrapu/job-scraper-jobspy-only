# src/utils/filtering.py
from __future__ import annotations

import re
from datetime import timedelta
from typing import Iterable, Optional, Tuple

import pandas as pd


# ----------------------------
# Small helpers
# ----------------------------

def _series_str(s: pd.Series) -> pd.Series:
    """Safe stringification with empty string for NaNs."""
    return s.astype(str).fillna("")

def _combine_text(row: pd.Series) -> str:
    """
    Combine common text fields for regex checks (employment type, remote hints, experience, etc.).
    We keep it conservative and avoid joining huge blobs twice.
    """
    parts = []
    for c in (
        "title", "TITLE",
        "description", "DESCRIPTION",
        "job_type", "JOB_TYPE",
        "employment_type", "EMPLOYMENT_TYPE",
        "job_function", "JOB_FUNCTION",
        "onsite_remote", "work_mode",
        "company", "COMPANY",
        "location", "LOCATION",
    ):
        if c in row and pd.notna(row[c]):
            parts.append(str(row[c]))
    return " ".join(parts)


# ----------------------------
# Work mode (Remote/On-site/Hybrid)
# ----------------------------

_REMOTE_PAT = re.compile(
    r"\b(remote|work\s*from\s*home|wfh|fully\s*remote|100%\s*remote|us-?remote|anywhere)\b",
    re.I,
)
_ONSITE_PAT = re.compile(
    r"\b(on[\s-]?site|onsite|in[\s-]?office|in[\s-]?person)\b",
    re.I,
)
_HYBRID_PAT = re.compile(
    r"\b(hybrid|(\d+\s*(days?|d)/week\s*in\s*office))\b",
    re.I,
)

def filter_by_work_mode(df: pd.DataFrame, work_mode: str = "any") -> pd.DataFrame:
    """
    Filter by desired work mode:
      - "any": no filtering
      - "remote only": keep clear-remote rows (prefer is_remote==True; include text-remote, exclude explicitly hybrid/onsite)
      - "on-site only": keep explicit on-site rows (is_remote==False or onsite text), exclude clear-remote
      - "hybrid only": keep hybrid-mention rows
    """
    if df is None or df.empty:
        return df

    mode = (work_mode or "any").strip().lower()
    if mode == "any":
        return df

    # Build boolean masks
    is_remote_col = df["is_remote"].astype("boolean") if "is_remote" in df.columns else pd.Series([pd.NA] * len(df))
    text = df.apply(_combine_text, axis=1)

    text_remote = text.str.contains(_REMOTE_PAT, na=False)
    text_onsite = text.str.contains(_ONSITE_PAT, na=False)
    text_hybrid = text.str.contains(_HYBRID_PAT, na=False)

    if mode.startswith("remote"):
        mask = (is_remote_col.fillna(False)) | text_remote
        # Prefer pure remote; drop obvious hybrid-only mentions
        mask = mask & ~text_hybrid
        return df[mask].reset_index(drop=True)

    if mode.startswith("on-site") or mode.startswith("onsite"):
        mask = (is_remote_col == False) | text_onsite  # noqa: E712
        # Exclude rows that also look remote
        mask = mask & ~text_remote
        return df[mask].reset_index(drop=True)

    if mode.startswith("hybrid"):
        mask = text_hybrid
        return df[mask].reset_index(drop=True)

    # Fallback: no filtering
    return df


# ----------------------------
# Employment type (Full-time, Contract, W2, etc.)
# ----------------------------

_EMPLOY_PATTERNS = {
    "full-time": re.compile(r"\bfull[\s-]*time\b|\bfulltime\b|\bFTE\b|\bpermanent\b|\bFT\b", re.I),
    "contract":  re.compile(r"\bcontract(?!or\b)\b|\bcontractor\b|\bC2C\b|\bcorp[-\s]*to[-\s]*corp\b|\bC2H\b|\bcontract[-\s]*to[-\s]*hire\b", re.I),
    "w2":        re.compile(r"\bW[\s-]?2\b", re.I),
    "parttime":  re.compile(r"\bpart[\s-]*time\b|\bparttime\b|\bPT\b", re.I),
    "internship": re.compile(r"\bintern(ship)?\b", re.I),
}

def filter_by_employment_type(df: pd.DataFrame, employment: Optional[str] = None) -> pd.DataFrame:
    """
    Keep rows that mention the given employment type (textual heuristics).
    employment: "any" | "full-time" | "contract" | "w2" | "parttime" | "internship"
    """
    if df is None or df.empty or not employment:
        return df

    norm = employment.strip().lower().replace("_", "-")
    if norm in ("any", "none"):
        return df
    if norm in {"full time", "fulltime", "ft"}:
        norm = "full-time"

    pat = _EMPLOY_PATTERNS.get(norm)
    if not pat:
        return df

    mask = df.apply(lambda r: bool(pat.search(_combine_text(r))), axis=1)
    return df[mask].reset_index(drop=True)


# ----------------------------
# Experience (min/max years)
# ----------------------------

_RANGE_PAT = re.compile(r"(?P<a>\d+)\s*(?:[-–to]{1,3})\s*(?P<b>\d+)\s*(?:\+)?\s*years?", re.I)
_SINGLE_PAT = re.compile(r"(?P<n>\d+)\s*\+?\s*years?", re.I)
_ENTRY_PAT = re.compile(r"\b(entry[\s-]*level|junior|jr\.?)\b", re.I)
_SENIOR_PAT = re.compile(r"\b(senior|sr\.?|lead|principal|staff)\b", re.I)

def _extract_years_range(text: str) -> Optional[Tuple[Optional[int], Optional[int]]]:
    """
    Extract a (min_years, max_years) tuple if possible from text.
    Returns (min, max) where either may be None if not explicit.
    """
    if not text:
        return None

    m = _ RANGE_PAT.search(text) if False else None  # avoid flake8 confusion below
    m = _RANGE_PAT.search(text)
    if m:
        a, b = int(m.group("a")), int(m.group("b"))
        lo, hi = (a, b) if a <= b else (b, a)
        return lo, hi

    m = _SINGLE_PAT.search(text)
    if m:
        n = int(m.group("n"))
        return n, None  # lower bound only

    # Heuristic for explicit level words if no numbers
    if _ENTRY_PAT.search(text):
        return 0, 2
    if _SENIOR_PAT.search(text):
        return 5, None

    return None

def filter_by_experience(
    df: pd.DataFrame,
    min_years: Optional[int] = None,
    max_years: Optional[int] = None,
    keep_unknown: bool = True,
) -> pd.DataFrame:
    """
    Keep rows whose required experience overlaps [min_years, max_years].
    - If a row has unknown experience and keep_unknown=True, keep it.
    - If only a lower or upper bound is present in the text, use that.
    """
    if df is None or df.empty or (min_years is None and max_years is None):
        return df

    texts = pd.concat(
        [
            _series_str(df.get("title", df.get("TITLE", pd.Series([""] * len(df))))),
            _series_str(df.get("description", df.get("DESCRIPTION", pd.Series([""] * len(df))))),
        ],
        axis=1,
    ).agg(" ".join, axis=1)

    def _ok(s: str) -> bool:
        rng = _extract_years_range(s)
        if rng is None:
            return keep_unknown
        lo, hi = rng
        # If we only know a lower bound
        if hi is None and lo is not None:
            if max_years is not None and lo > max_years:
                return False
            if min_years is not None and lo < min_years:
                # If only "min" desired and lo is below it, still fine—could be 3+ and user wants 5+? Drop then.
                return lo >= min_years
            return True
        # If we only know upper bound
        if lo is None and hi is not None:
            if min_years is not None and hi < min_years:
                return False
            return True
        # Both bounds known
        if lo is not None and min_years is not None and hi is not None and hi < min_years:
            return False
        if hi is not None and max_years is not None and lo is not None and lo > max_years:
            return False
        # overlaps
        return True

    mask = texts.apply(_ok)
    return df[mask].reset_index(drop=True)


# ----------------------------
# Title filter (with abbreviations & senior variants)
# ----------------------------

# Common senior/lead prefixes
_PREFIX = r"(?:(?:sr|senior|lead|principal|staff)\s+)?"

def _abbr_patterns_for(term: str) -> Iterable[str]:
    """
    Given a title term, return extra abbreviation patterns to match.
    Keep abbreviations precise (word-boundaries) to minimize false positives.
    """
    t = term.strip().lower()

    patterns = []

    if "business intelligence engineer" in t:
        patterns += [r"\bBIE\b"]
    if "business intelligence analyst" in t or t == "business intelligence":
        patterns += [r"\bBI\b", r"\bBI\s*analyst\b", r"\bBIE\b"]
    if "power bi" in t:
        patterns += [r"\bPower\s*BI\b", r"\bPBI\b"]
    if t == "business analyst":
        patterns += [r"\bBA\b"]  # keep BA; too short for many other terms otherwise
    # Avoid DA (too ambiguous: District Attorney, etc.)

    return patterns

def _term_to_regex(term: str) -> str:
    """
    Build a tolerant regex for a search term:
      - allow senior/lead prefixes
      - be case-insensitive
      - accept spaces/hyphens flexibly
    """
    t = term.strip()
    if not t:
        return ""
    # Escape each token but allow flexible whitespace/hyphen between words
    tokens = [re.escape(tok) for tok in re.split(r"\s+", t)]
    middle = r"[\s\-]+"  # space or hyphen between words
    core = middle.join(tokens)
    return rf"{_PREFIX}{core}"

def filter_title_contains_any(
    df: pd.DataFrame,
    titles: list[str],
    include_abbrevs: bool = True,
    add_match_column: bool = True,
) -> pd.DataFrame:
    """
    Keep rows whose title contains ANY of the requested phrases.
    - Case-insensitive
    - Tolerates Sr/Senior/Lead/Principal prefixes
    - Optionally matches common abbreviations (BI, BIE, BA, Power BI)
    """
    if df is None or df.empty or not titles:
        return df

    title_col = "title" if "title" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
    if not title_col:
        return df

    # Build regex patterns for provided titles
    main_patterns = [p for p in (_term_to_regex(t) for t in titles if t and t.strip()) if p]
    abbr_patterns: list[str] = []
    if include_abbrevs:
        for t in titles:
            abbr_patterns.extend(_abbr_patterns_for(t or ""))

    # Combine to a single alternation
    alts = []
    if main_patterns:
        alts.append(r"(?:%s)" % "|".join(main_patterns))
    if abbr_patterns:
        alts.append(r"(?:%s)" % "|".join(abbr_patterns))
    if not alts:
        return df

    big_pat = re.compile("|".join(alts), re.I)

    def _match_title(s: str) -> tuple[bool, Optional[str]]:
        m = big_pat.search(s or "")
        if not m:
            return False, None
        # Return the first non-empty named/unnamed group match as the matched phrase
        return True, m.group(0)

    keep = []
    matched = []
    for s in _series_str(df[title_col]):
        ok, ph = _match_title(s)
        keep.append(ok)
        matched.append(ph)

    mask = pd.Series(keep, index=df.index)
    out = df[mask].copy()

    if add_match_column:
        out["MATCH_TERM"] = [m for m, k in zip(matched, keep) if k]

    return out.reset_index(drop=True)


# ----------------------------
# Hours filter (date-aware for Indeed)
# ----------------------------

def filter_by_hours(df: pd.DataFrame, hours_old: int, keep_unknown: bool = True) -> pd.DataFrame:
    """
    Keep rows whose date_posted is within the last N hours.
    - Indeed frequently supplies date-only values ("YYYY-MM-DD"). For those rows,
      we compare by DATE (not time) to avoid excluding same-day posts.
    - For other sites with midnight timestamps (likely date-only), we also fall back to date compare.
    - If keep_unknown=True, rows with missing date_posted are kept.
    """
    if df is None or df.empty or hours_old is None:
        return df

    now = pd.Timestamp.now(tz=None)
    cutoff_ts = now - pd.Timedelta(hours=hours_old)
    cutoff_date = cutoff_ts.date()

    s = df.get("date_posted")
    if s is None:
        return df

    dt = pd.to_datetime(s, errors="coerce")

    # Start with: keep unknown if requested
    mask = pd.Series(keep_unknown & dt.isna(), index=df.index)

    # Detect Indeed rows
    site_col = "site_name" if "site_name" in df.columns else None
    is_indeed = df[site_col].str.lower().eq("indeed") if site_col else pd.Series(False, index=df.index)

    # Indeed: compare by DATE
    if is_indeed.any():
        date_vals = dt[is_indeed].dt.date
        mask.loc[is_indeed & dt.notna()] = date_vals >= cutoff_date

    # Others: time-aware, but treat midnight as date-only
    other = ~is_indeed
    if other.any():
        dt_other = dt[other]
        # Midnight detection (00:00)
        midnight = dt_other.dt.time == pd.Timestamp(0).time()
        if midnight.any():
            mask.loc[other & midnight & dt_other.notna()] = dt_other[midnight].dt.date >= cutoff_date
        # Full timestamps use precise comparison
        mask.loc[other & ~midnight & dt_other.notna()] = dt_other[~midnight] >= cutoff_ts

    return df[mask].reset_index(drop=True)
