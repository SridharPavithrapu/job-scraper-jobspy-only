from __future__ import annotations

import re
import pandas as pd
from urllib.parse import urlparse


def _normalize_url(u: str) -> str | None:
    """
    Return a stable URL key: host(without 'www.') + path (lowercased host, no query/fragment, no trailing slash).
    Scheme, query, and fragment are ignored; host is lowercased and 'www.' is stripped.
    """
    if not isinstance(u, str) or not u.strip():
        return None
    try:
        p = urlparse(u.strip())
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/")
        if not host:
            return None
        return f"{host}{path}"
    except Exception:
        return None


_WS_RE = re.compile(r"\s+", re.UNICODE)
_PUNC_RE = re.compile(r"[^\w\s]+", re.UNICODE)


def _norm_text(s: str) -> str:
    """
    Lightweight text normalizer for near-duplicate keys.
    Lowercase, strip punctuation, collapse spaces.
    """
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = _PUNC_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _choose_best(rows: pd.DataFrame) -> pd.Series:
    """
    From a group of near-duplicates, pick the most recent by date_posted if present,
    otherwise keep the first (stable).
    """
    if "date_posted" in rows.columns:
        try:
            dt = pd.to_datetime(rows["date_posted"], errors="coerce", utc=True)
            idx = dt.idxmax()
            if pd.notna(idx):
                return rows.loc[idx]
        except Exception:
            pass
    return rows.iloc[0]


def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Multi-step deduplication:

    1) URL-based dedupe (preferred): collapse identical normalized URLs.
    2) Composite near-dup pass (always): collapse identical (company,title,city,state) after text normalization.
       - Prefers most recent date_posted when available.
    3) Fallback when absolutely no keys: drop exact duplicates.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # -------- 1) URL-based dedupe (preferred) --------
    url_col = "job_url" if "job_url" in out.columns else ("JOB_URL" if "JOB_URL" in out.columns else None)
    if url_col:
        out["_url_norm"] = out[url_col].apply(_normalize_url)
        nonnull = out["_url_norm"].notna()
        # dedupe only by _url_norm, but KEEP the full original job_url
        out = pd.concat(
            [
                out[nonnull].drop_duplicates(subset=["_url_norm"], keep="first"),
                out[~nonnull],
            ],
            ignore_index=True,
        )
        out.drop(columns=["_url_norm"], inplace=True, errors="ignore")

    # -------- 2) Composite near-duplicate pass (always) --------
    # Use canonical column names if present; fall back to common legacy names.
    col_company = "company" if "company" in out.columns else ("COMPANY" if "COMPANY" in out.columns else None)
    col_title   = "title"   if "title"   in out.columns else ("TITLE"   if "TITLE"   in out.columns else None)

    # Prefer split city/state if available; otherwise try to parse from a single 'location' column.
    city_col  = "city"  if "city"  in out.columns else None
    state_col = "state" if "state" in out.columns else None
    loc_col   = None
    if not city_col or not state_col:
        loc_col = "location" if "location" in out.columns else ("LOCATION" if "LOCATION" in out.columns else None)

    if col_company and col_title and ( (city_col and state_col) or loc_col ):
        tmp = out.copy()

        tmp["_company_norm"] = tmp[col_company].map(_norm_text)
        tmp["_title_norm"]   = tmp[col_title].map(_norm_text)

        if city_col and state_col:
            tmp["_city_norm"]  = tmp[city_col].map(_norm_text)
            tmp["_state_norm"] = tmp[state_col].map(_norm_text)
        else:
            # Parse "City, ST" best effort
            city, state = [], []
            vals = tmp[loc_col].astype(str).fillna("")
            for v in vals:
                parts = [p.strip() for p in v.split(",")]
                c = parts[0] if len(parts) >= 1 else ""
                s = parts[1] if len(parts) >= 2 else ""
                city.append(_norm_text(c))
                state.append(_norm_text(s))
            tmp["_city_norm"]  = city
            tmp["_state_norm"] = state

        grp_cols = ["_company_norm", "_title_norm", "_city_norm", "_state_norm"]
        # For speed with large frames: aggregate via groupby then select preferred row per group
        picks = (
            tmp.groupby(grp_cols, dropna=False, sort=False, as_index=False, group_keys=False)
               .apply(_choose_best)
        )
        # 'picks' is a subset with duplicates collapsed; keep order stable
        out = picks.reset_index(drop=True)

        # Clean temp cols
        out.drop(columns=[c for c in ["_company_norm","_title_norm","_city_norm","_state_norm"] if c in out.columns],
                 inplace=True, errors="ignore")

    else:
        # -------- 3) Fallback when we lack keys --------
        # If URL step didn't apply and we have nothing else, drop exact duplicates.
        if not url_col:
            out = out.drop_duplicates(keep="first")

    return out.reset_index(drop=True)
