from __future__ import annotations

import pandas as pd
from urllib.parse import urlparse


def _normalize_url(u: str) -> str | None:
    """Return a stable URL key: host + path (lowercased host, no query/fragment)."""
    if not isinstance(u, str) or not u.strip():
        return None
    try:
        p = urlparse(u.strip())
        host = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        if not host:
            return None
        return f"{host}{path}"
    except Exception:
        return None


def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prefer deduplication by normalized job_url.
    If no usable URLs exist, fall back to a cross-board-safe composite key.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # 1) URL-based dedupe (preferred)
    url_col = None
    if "job_url" in out.columns:
        url_col = "job_url"
    elif "JOB_URL" in out.columns:
        url_col = "JOB_URL"

    if url_col:
        out["_url_norm"] = out[url_col].apply(_normalize_url)
        # Drop dups where _url_norm is present (keep first); keep rows with NaN separately
        nonnull = out["_url_norm"].notna()
        out = pd.concat(
            [
                out[nonnull].drop_duplicates(subset=["_url_norm"], keep="first"),
                out[~nonnull],
            ],
            ignore_index=True,
        )
        out.drop(columns=["_url_norm"], inplace=True, errors="ignore")

    # 2) Fallback composite key if URL isn’t usable
    need_fallback = (url_col is None) or out[url_col].isna().all()
    if need_fallback:
        keys = [c for c in ["site_name", "title", "company", "city", "state"] if c in out.columns]
        if not keys:
            # uppercase fallback (legacy pre-normalization)
            keys = [c for c in ["SITE", "TITLE", "COMPANY", "LOCATION"] if c in out.columns]
        if keys:
            out = out.drop_duplicates(subset=keys, keep="first")
        else:
            out = out.drop_duplicates(keep="first")

    return out.reset_index(drop=True)
