# src/utils/postclean.py
from __future__ import annotations
import re
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
from src.utils.dedupe import dedupe_jobs

# Default rules (can be overridden via function args or ENV later)
NEGATIVE_KWS = [
    "java","developer","software","frontend","backend","full stack","sdet","qa",
    "devops","cloud","architect","implementation","customer success","support","help desk",
    "sales","marketing","nurse","physician","driver","teacher","secretary","receptionist",
    "custodian","warehouse","mechanic","electrician",
]
ALLOW_KWS = [
    "data","analyst","analytics","bi","business intelligence","insight","reporting","visualization","sql",
]

def _canonicalize_url(u: str) -> str:
    try:
        p = urlsplit(str(u))
        return urlunsplit((p.scheme, p.netloc, p.path, "", ""))
    except Exception:
        return str(u)

def _title_is_relevant(title: str, allow_kws, negative_kws) -> bool:
    t = (title or "").lower()
    allow  = any(k in t for k in allow_kws)
    excl   = any(re.search(rf"\b{re.escape(k)}\b", t) for k in negative_kws)
    return allow and not excl

def canonicalize_urls(df: pd.DataFrame) -> pd.DataFrame:
    url_col = next((c for c in ("job_url","url","link","posting_url") if c in df.columns), None)
    if not url_col:
        return df
    df = df.copy()
    df["_url_norm"] = df[url_col].map(_canonicalize_url)
    nonnull = df["_url_norm"].notna()
    out = pd.concat(
        [df[nonnull].drop_duplicates(subset=["_url_norm"], keep="first"), df[~nonnull]],
        ignore_index=True,
    )
    return out.drop(columns=["_url_norm"])

def secondary_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    keys = [k for k in ("title","company","city","state") if k in df.columns]
    return df.drop_duplicates(subset=keys, keep="first") if keys else df

def filter_titles(df: pd.DataFrame, allow_kws=None, negative_kws=None) -> pd.DataFrame:
    allow_kws = allow_kws or ALLOW_KWS
    negative_kws = negative_kws or NEGATIVE_KWS
    if "title" not in df.columns:
        return df
    mask = df["title"].map(lambda t: _title_is_relevant(t, allow_kws, negative_kws))
    return df[mask].reset_index(drop=True)

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Map possible site column to one canonical pick for front display
    site_col = next((c for c in ("site_name","site","source","platform","job_platform") if c in df.columns), None)
    url_col  = next((c for c in ("job_url","url","link","posting_url") if c in df.columns), None)
    front = [c for c in ["title","company", url_col, site_col] if c]
    rest  = [c for c in df.columns if c not in front]
    return df[front + rest]

def apply_cleaning(
    df: pd.DataFrame,
    *,
    allow_kws: list[str] | None = None,
    negative_kws: list[str] | None = None,
    keep_unrelated: bool = False,  # toggle to skip title filtering if desired
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

   # 1–2) Robust de-dupe (URL-based via _url_norm + composite keys), preserves full job_url
    out = dedupe_jobs(out)

    # 3) Filter unrelated titles
    if not keep_unrelated:
        out = filter_titles(out, allow_kws=allow_kws, negative_kws=negative_kws)

    # 4) Reorder columns for output UX
    out = reorder_columns(out)
    return out.reset_index(drop=True)
