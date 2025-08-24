
def build_boolean_query(titles):
    parts = []
    for t in titles or []:
        t = (t or "").strip()
        if not t:
            continue
        if " " in t and not (t.startswith('"') and t.endswith('"')):
            parts.append(f'"{t}"')
        else:
            parts.append(t)
    return " OR ".join(parts) if parts else ""
