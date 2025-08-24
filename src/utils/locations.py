
def expand_locations(locs, enable=True):
    return [l.strip() for l in (locs or []) if l and l.strip()]
