try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

# https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
def nested_update(d, u):
    for k, v in u.items():
        if isinstance(v, Mapping):
            d[k] = nested_update(d.get(k, {}), v)
        elif isinstance(v, list) or isinstance(d[k], list):
            l1 = d.get(k, [])
            l2 = v or []
            d[k] = l1 + l2
        else:
            d[k] = v
    return d
