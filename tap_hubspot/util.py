import copy


def record_nodash(obj):
    transformed_obj = copy.deepcopy(obj)

    if not isinstance(obj, (dict, list)):
        return obj
    if isinstance(obj, dict):
        for key in obj:
            value = record_nodash(obj[key])
            transformed_obj.pop(key)
            key = key.replace("-", "_")
            transformed_obj[key] = value
    if isinstance(obj, list):
        for i in range(len(obj)):
            value = record_nodash(obj[i])
            transformed_obj[i] = value
    return transformed_obj


def schema_nodash(obj):
    type_field = obj.get("type")
    type = get_type(type_field)
    if not type:
        return obj
    if not type in ["array", "object"]:
        return obj
    if "object" == type:
        props = obj.get("properties", {})
        new_props = replace_props(props)
        obj["properties"] = new_props
    if "array" == type:
        items = obj.get("items", {})
        obj["items"] = schema_nodash(items)
    return obj


def get_type(type_field):
    if isinstance(type_field, str):
        return type_field
    if isinstance(type_field, list):
        types = set(type_field)
        if "null" in types:
            types.remove("null")
        return types.pop()
    return None


def replace_props(props):
    if not props:
        return props
    keys = list(props.keys())
    for k in keys:
        if not "-" in k:
            props[k] = schema_nodash(props[k])
        else:
            v = props.pop(k)
            new_key = k.replace("-", "_")
            new_value = schema_nodash(v)
            props[new_key] = new_value
    return props
