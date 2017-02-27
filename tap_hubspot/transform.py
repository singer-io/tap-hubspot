import datetime
from tap_hubspot import utils


class InvalidData(Exception):
    """Raise when data doesn't validate the schema"""


def transform_row(row, schema):
    print(row)
    return _transform_field(row, schema)


def _transform_datetime(value):
    return utils.strftime(datetime.datetime.utcfromtimestamp(int(value) * 0.001))


def _anyOf(data, schema_list):
    for schema in schema_list:
        try:
            return _transform_field(data, schema)
        except Exception as e:
            pass

    raise InvalidData("{} doesn't match any of {}".format(data, schema_list))


def _array(data, items_schema):
    return [_transform_field(value, items_schema) for value in data]


def _object(data, properties_schema):
    return {field: _transform_field(data[field], field_schema)
            for field, field_schema in properties_schema.items()
            if field in data}


def _type_transform(value, type_schema):
    if isinstance(type_schema, list):
        for typ in type_schema:
            try:
                return _type_transform(value, typ)
            except:
                pass

        raise InvalidData("{} doesn't match any of {}".format(value, type_schema))

    if not value:
        if type_schema != "null":
            raise InvalidData("Null is not allowed")
        else:
            return None

    if type_schema == "string":
        return str(value)

    if type_schema == "integer":
        return int(value)

    if type_schema == "number":
        return float(value)

    if type_schema == "boolean":
        return bool(value)

    raise InvalidData("Unknown type {}".format(type_schema))


def _format_transform(value, format_schema):
    if format_schema == "date-time":
        return _transform_datetime(value)

    raise InvalidData("Unknown format {}".format(format_schema))


def _transform_field(value, field_schema):
    if "anyOf" in field_schema:
        return _anyOf(value, field_schema["anyOf"])

    if field_schema["type"] == "array":
        return _array(value, field_schema["items"])

    if field_schema["type"] == "object":
        return _object(value, field_schema["properties"])

    value = _type_transform(value, field_schema["type"])
    if "format" in field_schema:
        value = _format_transform(value, field_schema["format"])

    return value
