from uuid import uuid4

from jsonschema import validate, ValidationError

from weavelib.exceptions import BadArguments


def from_type(pytype):
    types = {
        bool: "boolean",
        int: "number",
        float: "number",
        str: "string",
        dict: "object",
        list: "array",
    }

    if pytype not in types:
        raise BadArguments("Unsupported type.")

    return types[pytype]


class BaseSchema(object):
    pass


class Exactly(BaseSchema):
    def __init__(self, obj):
        self.obj = obj
        self.json_type = from_type(type(obj))

    def json_schema(self):
        return {
            "type": self.json_type,
            "enum": [self.obj]
        }

class JsonSchema(BaseSchema):
    def __init__(self, obj):
        self.schema = obj

    def json_schema(self):
        return self.schema


class OneOf(BaseSchema):
    def __init__(self, *objs):
        self.objs = objs

    def json_schema(self):
        return {
            "anyOf": [Exactly(x).json_schema() for x in self.objs]
        }


class ListOf(BaseSchema):
    def __init__(self, base_schema):
        self.item_type = base_schema

    def json_schema(self):
        return {"type": "array", "items": self.item_type.json_schema()}


class Type(BaseSchema):
    def __init__(self, pytype):
        if pytype not in (bool, int, float, str):
            raise BadArguments("Unsupported type.")
        self.json_type = from_type(pytype)

    def json_schema(self):
        return {"type": self.json_type}


class Parameter(object):
    SIMPLE_TYPE_SCHEMA = {
        str: {"type": "string"},
        int: {"type": "number"},
        bool: {"type": "boolean"}
    }

    def __init__(self, name, desc, schema):
        self.name = name
        self.desc = desc
        if isinstance(schema, type):
            if schema not in (str, int, bool):
                raise ValueError("Unexpected type for parameter.")
            self.param_schema = self.SIMPLE_TYPE_SCHEMA[schema]
        elif isinstance(schema, dict):
            # TODO: Validate with meta-schema
            self.param_schema = schema
        elif isinstance(schema, BaseSchema):
            self.param_schema = schema.json_schema()
        elif callable(schema):
            self.param_schema = schema

    @property
    def schema(self):
        if callable(self.param_schema):
            json_schema = self.param_schema()
            if not isinstance(json_schema, BaseSchema):
              raise ValueError("Callable should return a BaseSchema instance")
            return json_schema.json_schema()
        return self.param_schema

    @property
    def info(self):
        return {
            "name": self.name,
            "description": self.desc,
            "schema": self.schema
        }


class ArgParameter(Parameter):
    positional = True

    @staticmethod
    def from_info(info):
        try:
            return ArgParameter(info["name"], info["description"],
                                info["schema"])
        except KeyError:
            raise BadArguments("Invalid ArgParameter info object.")


class KeywordParameter(Parameter):
    positional = False

    @staticmethod
    def from_info(info):
        try:
            return KeywordParameter(info["name"], info["description"],
                                    info["schema"])
        except KeyError:
            raise BadArguments("Invalid KeywordParameter info object.")


class API(object):
    def __init__(self, name, desc, params):
        self.name = name
        self.description = desc
        self.args = [x for x in params if x.positional]
        self.kwargs = [x for x in params if not x.positional]

    @property
    def schema(self):
        obj = {
            "type": "object",
            "properties": {
                "command": {"enum": [self.name]},
                "id": {"type": "string"},
            },
            "additionalProperties": False,
            "required": ["command", "id"],
        }

        if self.args:
            obj["properties"]["args"] = {
                "type": "array",
                "items": [p.schema for p in self.args],
                "minItems": len(self.args),
                "maxItems": len(self.args)
            }
            obj["required"].append("args")

        if self.kwargs:
            obj["properties"]["kwargs"] = {
                "type": "object",
                "properties": {p.name: p.schema for p in self.kwargs},
                "required": [p.name for p in self.kwargs]
            }
            obj["required"].append("kwargs")

        return obj

    @property
    def info(self):
        return {
            "name": self.name,
            "description": self.description,
            "args": [x.info for x in self.args],
            "kwargs": {x.name: x.info for x in self.kwargs},
            "request_schema": self.schema,
            "response_schema": {}
        }

    def validate_call(self, *args, **kwargs):
        obj = {"command": self.name, "id": "invocation-" + str(uuid4())}
        if args:
            obj["args"] = list(args)
        if kwargs:
            obj["kwargs"] = kwargs

        try:
            validate(obj, self.schema)
        except ValidationError:
            raise BadArguments("Bad parameters for function call.")

        return obj

    @staticmethod
    def from_info(info):
        try:
            api = API(info["name"], info["description"], [])
        except KeyError:
            raise BadArguments("Invalid API info object.")

        api.args = [ArgParameter.from_info(x) for x in info.get("args", [])]
        api.kwargs = [KeywordParameter.from_info(x) for x in
                      info.get("kwargs", {}).values()]

        return api
