import pytest
from jsonschema import validate

from weavelib.exceptions import BadArguments
from weavelib.rpc import ArgParameter, KeywordParameter, JsonSchema, OneOf
from weavelib.rpc import Exactly, ListOf, Type
from weavelib.rpc.api import API


class TestParameter(object):
    def test_bad_type(self):
        with pytest.raises(ValueError):
            ArgParameter("", "", dict)

    def test_schema(self):
        assert {"type": "string"} == ArgParameter("", "", str).schema
        assert {"type": "number"} == ArgParameter("", "", int).schema
        assert {"type": "boolean"} == KeywordParameter("", "", bool).schema
        assert {"type": "object"} == KeywordParameter("", "",
                                                      {"type": "object"}).schema

    def test_info(self):
        assert ArgParameter("a", "b", str).info == {
            "name": "a",
            "description": "b",
            "schema": {"type": "string"}
        }

    def test_arg_parameter_from_info(self):
        obj = {
            "name": "a",
            "description": "b",
            "schema": {"type": "object"}
        }
        assert ArgParameter.from_info(obj).info == obj

    def test_arg_from_bad_info(self):
        with pytest.raises(BadArguments):
            KeywordParameter.from_info({})

        with pytest.raises(BadArguments):
            ArgParameter.from_info({})


class TestAPI(object):
    def test_validate_schema_without_args(self):
        api = API("name", "desc", [])
        obj = {"command": "name", "id": ""}

        assert validate(obj, api.schema) is None

        api.validate_call()

        with pytest.raises(BadArguments):
            api.validate_call(1, 2, 3, k=5)

    def test_validate_schema_with_args(self):
        api = API("name", "desc", [
            ArgParameter("a1", "d1", str),
            KeywordParameter("a2", "d2", int),
            ArgParameter("a3", "d3", bool),
        ])

        api.validate_call("a1", False, a2=5)

        with pytest.raises(BadArguments):
            api.validate_call()

        with pytest.raises(BadArguments):
            api.validate_call("a", True, {1: 2}, a4=5)

    def test_info(self):
        api = API("name", "desc", [
            KeywordParameter("a2", "d2", int),
            ArgParameter("a1", "d1", str),
            KeywordParameter("a3", "d3", bool),
        ])

        assert api.info == {
            "name": "name",
            "description": "desc",
            "args": [x.info for x in api.args],
            "kwargs": {p.name: p.info for p in api.kwargs},
            "request_schema": {
                "additionalProperties": False,
                "properties": {
                    "args": {
                        "items": [{"type": "string"}],
                        "maxItems": 1,
                        "minItems": 1,
                        "type": "array"
                    },
                    "command": {"enum": ["name"]},
                    "id": {"type": "string"},
                    "kwargs": {
                        "properties": {
                            "a2": {"type": "number"},
                            "a3": {"type": "boolean"}
                        },
                        "required": ["a2", "a3"],
                        "type": "object"
                    }
                },
                "required": ["command", "id", "args", "kwargs"],
                "type": "object"
            },
            "response_schema": {}
        }

    def test_validate_call(self):
        api = API("name", "desc", [
            KeywordParameter("a2", "d2", int),
            ArgParameter("a1", "d1", str),
            KeywordParameter("a3", "d3", bool),
        ])

        obj = api.validate_call("str", a2=5, a3=False)
        obj.pop("id")
        assert obj == {
            "command": "name",
            "args": ["str"],
            "kwargs": {"a2": 5, "a3": False}
        }

    def test_api_reconstruct(self):
        api = API("name", "desc", [
            KeywordParameter("a2", "d2", int),
            ArgParameter("a1", "d1", str),
            KeywordParameter("a3", "d3", bool),
        ])

        assert API.from_info(api.info).info == api.info

    def test_api_reconstruct_without_args(self):
        api = API("name", "desc", [])
        assert API.from_info(api.info).info == api.info

    def test_api_bad_reconstruct(self):
        with pytest.raises(BadArguments):
            API.from_info({})

    def test_validate_schema_with_callable(self):
        count = 0
        def schema():
            return [
                Exactly({"h": [1, 2, "hi", {"a": "b"}]}),
                OneOf({"a": "b"}, {"c": "d"}, [1, 2]),
                JsonSchema({"type": "string"}),
                ListOf(Exactly(1)),
                Type(int),
            ][count]

        api = API("name", "desc", [
            ArgParameter("a1", "d1", schema)
        ])

        with pytest.raises(BadArguments):
            api.validate_call({"h": [1, 2, "hi", {"a": "c"}]})
        api.validate_call({"h": [1, 2, "hi", {"a": "b"}]})

        count = 1
        with pytest.raises(BadArguments):
            api.validate_call({"h": [1, 2, "hi", {"a": "b"}]})
        api.validate_call({"a": "b"})
        api.validate_call({"c": "d"})
        api.validate_call([1, 2])

        count = 2
        with pytest.raises(BadArguments):
          api.validate_call([1, 2])
        api.validate_call("test")

        count = 3
        with pytest.raises(BadArguments):
            api.validate_call("test")
        api.validate_call([1])
        api.validate_call([1]*10)

        count = 4
        with pytest.raises(BadArguments):
            api.validate_call([1])
        api.validate_call(5)
        api.validate_call(6)
