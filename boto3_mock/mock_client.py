import copy

import jsonschema
from jsonschema import ValidationError

from datetime import datetime
from dateutil.tz import tzlocal

from botocore import xform_name
from botocore.model import ServiceModel
from botocore.serialize import create_serializer

import botocore.session

botocore_session = botocore.session.get_session()
botocore_loader = botocore_session.get_component("data_loader")


def load_service_description(service_name):

    service_description = botocore_loader.load_service_model(
        service_name, "service-2", api_version=None
    )

    return service_description


class MockClient(object):

    __instances = {}

    # To deal with datetime objects in boto3 response with string
    _TIMESTAMP_PATTERN = (
        "^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
        "[0-9]{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{4}"
        " [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}$"
    )
    _TIMESTAMP_DESCRIPTION = "timestamp"

    _TYPE_MAP = {
        "blob": "string",
        "boolean": "boolean",
        "double": "number",
        "float": "number",
        "integer": "integer",
        "list": "array",
        "long": "integer",
        "string": "string",
        "structure": "object",
        "timestamp": "string",
    }

    _RESPONSE_METADATA_SCHEMA = {
        "type": "object",
        "required": [
            "RequestId",
            "HTTPStatusCode",
            "HTTPHeaders",
            "RetryAttempts",
        ],
        "properties": {
            "RequestId": {"type": "string"},
            "HTTPStatusCode": {"type": "integer"},
            "HTTPHeaders": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "RetryAttempts": {"type": "integer"},
        },
        "additionalProperties": True,
    }

    def _add_response_metadata_schema(self, operation_name, schema):
        schema_properties = schema["properties"]
        schema_properties["ResponseMetadata"] = self._RESPONSE_METADATA_SCHEMA

    def _shape_to_schema(self, shape, shapes):

        shape_info = shapes[shape]
        shape_type = shape_info["type"]

        schema = {"type": MockClient._TYPE_MAP[shape_type]}

        if shape_type == "list":
            member = shape_info["member"]
            member_shape = member["shape"]
            if "min" in shape_info:
                schema["minItems"] = shape_info["min"]
            if "max" in shape_info:
                schema["maxItems"] = shape_info["max"]

            schema["items"] = self._shape_to_schema(member_shape, shapes)

            return schema

        if shape_type == "structure":
            if "required" in shape_info:
                schema["required"] = shape_info["required"]
            properties = schema.setdefault("properties", {})
            for member_name, member_info in shape_info["members"].items():
                member_shape = member_info["shape"]
                properties[member_name] = self._shape_to_schema(
                    member_shape, shapes
                )
            if "additionalProperties" not in schema:
                schema["additionalProperties"] = False
            return schema

        if "enum" in shape_info:
            schema["enum"] = shape_info["enum"]

        if "min" in shape_info:
            if shape_type == "string":
                schema["minLength"] = shape_info["min"]
            else:
                schema["minimum"] = shape_info["min"]
        if "max" in shape_info:
            if shape_type == "string":
                schema["maxLength"] = shape_info["max"]
            else:
                schema["maximum"] = shape_info["max"]

        if "pattern" in shape_info:
            schema["pattern"] = shape_info["pattern"]

        if shape_type == "timestamp":
            schema["pattern"] = self._TIMESTAMP_PATTERN
            schema["description"] = self._TIMESTAMP_DESCRIPTION

        return schema

    def _extract_schema(self, operation_name, shape, shapes):
        schema = self._shape_to_schema(shape, shapes)

        return schema

    def _extract_schemas(self, service_description):
        schemas = {}

        operations = service_description["operations"]
        shapes = service_description["shapes"]
        for operation_name, operation_info in operations.items():
            if "output" not in operation_info:
                continue

            output_info = operation_info["output"]
            schemas[operation_name] = self._extract_schema(
                operation_name, output_info["shape"], shapes
            )
            self._add_response_metadata_schema(
                operation_name, schemas[operation_name]
            )

        return schemas

    @classmethod
    def get_instance(cls, service_name):
        if service_name not in cls.__instances:
            cls._add_methods(service_name)
            cls.__instances[service_name] = cls.__private_init__(
                super(MockClient, cls).__new__(cls), service_name
            )

        return cls.__instances[service_name]

    def __new__(cls, *args, **kwargs):
        raise NotImplementedError("Cannot initialize via Constructor")

    @classmethod
    def __private_init__(cls, self, service_name):
        self.service_name = service_name

        service_description = load_service_description(service_name)
        service_description_copy = copy.deepcopy(service_description)
        self.schemas = self._extract_schemas(service_description_copy)
        self._mocks = {}

        return self

    def validate_response(self, operation_name, response_dict):
        response_schema = self.schemas[operation_name]
        jsonschema.validate(response_dict, response_schema)

    def set_mock(self, operation_name, mock):
        self._mocks[operation_name] = mock

    @classmethod
    def _add_methods(cls, service_name):
        service_description = load_service_description(service_name)

        service_model = ServiceModel(
            service_description, service_name=service_name
        )
        operations = service_description["operations"]
        for operation_key, operation_info in operations.items():
            operation_name = operation_info["name"]
            method = cls._create_method(operation_name, service_model)
            setattr(cls, method.__name__, method)

    def _convert_response(self, response, schema):
        response_type = schema["type"]

        if response_type == "object":
            properties = schema.get("properties", {})
            for key, item_schema in properties.items():
                if key not in response:
                    continue
                response[key] = self._convert_response(
                    response[key], item_schema
                )
            return response

        if response_type == "array":
            item_schema = schema["items"]

            for index in range(len(response)):
                response[index] = self._convert_response(
                    response[index], item_schema
                )
            return response

        if response_type == "string":
            if schema.get("description", "") == self._TIMESTAMP_DESCRIPTION:
                dt = datetime.strptime(response, "%a, %d %b %Y %H:%M:%S.%f")
                dt = dt.replace(tzinfo=tzlocal())
                return dt

        return response

    @classmethod
    def _create_method(cls, operation_name, service_model):
        method_name = xform_name(operation_name)
        operation_model = service_model.operation_model(operation_name)
        protocol = service_model.metadata["protocol"]
        serializer = create_serializer(protocol)

        def _make_api_call_mock(self, *args, **kwargs):
            if args:
                raise TypeError(
                    "{}() only accepts keyword arguments.".format(method_name)
                )
            api_params = kwargs
            serializer.serialize_to_request(api_params, operation_model)

            mock = self._mocks.get(operation_name)
            if not mock:
                raise NotImplementedError(
                    "The mock for {}() has not been set.".format(method_name)
                )
            try:
                response_dict = mock(*args, **kwargs)
                self.validate_response(operation_name, response_dict)
                schema = self.schemas[operation_name]
                return self._convert_response(response_dict, schema)
            except ValidationError:
                raise

        _make_api_call_mock.__name__ = str(method_name)

        return _make_api_call_mock
