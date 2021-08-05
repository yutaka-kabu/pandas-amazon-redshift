# -*- coding: utf-8 -*-

import pytest

from unittest.mock import Mock

from .utils import load_data


def deep_equals(obj1, obj2):
    if type(obj1) is dict:
        if type(obj2) is not dict:
            return False
        if len(obj1) != len(obj2):
            return False
        for key in obj1:
            if key not in obj2:
                return False
            if not deep_equals(obj1[key], obj2[key]):
                return False
        return True

    if type(obj1) is list:
        if type(obj2) is not list:
            return False
        if len(obj1) != len(obj2):
            return False
        for index in range(len(obj1)):
            if not deep_equals(obj1[index], obj2[index]):
                return False
        return True

    return obj1 == obj2


def test_mock_client_constructor_should_fail(monkeypatch):
    from boto3_mock import MockClient

    with pytest.raises(
        NotImplementedError, match="Cannot initialize via Constructor"
    ):
        MockClient("redshift-data")


def test_mock_client_should_be_singleton(monkeypatch, mock_boto3_client):
    from boto3_mock import MockClient

    client1 = MockClient.get_instance("rds")
    client2 = MockClient.get_instance("rds")

    assert id(client1) == id(client2)

    import boto3

    client3 = boto3.client("rds")

    assert id(client1) == id(client3)


@pytest.mark.parametrize(
    "service_name, operation_name, expected_schema_path",
    [
        (
            "kinesis",
            "DescribeStreamSummary",
            "kinesis-describe-stream-summary-output-schema.json",
        ),
    ],
)
def test_mock_client_schemas(
    monkeypatch, service_name, operation_name, expected_schema_path
):
    from boto3_mock import MockClient

    expected_schema = load_data(expected_schema_path)

    mock_client = MockClient.get_instance(service_name)
    actual_schema = mock_client.schemas[operation_name]

    assert deep_equals(actual_schema, expected_schema)


def test_mock_client_without_setting_request_dict_should_fail(
    monkeypatch, mock_boto3_client
):
    import boto3

    client = boto3.client("ec2")

    with pytest.raises(
        NotImplementedError,
        match=r"The mock for create_vpc\(\) has not been set.",
    ):
        client.create_vpc(CidrBlock="10.0.0.0/16")


@pytest.mark.parametrize(
    "service_name, operation_name, request_params, response_dict_path",
    [
        (
            "ec2",
            "CreateVpc",
            {"CidrBlock": "10.0.0.0/16"},
            "ec2-create-vpc-response.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response.json",
        ),
    ],
)
def test_mock_client_return_response_dict(
    monkeypatch,
    mock_boto3_client,
    service_name,
    operation_name,
    request_params,
    response_dict_path,
):
    import boto3
    from botocore import xform_name

    response_dict = load_data(response_dict_path)

    client = boto3.client(service_name)
    mock = Mock(return_value=response_dict)
    client.set_mock(operation_name, mock)

    py_op_name = xform_name(operation_name)
    method_to_call = getattr(client, py_op_name)

    actual_response = method_to_call(**request_params)

    assert deep_equals(actual_response, response_dict)


def test_mock_client_return_valid_datetime(monkeypatch, mock_boto3_client):
    import boto3

    response_dict = load_data("kinesis-describe-stream-summary-response.json")

    client = boto3.client("kinesis")
    mock = Mock(return_value=response_dict)
    client.set_mock("DescribeStreamSummary", mock)

    actual_response = client.describe_stream_summary(StreamName="test")

    stream_description_summary = actual_response["StreamDescriptionSummary"]
    stream_creation_timestamp = stream_description_summary[
        "StreamCreationTimestamp"
    ]
    assert stream_creation_timestamp.year == 2021
    assert stream_creation_timestamp.month == 6
    assert stream_creation_timestamp.day == 17
    assert stream_creation_timestamp.hour == 7
    assert stream_creation_timestamp.minute == 17
    assert stream_creation_timestamp.second == 29
    assert stream_creation_timestamp.microsecond == 123456
    from dateutil.tz import tzlocal

    assert stream_creation_timestamp.tzinfo == tzlocal()


def test_mock_client_unexpected_methods_called(monkeypatch, mock_boto3_client):
    import boto3

    response_dict = load_data("ec2-create-vpc-response.json")

    client = boto3.client("ec2")
    mock = Mock(return_value=response_dict)
    client.set_mock("CreateVpn", mock)

    with pytest.raises(AttributeError):
        client.create_vpn(CidrBlock="10.0.0.0/16")


def test_mock_client_w_invalid_param_should_fail(
    monkeypatch, mock_boto3_client
):
    import boto3
    from botocore.exceptions import ParamValidationError

    response_dict = load_data("ec2-create-vpc-response.json")

    client = boto3.client("ec2")
    mock = Mock(return_value=response_dict)
    client.set_mock("CreateVpc", mock)

    with pytest.raises(ParamValidationError):
        client.create_vpc(Cidr="10.0.0.0/16")


def test_mock_client_should_fail_unless_using_keyword_args(
    monkeypatch, mock_boto3_client
):
    import boto3

    response_dict = load_data("ec2-create-vpc-response.json")

    client = boto3.client("ec2")
    mock = Mock(return_value=response_dict)
    client.set_mock("CreateVpc", mock)

    with pytest.raises(
        TypeError, match=r"create_vpc\(\) only accepts keyword arguments"
    ):
        client.create_vpc("10.0.0.0/16")


@pytest.mark.parametrize(
    "service_name, operation_name, request_params, response_dict_path",
    [
        (
            "ec2",
            "CreateVpc",
            {"CidrBlock": "10.0.0.0/16"},
            "ec2-create-vpc-response-w-redundant-properties.json",
        ),
        (
            "ec2",
            "CreateVpc",
            {"CidrBlock": "10.0.0.0/16"},
            "ec2-create-vpc-response-w-insufficient-properties.json",
        ),
        (
            "ec2",
            "CreateVpc",
            {"CidrBlock": "10.0.0.0/16"},
            "ec2-create-vpc-response-out-of-enum.json",
        ),
        (
            "ec2",
            "CreateVpc",
            {"CidrBlock": "10.0.0.0/16"},
            "ec2-create-vpc-response-w-invalid-value-type.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-short-array.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-long-array.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-small-int.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-large-int.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-short-str.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-too-long-str.json",
        ),
        (
            "kinesis",
            "DescribeStreamSummary",
            {"StreamName": "test"},
            "kinesis-describe-stream-summary-response-w-unexpected-str.json",
        ),
    ],
)
def test_mock_client_w_invalid_response_dict_should_fail(
    monkeypatch,
    mock_boto3_client,
    service_name,
    operation_name,
    request_params,
    response_dict_path,
):
    import boto3
    from botocore import xform_name
    from jsonschema.exceptions import ValidationError

    response_dict = load_data(response_dict_path)

    client = boto3.client(service_name)
    mock = Mock(return_value=response_dict)
    client.set_mock(operation_name, mock)

    with pytest.raises(ValidationError):
        py_op_name = xform_name(operation_name)
        method_to_call = getattr(client, py_op_name)
        method_to_call(**request_params)
