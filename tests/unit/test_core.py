# -*- coding: utf-8 -*-

import pytest
from unittest.mock import MagicMock

from copy import deepcopy

import pandas

from .utils import load_data
from tests.utils.df_handler import transform_df


def set_list_tables_mock(client):
    list_tables_response = load_data("redshift-data-list-tables-response.json")
    list_tables_mock = MagicMock(return_value=list_tables_response)
    client.set_mock("ListTables", list_tables_mock)

    return list_tables_mock


def set_execute_statement_mock(client, check_kwargs=None):
    # to pass parmas to describe_statement_mock
    info_for_statements = {}

    execute_statement_response_base = load_data(
        "redshift-data-execute-statement-response-base.json"
    )
    execute_statement_mock = MagicMock()

    def execute_statement_side_effect(*args, **kwargs):
        cluster_identifier = kwargs["ClusterIdentifier"]
        database = kwargs["Database"]
        sql = kwargs["Sql"]

        if check_kwargs:
            check_kwargs(kwargs)

        response = deepcopy(execute_statement_response_base)
        response["ClusterIdentifier"] = cluster_identifier
        response["Database"] = database
        response["Id"] = "{}{:0=2}".format(
            response["Id"], execute_statement_mock.call_count
        )

        info_for_statement = info_for_statements.setdefault(response["Id"], {})
        info_for_statement["ClusterIdentifier"] = cluster_identifier
        info_for_statement["Database"] = database
        info_for_statement["Sql"] = sql
        return response

    execute_statement_mock.side_effect = execute_statement_side_effect
    client.set_mock("ExecuteStatement", execute_statement_mock)

    return info_for_statements, execute_statement_mock


def set_describe_statement_mock(client, info_for_statements, **response_diff):
    describe_statement_response_base = load_data(
        "redshift-data-describe-statement-response-base.json"
    )
    describe_statement_mock = MagicMock()

    def describe_statement_side_effect(*args, **kwargs):
        statement_id = kwargs["Id"]

        info_for_statement = info_for_statements[statement_id]
        sql = info_for_statement["Sql"]
        cluster_identifier = info_for_statement["ClusterIdentifier"]

        response = deepcopy(describe_statement_response_base)
        response["Id"] = statement_id
        response["ClusterIdentifier"] = cluster_identifier
        response["QueryString"] = sql
        response.update(response_diff)

        return response

    describe_statement_mock.side_effect = describe_statement_side_effect
    client.set_mock("DescribeStatement", describe_statement_mock)

    return describe_statement_mock


def test_to_redshift_w_no_secret_arn_and_no_db_user_should_fail(
    writer_under_test,
):
    from pandas_amazon_redshift.errors import InvalidAuthentication

    with pytest.raises(InvalidAuthentication):
        writer_under_test(
            pandas.DataFrame([[1]], columns=["col"]),
            table="table",
            dtype={"col": "INTEGER"},
        )


def test_read_redshift_w_no_secret_arn_and_no_db_user_should_fail(
    reader_under_test,
):
    from pandas_amazon_redshift.errors import InvalidAuthentication

    with pytest.raises(InvalidAuthentication):
        reader_under_test("SELECT 1")


@pytest.fixture()
def mock_boto3_client_for_reader(mock_boto3_client):
    client = mock_boto3_client("redshift-data")

    info_for_statements, _ = set_execute_statement_mock(client)

    set_describe_statement_mock(client, info_for_statements)

    return client


@pytest.mark.parametrize(
    "config_path",
    [
        "config-read-redshift-dtype-emptytbl.json",
        "config-read-redshift-dtype-1.json",
        "config-read-redshift-dtype-booltbl.json",
        "config-read-redshift-dtype-floattbl.json",
        "config-read-redshift-dtype-inttbl.json",
        "config-read-redshift-dtype-texttbl.json",
        "config-read-redshift-dtype-misctbl.json",
        "config-read-redshift-dtype-numerictbl.json",
        "config-read-redshift-dtype-datetimetbl.json",
        "config-read-redshift-dtype-supertbl.json",
        "config-read-redshift-next-token.json",
    ],
)
def test_read_redshift_success(
    reader_under_test, mock_boto3_client_for_reader, config_path
):
    config = load_data(config_path)

    def get_statement_result_side_effect(*args, **kwargs):
        response = config["GetStatementResultResponse"]
        next_token = kwargs.get("NextToken", "default")
        return response[next_token]

    get_statement_result_mock = MagicMock(
        side_effect=get_statement_result_side_effect
    )

    mock_boto3_client_for_reader.set_mock(
        "GetStatementResult", get_statement_result_mock
    )

    expected_df_config = config["ExpectedDf"]
    expected_df = pandas.DataFrame(expected_df_config["Data"])
    expected_df = transform_df(expected_df, expected_df_config)

    actual_df = reader_under_test(config["Sql"], db_user="testuser")
    actual_df_config = config.get("ActualDf", {})
    actual_df = transform_df(actual_df, actual_df_config)

    pandas.testing.assert_frame_equal(actual_df, expected_df, check_exact=True)


@pytest.mark.parametrize(
    "mock_args, sql, error_cls, error_msg",
    [
        (
            {
                "Status": "FAILED",
                "Error": 'ERROR: relation "not_existing_tbl" does not exist',
            },
            "SELECT col FROM not_existing_tbl",
            "QueryFailedError",
            r"The following query was failed "
            r"\[ID: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXX01 \(sql: '{}'\)\]\n"
            r"\({}\)",
        ),
        (
            {
                "Status": "ABORTED",
            },
            "SELECT 1",
            "QueryAbortedError",
            r"The following query was stopped by user "
            r"\[ID: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXX01 \(sql: '{}'\)\]",
        ),
    ],
)
def test_read_redshift_fail(
    reader_under_test,
    mock_boto3_client,
    mock_args,
    sql,
    error_cls,
    error_msg,
):
    from pandas_amazon_redshift.errors import QueryFailedError  # noqa
    from pandas_amazon_redshift.errors import QueryAbortedError  # noqa

    client = mock_boto3_client("redshift-data")

    info_for_statements, _ = set_execute_statement_mock(client)

    set_describe_statement_mock(client, info_for_statements, **mock_args)

    format_args = [sql]
    if "Error" in mock_args:
        format_args.append(mock_args["Error"])

    error_msg = error_msg.format(*format_args)

    with pytest.raises(locals()[error_cls], match=error_msg):
        reader_under_test(
            sql,
            secret_arn="arn:aws:secretsmanager:us-east-1:"
            "012345678901:secret:TestSecret-ZZZZZZ",
        )


@pytest.fixture()
def mock_for_writer(mock_boto3_client):
    client = mock_boto3_client("redshift-data")

    set_list_tables_mock(client)

    info_for_statements, execute_statement_mock = set_execute_statement_mock(
        client
    )

    set_describe_statement_mock(client, info_for_statements)

    return execute_statement_mock


def test_to_redshift_if_exists_fail(writer_under_test, mock_for_writer):
    from pandas_amazon_redshift.errors import TableCreationError

    expected_error_msg = (
        "Could not create the table "
        "existing_tbl in the schema public because it already exists."
    )

    with pytest.raises(TableCreationError, match=expected_error_msg):
        writer_under_test(
            pandas.DataFrame([[1]], columns=["col"]),
            table="existing_tbl",
            dtype={"col": "INTEGER"},
            db_user="testuser",
        )


def test_to_redshift_success(
    writer_under_test,
    mock_for_writer,
    cluster_identifier,
    database,
):
    from pandas_amazon_redshift.types import Integer

    writer_under_test(
        pandas.DataFrame([[1, 1]]),
        table="newtbl",
        dtype=Integer(),
        secret_arn="arn:aws:secretsmanager:us-east-1:"
        "012345678901:secret:TestSecret-ZZZZZZ",
    )

    mock_for_writer.assert_any_call(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        SecretArn="arn:aws:secretsmanager:us-east-1:"
        "012345678901:secret:TestSecret-ZZZZZZ",
        Sql='CREATE TABLE "public"."newtbl" ("0" INTEGER,"1" INTEGER);',
    )
    mock_for_writer.assert_any_call(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        SecretArn="arn:aws:secretsmanager:us-east-1:"
        "012345678901:secret:TestSecret-ZZZZZZ",
        Sql='INSERT INTO "public"."newtbl" ("0","1") VALUES (1,1);',
    )


@pytest.mark.parametrize(
    "config_path",
    [
        "config-to-redshift-dtype-newinttbl.json",
        "config-to-redshift-dtype-newfloattbl.json",
        "config-to-redshift-dtype-newbooltbl.json",
        "config-to-redshift-dtype-newtexttbl.json",
        "config-to-redshift-dtype-newdatetimetbl.json",
        "config-to-redshift-dtype-newtimetbl.json",
        "config-to-redshift-dtype-newsupertbl.json",
        "config-to-redshift-dtype-newemptytbl.json",
    ],
)
def test_to_redshift_dtype(
    writer_under_test,
    mock_for_writer,
    cluster_identifier,
    database,
    config_path,
):
    config = load_data(config_path)

    input_df_config = config["InputDf"]
    input_df = pandas.DataFrame(input_df_config["Data"])
    input_df = transform_df(input_df, input_df_config)

    writer_under_test(
        input_df,
        table=config["TableName"],
        dtype=config["ToRedshiftDtype"],
        db_user="testuser",
    )

    mock_for_writer.assert_any_call(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser="testuser",
        Sql=config["ExpectedCreateTblSql"],
    )
    if "ExpectedInsertSql" in config:
        mock_for_writer.assert_any_call(
            ClusterIdentifier=cluster_identifier,
            Database=database,
            DbUser="testuser",
            Sql=config["ExpectedInsertSql"],
        )
    else:
        assert mock_for_writer.call_count == 1


@pytest.mark.parametrize(
    "input_df, to_redshift_dtype, error_cls, error_msg",
    [
        (
            {"a": ["a"]},
            {"a": "INTEGER"},
            "DataEncodeError",
            r"Column 'a': object cannot be converted to an IntegerDtype",
        ),
        (
            {"a": ["い"]},
            {"a": "date"},
            "DataEncodeError",
            r"Column 'a': cannot parse to datetime Unknown string format: い",
        ),
        (
            {"a": ["い"]},
            {"a": "BPCHAR"},
            "DataEncodeError",
            r"Column 'a': multibyte characters must not be included",
        ),
        (
            {"a": ["い1"]},
            {"a": "VARCHAR(3)"},
            "DataEncodeError",
            r"Column 'a': 'い1' exceeds length \(3\)",
        ),
        (
            {"a": [{1}]},
            {"a": "SUPER"},
            "DataEncodeError",
            r"Column 'a': unsupported datatype <class 'set'> for SUPER type",
        ),
        (
            {"a": [2.2250738585072e-308]},
            {"a": "DOUBLE PRECISION"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'DOUBLE PRECISION'",
        ),
        (
            {"a": [-2.2250738585072e-308]},
            {"a": "DOUBLE PRECISION"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'DOUBLE PRECISION'",
        ),
        (
            {"a": [1.797693134862311e308]},
            {"a": "DOUBLE PRECISION"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'DOUBLE PRECISION'",
        ),
        (
            {"a": [-1.797693134862311e308]},
            {"a": "DOUBLE PRECISION"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'DOUBLE PRECISION'",
        ),
        (
            {"a": [1.17549e-38]},
            {"a": "REAL"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'REAL'",
        ),
        (
            {"a": [-1.17549e-38]},
            {"a": "REAL"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'REAL'",
        ),
        (
            {"a": [3.402821e38]},
            {"a": "REAL"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'REAL'",
        ),
        (
            {"a": [-3.402821e38]},
            {"a": "REAL"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'REAL'",
        ),
        (
            {"a": [-2147483649]},
            {"a": "INTEGER"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'INTEGER'",
        ),
        (
            {"a": [2147483648]},
            {"a": "INTEGER"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'INTEGER'",
        ),
        (
            {"a": [-32769]},
            {"a": "SMALLINT"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'SMALLINT'",
        ),
        (
            {"a": [32768]},
            {"a": "SMALLINT"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'SMALLINT'",
        ),
        (
            {"a": [-32769]},
            {"a": "SMALLINT"},
            "DataEncodeError",
            r"Column 'a': '.*?' is out of range for type 'SMALLINT'",
        ),
        (
            {"a": [100]},
            {"a": "NUMERIC(3,1)"},
            "DataEncodeError",
            r"Column 'a': '100\.0' is out of range for type 'NUMERIC\(3,1\)'",
        ),
        (
            {"a": ["2021-07-01 00:00:00"]},
            {"a": "DATETIME"},
            "InvalidRedshiftType",
            r"Redshift type not found for 'DATETIME'",
        ),
        (
            {"a": [[1]]},
            {"a": "ARRAY<INTEGER>"},
            "InvalidRedshiftType",
            r"Redshift type not found for 'ARRAY<INTEGER>'",
        ),
        (
            {"a": ["a"]},
            {"a": "CHAR(4097)"},
            "InvalidRedshiftType",
            r"The length '4097' is too long for 'CHAR'",
        ),
        (
            {"a": ["a"]},
            {"a": "VARCHAR(65536)"},
            "InvalidRedshiftType",
            r"The length '65536' is too long for 'VARCHAR'",
        ),
        (
            {"": [1]},
            {"": "INTEGER"},
            "MetadataEncodeError",
            r"Empty table or column name specified",
        ),
        (
            {"\x00": [1]},
            {"\x00": "INTEGER"},
            "MetadataEncodeError",
            r"Identifier cannot contain NULs",
        ),
        (
            [list(range(10000))],
            "INTEGER",
            "DataEncodeError",
            r"Row\[0\] is beyond size limitation",
        ),
        (
            {"a": [chr(57292)]},
            "VARCHAR(3)",
            "DataEncodeError",
            r"Column 'a': .* is cannot be converted to UTF-8",
        ),
        (
            {chr(57292): ["a"]},
            "VARCHAR(3)",
            "MetadataEncodeError",
            r"Identifier .* is cannot be converted to UTF-8",
        ),
        ({}, {}, "NoColumnError", r"Table must have at least one column"),
    ],
)
def test_to_redshift_data_fail(
    writer_under_test,
    mock_for_writer,
    input_df,
    to_redshift_dtype,
    error_cls,
    error_msg,
):
    from pandas_amazon_redshift.errors import DataEncodeError  # noqa
    from pandas_amazon_redshift.errors import InvalidRedshiftType  # noqa
    from pandas_amazon_redshift.errors import MetadataEncodeError  # noqa
    from pandas_amazon_redshift.errors import NoColumnError  # noqa

    with pytest.raises(locals()[error_cls], match=error_msg):
        writer_under_test(
            pandas.DataFrame(input_df),
            table="data_encode_err_tbl",
            dtype=to_redshift_dtype,
            db_user="testuser",
        )
    mock_for_writer.assert_not_called()


def test_to_redshift_load_chunks(writer_under_test, mock_boto3_client):
    from pandas_amazon_redshift.core import RedshiftConnector

    client = mock_boto3_client("redshift-data")

    set_list_tables_mock(client)

    def check_insert_sql(kwargs):
        sql = kwargs["Sql"]
        sql_len = len(sql.encode("utf-8", "strict"))
        assert sql_len < RedshiftConnector._MAX_STATEMENT_SIZE

    info_for_statements, execute_statement_mock = set_execute_statement_mock(
        client, check_insert_sql
    )

    set_describe_statement_mock(client, info_for_statements)

    writer_under_test(
        pandas.DataFrame({"col": list(range(50000))}),
        table="loadchunktbl",
        dtype={"col": "INTEGER"},
        db_user="testuser",
    )

    assert execute_statement_mock.call_count >= 3
