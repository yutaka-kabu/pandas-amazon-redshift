from __future__ import annotations

import logging
from typing import cast
from time import sleep
import boto3

from collections import OrderedDict

import numpy
import pandas
from pandas import DataFrame
from pandas.core.dtypes.common import is_dict_like

from .utils import to_utf8_bytes
from .types import get_redshift_type, RedshiftType, DtypeArg
from .errors import (
    InvalidAuthentication,
    QueryFailedError,
    QueryAbortedError,
    TableCreationError,
    MetadataEncodeError,
    DataEncodeError,
    NoColumnError,
)

logger = logging.getLogger(__name__)


def to_redshift(
    dataframe,
    cluster_identifier: str,
    database: str,
    table: str,
    dtype: DtypeArg,
    schema: str = "public",
    db_user: str | None = None,
    secret_arn: str | None = None,
    if_exists: str = "fail",
) -> None:
    """Write a DataFrame to a Redshift table.

    The main method a user calls to export pandas DataFrame contents to
    Amazon Redshift table.

    This method uses the RedshiftDataAPIService for Boto 3 to make requests to
    Amazon Redshift, documented `here
    <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Client.execute_statement>`__.

    The authentication method must be adopted from temporary
    credentials (db_user/database) or AWS Secrets Manager (secret_arn).

    Parameters
    ----------
    dataframe : pandas.DataFrame
        DataFrame to be written to a Amazon Redshift table.
    cluster_identifier : str
        The cluster identifier.
    database : str
        The name of database. This parameter is required when authenticating
        using temporary credentials.
    table : str
        Name of table to be written
    dtype: dict or scalar
        Specifying the datatype for columns. If a dictionary is used, the
        keys should be the column names and the values should be the
        Redshift types or equivalent strings. If a scalar is provided,
        it will be applied to all columns.
    schema, default "public"
        Name of schema where the table is written
    db_user : str, optional
        The database user name. This parameter is required when authenticating
        using temporary credentials.
    secret_arn : str, optional
        The name or ARN of the secret that enables access to the database.
        This parameter is required when authenticating using AWS Secrets
        Manager.
    if_exists : str, default 'skip'
        Behavior when the desination table exists. Value can be one of:

        ``'fail'``
            If table exists, abort the process with error.
    """

    connector = RedshiftConnector(
        cluster_identifier, database, db_user=db_user, secret_arn=secret_arn
    )

    if connector.exists_table(table, schema):
        if if_exists == "fail":
            raise TableCreationError(
                "Could not create the table {} in the schema {} "
                "because it already exists.".format(table, schema)
            )

    dtype = connector.prep_dtype(dataframe, dtype)

    data, columns = connector.encode_data(dataframe, dtype, table, schema)

    connector.create_table(columns, dtype, table, schema)

    connector.load_dataframe(data, columns, dtype, table, schema)


def read_redshift(
    select_sql,
    cluster_identifier: str,
    database: str,
    db_user: str | None = None,
    secret_arn: str | None = None,
) -> DataFrame:
    """Load data from Amazon Redshift using Amazon Redshift Data API

    The main method a user calls to execute a SQL in Amazon Redshift
    and read result into a pandas DataFrame.

    This method uses the RedshiftDataAPIService for Boto3 to make requests to
    Amazon Redshift, documented `here
    <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Client.execute_statement>`__.

    The authentication method must be adopted from temporary credentials
    (db_user/database) or AWS Secrets Manager (secret_arn).

    Parameters
    ----------
    sql : str
        SQL to return data values.
    cluster_identifier : str
        The cluster identifier.
    database : str
        The name of database. This parameter is required when authenticating
        using temporary credentials.
    db_user : str, optional
        The database user name. This parameter is required when authenticating
        using temporary credentials.
    secret_arn : str, optional
        The name or ARN of the secret that enables access to the database.
        This parameter is required when authenticating using AWS Secrets
        Manager.

    Returns
    -------
    result_df : pandas.DataFrame
        DataFrame storing data from Amazon Redshift.
    """

    connector = RedshiftConnector(
        cluster_identifier, database, db_user=db_user, secret_arn=secret_arn
    )

    result_df = connector.to_dataframe(select_sql)

    return result_df


class RedshiftConnector(object):
    client = None

    def __new__(cls, *args, **kwargs):
        if not cls.client:
            cls.client = boto3.client("redshift-data")
        return super(RedshiftConnector, cls).__new__(cls)

    def __init__(
        self, cluster_identifier, database, db_user=None, secret_arn=None
    ):
        self.cluster_identifier = cluster_identifier
        self.db_user = db_user
        self.database = database
        self.secret_arn = secret_arn
        if self.secret_arn is None and self.db_user is None:
            raise InvalidAuthentication(
                "Authentication requires db_user or secret_arn."
            )

    # the specific string to identify no token (no more results)
    _NO_NEXT_TOKEN = ""

    def _has_next_token(self, params):
        # The case "NextToken" is not included means the first iteration
        if "NextToken" not in params:
            return True

        if params["NextToken"] == self._NO_NEXT_TOKEN:
            return False

        return True

    def run_query(self, sql):
        execute_statement_params = {
            "ClusterIdentifier": self.cluster_identifier,
            "Database": self.database,
            "Sql": sql,
        }
        if self.secret_arn is not None:
            execute_statement_params["SecretArn"] = self.secret_arn
        else:
            execute_statement_params["DbUser"] = self.db_user

        result = self.client.execute_statement(**execute_statement_params)
        statement_id = result["Id"]

        logger.debug(
            "Statement ID: {} (sql: '{}') started".format(statement_id, sql)
        )

        return result

    def wait(self, sqls, statement_ids):
        for index in range(len(statement_ids)):
            sql = sqls[index]
            statement_id = statement_ids[index]

            status, description = "", {}
            while status not in {"FINISHED", "FAILED", "ABORTED"}:
                description = self.client.describe_statement(Id=statement_id)
                status = description["Status"]
                sleep(1.0)

            if status == "FAILED":
                raise QueryFailedError(
                    "The following query was failed "
                    "[ID: {} (sql: '{}')]\n({})".format(
                        statement_id, sql, description["Error"]
                    )
                )
            elif status == "ABORTED":
                raise QueryAbortedError(
                    "The following query was stopped by user "
                    "[ID: {} (sql: '{}')]".format(statement_id, sql)
                )

            logger.debug(
                "Statement ID: {} (sql: '{}') finished".format(
                    statement_id, sql
                )
            )

    @staticmethod
    def _decode_data(df, redshift_types):
        """Make the DataFrame's column types align with the Redshift table
        column types.
        Need to work around limited NA value support. Floats are always
        fine, ints must always be floats if there are Null values.
        Booleans are hard because converting bool column with None replaces
        all Nones with false. Therefore only convert bool if there are no
        NA values.
        Datetimes should already be converted to np.datetime64 if supported,
        but here we also force conversion if required.
        """

        for col_name, type_name in redshift_types.items():
            col = df[col_name]
            redshift_type = get_redshift_type(type_name)

            df[col_name] = redshift_type.decode(col)

    def to_dataframe(self, select_sql):
        run_result = self.run_query(select_sql)

        statement_id = run_result["Id"]

        self.wait([select_sql], [statement_id])

        # Now status is "FINISHED"
        get_statement_result_params = {"Id": statement_id}
        redshift_types = OrderedDict()
        data = []
        while self._has_next_token(get_statement_result_params):
            statement_result = self.client.get_statement_result(
                **get_statement_result_params
            )

            if "NextToken" not in get_statement_result_params:
                for col in statement_result.get("ColumnMetadata", []):
                    col_name = col["name"]
                    col_type = col["typeName"]
                    redshift_types[col_name] = col_type

            chunk = [
                tuple(
                    [
                        numpy.nan
                        if "isNull" in cell
                        else list(cell.values())[0]
                        for cell in record
                    ]
                )
                for record in statement_result.get("Records", [])
            ]
            data.extend(chunk)

            get_statement_result_params["NextToken"] = statement_result.get(
                "NextToken", self._NO_NEXT_TOKEN
            )

        # to set approprite columns and dtypes for empty table
        # from_records and astype are necessary
        data = pandas.DataFrame.from_records(
            data, columns=list(redshift_types.keys())
        )

        self._decode_data(data, redshift_types)

        return data

    def prep_dtype(self, dataframe, dtype):
        if not is_dict_like(dtype):
            # error: Value expression in dictionary comprehension has
            # incompatible type "Union[str, RedshiftType,
            # Dict[Hashable, Union[str, RedshiftType]]]";
            # expected type "Union[str, RedshiftType]"
            dtype = {column: dtype for column in dataframe}
        else:
            dtype = cast(dict, dtype)

        for column in dataframe:
            my_type = dtype[column]
            if not isinstance(my_type, RedshiftType):
                my_type = get_redshift_type(my_type)
                dtype[column] = my_type

        return dtype

    def create_table(self, columns, dtype, table, schema):
        encode = self._encode_metadata

        col_def = [
            "{} {}".format(encode(column), dtype[column]) for column in columns
        ]
        create_table_sql = "CREATE TABLE {}.{} ({});".format(
            encode(schema), encode(table), ",".join(col_def)
        )
        run_result = self.run_query(create_table_sql)

        statement_id = run_result["Id"]

        self.wait([create_table_sql], [statement_id])

    _MAX_STATEMENT_SIZE = 100000

    def encode_data(self, dataframe, dtype, table, schema):
        encoded_df = dataframe.copy()
        columns = list(encoded_df.columns)

        if len(columns) == 0:
            raise NoColumnError("Table must have at least one column")

        for column in columns:
            my_type = dtype[column]
            try:
                encoded_df[column] = my_type.encode(encoded_df[column])
            except TypeError as err:
                raise DataEncodeError(
                    "Column '{}': {}".format(column, str(err))
                )

        # to efficiently access to rows in dataframe
        data = encoded_df.to_numpy()

        self._check_row_length(data, columns, table, schema)

        return data, columns

    def _encode_metadata(self, name):
        try:
            uname = to_utf8_bytes(str(name)).decode("utf-8")
        except TypeError as err:
            raise MetadataEncodeError("Identifier {}".format(str(err)))

        if not len(uname):
            raise MetadataEncodeError("Empty table or column name specified")

        nul_index = uname.find("\x00")
        if nul_index >= 0:
            raise MetadataEncodeError("Identifier cannot contain NULs")

        return '"{}"'.format(uname.replace('"', '""'))

    def _check_row_length(self, data, columns, table, schema):
        insert_sql_prefix = self._generate_insert_sql_prefix(
            columns, table, schema
        )

        for index in range(len(data)):
            row = data[index]

            insert_sql_value = self._generate_insert_sql_value(row)
            insert_sql = insert_sql_prefix + insert_sql_value
            insert_sql_size = len(to_utf8_bytes(insert_sql))
            if insert_sql_size >= self._MAX_STATEMENT_SIZE:
                raise DataEncodeError(
                    "Row[{}] is beyond size limitation".format(index)
                )

    def _generate_insert_sql_prefix(self, columns, table, schema):
        encode = self._encode_metadata

        insert_sql_prefix = "INSERT INTO {}.{} ({}) VALUES ".format(
            encode(schema),
            encode(table),
            ",".join([encode(column) for column in columns]),
        )

        return insert_sql_prefix

    def _generate_insert_sql_value(self, row):
        return "({})".format(",".join(row))

    def _generate_insert_sqls(self, data, columns, dtype, table, schema):
        insert_sql_prefix = self._generate_insert_sql_prefix(
            columns, table, schema
        )

        remaining_rows = len(data)
        total_rows = remaining_rows

        insert_sql = insert_sql_prefix
        for index in range(total_rows):
            row = data[index]
            insert_sql_value = self._generate_insert_sql_value(row)
            if index == 0:
                insert_sql = insert_sql_prefix + insert_sql_value
                continue
            tmp_insert_sql = insert_sql + "," + insert_sql_value
            statement_size = len(to_utf8_bytes(tmp_insert_sql))
            if statement_size >= self._MAX_STATEMENT_SIZE:
                remaining_rows = max(0, total_rows - index)
                yield remaining_rows, insert_sql + ";"
                insert_sql = insert_sql_prefix + insert_sql_value
            else:
                insert_sql = tmp_insert_sql
        yield 0, insert_sql + ";"

    def _load_chunks(self, data, columns, dtype, table, schema):
        if len(data) == 0:
            return

        insert_sqls = self._generate_insert_sqls(
            data, columns, dtype, table, schema
        )

        statement_ids, run_sqls = [], []
        for remaining_rows, insert_sql in insert_sqls:
            yield remaining_rows

            run_result = self.run_query(insert_sql)

            statement_id = run_result["Id"]
            statement_ids.append(statement_id)
            run_sqls.append(insert_sql)

        self.wait(run_sqls, statement_ids)

    def load_dataframe(self, data, columns, dtype, table, schema="public"):
        total_rows = len(data)
        progress = self._load_chunks(data, columns, dtype, table, schema)
        for remaining_rows in progress:
            logger.info(
                "{} out of {} rows loaded.".format(
                    total_rows - remaining_rows, total_rows
                )
            )

    def exists_table(self, table, schema="public", database=None):
        table_map = self._get_table_map(schema, database)
        return table in table_map

    def _get_table_map(self, schema, database):
        # in this method, it is possible to list tables for another database
        # when listing tables for another table, should specify the parameter
        # database
        list_tables_params = {"ClusterIdentifier": self.cluster_identifier}
        # if `database` param is skipped, database is designated as the same
        # as connected database
        list_tables_params["Database"] = (
            database if database is not None else self.database
        )
        list_tables_params["SchemaPattern"] = schema
        # add authentication info
        if self.secret_arn is not None:
            list_tables_params["SecretArn"] = self.secret_arn
        else:
            list_tables_params["ConnectedDatabase"] = self.database
            list_tables_params["DbUser"] = self.db_user

        table_map = set()
        while self._has_next_token(list_tables_params):
            list_tables_result = self.client.list_tables(**list_tables_params)
            for table_info in list_tables_result["Tables"]:
                table_map.add(table_info["name"])
            list_tables_params["NextToken"] = list_tables_result.get(
                "NextToken", self._NO_NEXT_TOKEN
            )
        return table_map
