# Copyright 2019. PyData Development Team
# Distributed under BSD 3-Clause License.
# See LICENSE.txt for details.

"""Simple upload example."""

import argparse

import sys

sys.path.append("..")


def main(
    cluster_identifier,
    database,
    table,
    secret_arn,
):
    # [START pandas_redshift_to_redshift_simple]
    import pandas
    import pandas_redshift

    # TODO: Set cluster_identifier to specify your Redshift cluster.
    # cluster_identifier = "my-cluster"

    # TODO: Set the name of database to store your data.
    # database = "my_database"

    # TODO: Set the name of table to store your data.
    # table = "my_table"

    # TODO: Set database user name or secret ARN to access the database above.
    #       In this sample, we use ``secret_arn``.
    # secret_arn = """
    # arn:aws:secretsmanager:<aws-region>:<aws-account-id>:secret:<secret-name>
    # """

    df = pandas.DataFrame(
        {
            "my_string": ["a", "b", "c"],
            "my_int64": [1, 2, 3],
            "my_float64": [4.0, 5.0, 6.0],
            "my_bool1": [True, False, True],
            "my_bool2": [False, True, False],
            "my_dates": pandas.date_range("now", periods=3),
        }
    )

    dtype = {
        "my_string": "CHAR",
        "my_int64": "BIGINT",
        "my_float64": "DOUBLE PRECISION",
        "my_bool1": "BOOLEAN",
        "my_bool2": "BOOLEAN",
        "my_dates": "DATE",
    }

    pandas_redshift.to_redshift(
        df, cluster_identifier, database, table, dtype, secret_arn=secret_arn
    )
    # [END pandas_redshift_to_redshift_simple]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-identifier")
    parser.add_argument("--database")
    parser.add_argument("--secret-arn")
    parser.add_argument("--table")
    args = parser.parse_args()
    main(**vars(args))
