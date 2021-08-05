# Copyright 2021. Yutaka Kabutoya
# Distributed under BSD 3-Clause License.
# See LICENSE.txt for details.

"""Simple query example."""

import argparse

import sys

sys.path.append("..")


def main(
    cluster_identifier,
    database,
    db_user,
):
    # [START pandas_amazon_redshift_read_redshift_simple]
    import pandas_amazon_redshift

    # TODO: Set cluster_identifier to specify your Redshift cluster.
    # cluster_identifier = "my-cluster"

    # TODO: Set the name of database where your data is stored.
    # database = "my_database"

    # TODO: Set database user name or secret ARN to access the database above.
    #       In this sample, we use ``db_user``.
    # db_user = "my_db_user"

    sql = """
    SELECT sum(qtysold)
    FROM   sales, date
    WHERE  sales.dateid = date.dateid
    AND    caldate = '2008-01-05';
    """
    df = pandas_amazon_redshift.read_redshift(
        sql, cluster_identifier, database, db_user=db_user
    )
    # [END pandas_amazon_redshift_read_redshift_simple]
    print(df)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-identifier")
    parser.add_argument("--database")
    parser.add_argument("--db-user")
    args = parser.parse_args()
    main(**vars(args))
