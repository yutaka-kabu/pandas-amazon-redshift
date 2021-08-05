import pytest

import os
import os.path
import shutil

import wget
import zipfile

from tests.utils import load_json


@pytest.fixture(scope="session")
def base_dir():
    abspath_for_this_script = os.path.abspath(__file__)
    return os.path.dirname(abspath_for_this_script)


@pytest.fixture(scope="session")
def config(base_dir):
    conf = load_json(base_dir, "conftest.json")
    sample_data_config = conf["SampleData"]
    sample_data_config["Dir"] = os.path.join(
        base_dir, sample_data_config["Dir"]
    )
    return conf


@pytest.fixture(scope="class")
def prepare_sample_data(config):
    sample_data_config = config["SampleData"]
    data_dir = sample_data_config["Dir"]
    os.makedirs(data_dir)

    file_name = wget.download(sample_data_config["Url"], out=data_dir)
    file_path = os.path.join(data_dir, file_name)

    with zipfile.ZipFile(file_path) as archive:
        archive.extractall(data_dir)

    yield

    shutil.rmtree(data_dir)


def run_query(conn, sql):
    res = conn.run_query(sql)
    statement_id = res["Id"]
    conn.wait([sql], [statement_id])


@pytest.fixture(scope="class")
def prepare_database(config):
    from pandas_amazon_redshift.core import RedshiftConnector

    redshift_config = config["Redshift"]

    conn = RedshiftConnector(
        cluster_identifier=redshift_config["ClusterIdentifier"],
        database=redshift_config["BaseDatabase"],
        secret_arn=redshift_config["SecretArn"],
    )

    sql = "CREATE DATABASE {}".format(redshift_config["TestDatabase"])
    run_query(conn, sql)

    yield

    sql = "DROP DATABASE {}".format(redshift_config["TestDatabase"])
    run_query(conn, sql)
