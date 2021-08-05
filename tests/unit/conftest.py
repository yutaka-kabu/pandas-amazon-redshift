import pytest
import functools


@pytest.fixture(autouse=True)
def mock_boto3_client(monkeypatch):
    def mock_client(service_name):
        from boto3_mock import MockClient

        return MockClient.get_instance(service_name)

    monkeypatch.setattr("boto3.client", mock_client)
    return mock_client


@pytest.fixture()
def cluster_identifier():
    return "redshift-cluster-1"


@pytest.fixture()
def database():
    return "test"


@pytest.fixture()
def reader_under_test(cluster_identifier, database):
    import pandas_amazon_redshift

    return functools.partial(
        pandas_amazon_redshift.read_redshift,
        cluster_identifier=cluster_identifier,
        database=database,
    )


@pytest.fixture()
def writer_under_test(cluster_identifier, database):
    import pandas_amazon_redshift

    return functools.partial(
        pandas_amazon_redshift.to_redshift,
        cluster_identifier=cluster_identifier,
        database=database,
    )
