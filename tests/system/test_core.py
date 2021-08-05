import pytest
import functools
import os.path

import pandas

import pandas_amazon_redshift

from tests.utils.df_handler import transform_df


@pytest.fixture()
def writer_under_test(config):
    redshift_config = config["Redshift"]

    return functools.partial(
        pandas_amazon_redshift.to_redshift,
        cluster_identifier=redshift_config["ClusterIdentifier"],
        database=redshift_config["TestDatabase"],
        secret_arn=redshift_config["SecretArn"],
    )


@pytest.fixture()
def reader_under_test(config):
    redshift_config = config["Redshift"]

    return functools.partial(
        pandas_amazon_redshift.read_redshift,
        cluster_identifier=redshift_config["ClusterIdentifier"],
        database=redshift_config["TestDatabase"],
        secret_arn=redshift_config["SecretArn"],
    )


@pytest.mark.usefixtures("prepare_sample_data", "prepare_database")
class TestSampleDataIntegration(object):
    def test_to_redshift(self, config, writer_under_test):
        sample_data_config = config["SampleData"]
        data_dir = sample_data_config["Dir"]

        table_config_dict = sample_data_config["Tables"]
        for table_name, table_config in table_config_dict.items():
            file_name = table_config["File"]
            full_path = os.path.join(data_dir, file_name)

            df = pandas.read_csv(
                full_path,
                names=[elm[0] for elm in table_config["Dtype"]],
                **table_config["AdditionalParams"]
            )

            writer_under_test(
                df,
                table=table_name,
                dtype={elm[0]: elm[1] for elm in table_config["Dtype"]},
            )

    def test_read_redshift(self, config, reader_under_test):
        sample_data_config = config["SampleData"]
        queries = sample_data_config["Queries"]
        for query in queries:
            sql = query["Sql"]

            expected_df_config = query["ExpectedDf"]
            expected_df = pandas.DataFrame(expected_df_config["Data"])
            expected_df = transform_df(expected_df, expected_df_config)

            actual_df = reader_under_test(sql)

            pandas.testing.assert_frame_equal(
                actual_df,
                expected_df,
                check_exact=True,
            )


@pytest.mark.usefixtures("prepare_database")
class TestEmptyDataIntegration(object):
    @pytest.fixture()
    def input_df(self, config):
        empty_data_config = config["EmptyData"]
        input_df_config = empty_data_config["InputDf"]
        input_df = pandas.DataFrame(input_df_config["Data"])
        return transform_df(input_df, input_df_config)

    def test_to_redshift(self, config, input_df, writer_under_test):
        empty_data_config = config["EmptyData"]

        writer_under_test(input_df, **empty_data_config["WriterArguments"])

    def test_read_redshift(self, config, input_df, reader_under_test):
        empty_data_config = config["EmptyData"]
        output_df_config = empty_data_config["OutputDf"]

        output_df = reader_under_test(empty_data_config["Sql"])
        output_df = transform_df(output_df, output_df_config)

        pandas.testing.assert_frame_equal(
            output_df,
            input_df,
            check_exact=True,
        )
