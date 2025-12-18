import pytest
import pandas as pd
import logging
from datetime import datetime

from pyspark.sql import SparkSession

from src.utils.catalog_interaction import (
    _quote_identifier,
    catalog_exists,
    schema_exists,
    table_exists,
    create_table_permission_check,
    drop_table,
    write_to_table,
    upsert_pandas_df,
    read_table_to_pandas,
    prune_old_rows,
    truncate_table,
    delete_rows_after_timestamp,
    update_table,
    delete_rows_with_null_columns,
    clone_table,
    pd_df_columns,
)

# Default marker: Spark / Delta integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def logger():
    logger = logging.getLogger("pytest_logger")
    logger.setLevel(logging.DEBUG)
    return logger


@pytest.fixture(scope="session")
def test_catalog():
    return "main"


@pytest.fixture(scope="session")
def test_schema(spark, test_catalog):
    schema = "pytest_schema"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{schema}")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.{schema} CASCADE")


@pytest.fixture
def empty_test_table(spark, test_catalog, test_schema, logger):
    table = "pytest_table"
    create_table_permission_check(
        spark,
        test_catalog,
        test_schema,
        table,
        logger,
        columns_pandas_dtypes=pd_df_columns,
    )
    yield table
    drop_table(spark, test_catalog, test_schema, table, logger)


@pytest.fixture
def sample_pandas_df():
    return pd.DataFrame(
        {
            "ESN": [1, 1, 1, 2, 2],
            "reportdatetime": pd.to_datetime([
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
                "2023-01-01",
                "2023-01-02",
            ]),
            "NEW_FLAG": [1, 1, 1, 1, 1],
        }
    )
class TestIdentifierHelpers:
    """Pure Python helpers - no Spark interaction."""

    @pytest.mark.unit
    def test_quote_identifier_simple(self):
        assert _quote_identifier("simple") == "simple"

    @pytest.mark.unit
    def test_quote_identifier_with_space(self):
        assert _quote_identifier("with space") == "`with space`"

    @pytest.mark.unit
    def test_quote_identifier_with_special_chars(self):
        assert _quote_identifier("a-b") == "`a-b`"

    @pytest.mark.unit
    def test_quote_identifier_idempotent(self):
        assert _quote_identifier("`already`") == "`already`"


class TestMetadataChecks:
    """Catalog / schema / table existence checks."""

    @pytest.mark.unit
    def test_catalog_exists(self, spark, test_catalog):
        assert catalog_exists(spark, test_catalog) is True

    @pytest.mark.unit
    def test_schema_exists(self, spark, test_catalog, test_schema):
        assert schema_exists(spark, test_catalog, test_schema) is True

    @pytest.mark.unit
    def test_table_exists_false(self, spark, test_catalog, test_schema):
        assert table_exists(spark, test_catalog, test_schema, "non_existing") is False

class TestTableLifecycle:
    def test_create_and_drop_table(self, spark, test_catalog, test_schema, logger):
        table = "create_drop_test"

        create_table_permission_check(
            spark, test_catalog, test_schema, table, logger
        )
        assert table_exists(spark, test_catalog, test_schema, table)

        drop_table(spark, test_catalog, test_schema, table, logger)
        assert not table_exists(spark, test_catalog, test_schema, table)

class TestWriteAndRead:
    def test_write_to_table(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.count() == len(sample_pandas_df)

    def test_write_empty_dataframe(self, spark, test_catalog, test_schema, empty_test_table, logger):
        write_to_table(
            spark,
            pd.DataFrame(),
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.count() == 0

    def test_read_table_to_pandas(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        pdf = read_table_to_pandas(
            spark,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        assert isinstance(pdf, pd.DataFrame)
        assert len(pdf) == len(sample_pandas_df)

class TestUpsertAndMutation:
    def test_upsert_insert_then_update(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        upsert_pandas_df(
            spark,
            sample_pandas_df,
            test_catalog,
            test_schema,
            empty_test_table,
            logger,
        )

        df2 = sample_pandas_df.copy()
        df2.loc[0, "NEW_FLAG"] = 0

        upsert_pandas_df(
            spark,
            df2,
            test_catalog,
            test_schema,
            empty_test_table,
            logger,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.filter(df.NEW_FLAG == 0).count() == 1

    def test_update_table(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        update_table(
            spark,
            test_catalog,
            test_schema,
            empty_test_table,
            "2023-01-02 00:00:00",
            logger,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.filter(df.NEW_FLAG == 0).count() > 0

class TestPruneAndDelete:
    def test_prune_old_rows(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        prune_old_rows(
            spark,
            test_catalog,
            test_schema,
            empty_test_table,
            n_pts=2,
            logger=logger,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.count() == 4

    def test_delete_rows_after_timestamp(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        delete_rows_after_timestamp(
            spark,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
            "2023-01-02 00:00:00",
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.filter(df.reportdatetime > datetime(2023, 1, 2)).count() == 0

    def test_delete_rows_with_null_columns(self, spark, test_catalog, test_schema, empty_test_table, logger):
        df = pd.DataFrame(
            {
                "ESN": [1, 2],
                "reportdatetime": pd.to_datetime(["2023-01-01", "2023-01-02"]),
                "NEW_FLAG": [1, None],
            }
        )

        write_to_table(
            spark,
            df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        delete_rows_with_null_columns(
            spark,
            "TEST",
            test_catalog,
            test_schema,
            empty_test_table,
            ["NEW_FLAG"],
            logger,
        )

        df_after = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df_after.count() == 1

class TestMaintenanceOperations:
    def test_truncate_table(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        truncate_table(
            spark,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        df = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        assert df.count() == 0

    def test_clone_table(self, spark, test_catalog, test_schema, empty_test_table, sample_pandas_df, logger):
        write_to_table(
            spark,
            sample_pandas_df,
            logger,
            test_catalog,
            test_schema,
            empty_test_table,
        )

        target_table = "pytest_clone"

        clone_table(
            spark,
            test_catalog,
            test_schema,
            empty_test_table,
            test_catalog,
            test_schema,
            target_table,
            logger,
        )

        assert table_exists(spark, test_catalog, test_schema, target_table)

        df_src = spark.table(f"{test_catalog}.{test_schema}.{empty_test_table}")
        df_tgt = spark.table(f"{test_catalog}.{test_schema}.{target_table}")

        assert df_src.count() == df_tgt.count()

        drop_table(spark, test_catalog, test_schema, target_table, logger)
