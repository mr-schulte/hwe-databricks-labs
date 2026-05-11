"""Shared pytest fixtures for notebook SQL tests."""

import os
import re
import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from tests.notebook_utils import get_all_sql_cells, strip_identity

# Paths to DDL notebooks (relative to repo root)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BRONZE_DDL = os.path.join(_REPO_ROOT, "labs", "week4", "create_bronze.ipynb")
_SILVER_DDL = os.path.join(_REPO_ROOT, "labs", "week5", "create_silver.ipynb")
_GOLD_DDL = os.path.join(_REPO_ROOT, "labs", "week6", "create_gold.ipynb")


def _is_only_comments(sql):
    """Return True if the SQL contains no executable statements (only comments/whitespace)."""
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return False
    return True


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    """Session-scoped SparkSession with Delta Lake configured."""
    warehouse_dir = str(tmp_path_factory.mktemp("warehouse"))
    derby_dir = str(tmp_path_factory.mktemp("derby"))

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("notebook-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session
    session.stop()


@pytest.fixture()
def spark(spark_session):
    """SparkSession with bronze/silver/gold schemas and tables.

    Extracts DDL from the create_*.ipynb notebooks and runs it. For gold
    tables, strips GENERATED ALWAYS AS IDENTITY so they work in local Spark.
    Cells that contain only comments (TODO placeholders) are skipped.
    Tables are torn down after each test.
    """
    # Create schemas
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS gold")

    # Run DDL from each notebook
    for ddl_path, needs_strip in [
        (_BRONZE_DDL, False),
        (_SILVER_DDL, False),
        (_GOLD_DDL, True),
    ]:
        for sql in get_all_sql_cells(ddl_path):
            sql = sql.strip()
            sql_upper = " ".join(sql.upper().split())
            sql_upper_no_comments = " ".join(
                line for line in sql_upper.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not sql or sql_upper_no_comments.startswith("CREATE SCHEMA") or "USE CATALOG" in sql_upper or _is_only_comments(sql):
                continue
            if needs_strip:
                sql = strip_identity(sql)
            if "CREATE TABLE" in sql_upper and "USING" not in sql_upper:
                sql_clean = re.sub(r"(\s*--[^\n]*)?\s*;?\s*$", "", sql).rstrip()
                sql = re.sub(r"\)\s*$", ") USING DELTA", sql_clean)
            spark_session.sql(sql)

    yield spark_session

    # Tear down
    spark_session.sql("DROP SCHEMA IF EXISTS gold CASCADE")
    spark_session.sql("DROP SCHEMA IF EXISTS silver CASCADE")
    spark_session.sql("DROP SCHEMA IF EXISTS bronze CASCADE")
