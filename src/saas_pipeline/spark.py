"""Spark session construction for local and Databricks execution."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from omegaconf import DictConfig


def build_spark(config: DictConfig) -> SparkSession:
    """Build a Delta-enabled Spark session using Databricks-compatible settings."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))
    local_hadoop = Path(config.project_root) / ".hadoop"
    if os.name == "nt" and (local_hadoop / "bin" / "winutils.exe").is_file():
        os.environ.setdefault("HADOOP_HOME", str(local_hadoop.resolve()))
        hadoop_bin = str((local_hadoop / "bin").resolve())
        if hadoop_bin not in os.environ.get("PATH", "").split(os.pathsep):
            os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    on_databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    builder = (
        SparkSession.builder.appName(f"{config.app.name}-{config.environment}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if not on_databricks and config.spark.master:
        builder = builder.master(config.spark.master)
    local_jars = Path(config.project_root) / ".spark-jars"
    jar_paths = sorted(local_jars.glob("*.jar")) if os.name == "nt" else []
    if on_databricks:
        spark = builder.getOrCreate()
    elif jar_paths:
        classpath = os.pathsep.join(str(path.resolve()) for path in jar_paths)
        builder = builder.config("spark.driver.extraClassPath", classpath).config(
            "spark.executor.extraClassPath", classpath
        )
        spark = builder.getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(config.spark.log_level)
    return spark
