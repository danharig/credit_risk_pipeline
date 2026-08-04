import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "s3a://credit-risk-data/processed/"
GOLD_PATH = "s3a://credit-risk-data/gold/loan_risk_summary/"

SPARK_ENV = os.environ.get("SPARK_ENV", "cluster")

S3_ENDPOINT = (
    "http://localhost:4566"
    if SPARK_ENV == "local"
    else "http://localstack:4566"
)

S3A_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def build_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("CreditRiskGoldLayer")

    if SPARK_ENV == "local":
        builder = (
            builder.master("local[*]")
            .config("spark.jars.packages", S3A_PACKAGES)
        )

    return (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def build_gold_summary(df):
    return (
        df.groupBy(
            "loan_grade",
            "risk_bucket",
            "loan_intent",
        )
        .agg(
            F.count("*").alias("total_loans"),
            F.sum("loan_status").alias("defaulted_loans"),
            F.round(
                F.avg("loan_status") * 100,
                2,
            ).alias("default_rate_pct"),
            F.round(
                F.avg("loan_amnt"),
                2,
            ).alias("avg_loan_amount"),
            F.round(
                F.sum("loan_amnt"),
                2,
            ).alias("total_loan_amount"),
            F.round(
                F.avg("loan_int_rate"),
                2,
            ).alias("avg_interest_rate"),
            F.round(
                F.avg("person_income"),
                2,
            ).alias("avg_person_income"),
            F.round(
                F.avg("loan_percent_income"),
                4,
            ).alias("avg_loan_percent_income"),
        )
        .withColumn(
            "paid_loans",
            F.col("total_loans") - F.col("defaulted_loans"),
        )
        .withColumn(
            "high_default_flag",
            F.when(
                F.col("default_rate_pct") >= 25,
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .select(
            "loan_grade",
            "risk_bucket",
            "loan_intent",
            "total_loans",
            "paid_loans",
            "defaulted_loans",
            "default_rate_pct",
            "avg_loan_amount",
            "total_loan_amount",
            "avg_interest_rate",
            "avg_person_income",
            "avg_loan_percent_income",
            "high_default_flag",
        )
        .orderBy(
            F.desc("default_rate_pct"),
            F.desc("total_loans"),
        )
    )


def main():
    print("Gold-layer job started", flush=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created", flush=True)

    silver_df = spark.read.parquet(SILVER_PATH)

    print(
        f"Silver row count: {silver_df.count()}",
        flush=True,
    )

    gold_df = build_gold_summary(silver_df)

    print(
        f"Gold summary row count: {gold_df.count()}",
        flush=True,
    )

    gold_df.show(
        30,
        truncate=False,
    )

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("loan_grade")
        .parquet(GOLD_PATH)
    )

    print(
        f"Wrote gold summary to {GOLD_PATH}",
        flush=True,
    )

    spark.stop()


if __name__ == "__main__":
    main()