import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


RAW_PATH = "s3a://credit-risk-data/raw/dataset.csv"
PROCESSED_PATH = "s3a://data/processed/"

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
    builder = SparkSession.builder.appName("CreditRiskETL")

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


from pyspark.sql import DataFrame

def clean_and_transform(df: DataFrame) -> DataFrame:
   """
    Cleans raw loan records by:

    - Removing invalid ages
    - Filling missing employment lengths
    - Filling missing interest rates
    - Creating loan status labels
    - Creating risk buckets

    Returns a cleaned Spark DataFrame.
    """
    df = df.filter(
        (F.col("person_age") >= 18)
        & (F.col("person_age") <= 100)
    )

    df = (
        df.withColumn(
            "person_emp_length",
            F.when(
                F.col("person_emp_length").isNull(),
                F.lit(0),
            ).otherwise(F.col("person_emp_length")),
        )
        .filter(F.col("person_emp_length") <= 60)
    )

    avg_rate = df.select(F.avg("loan_int_rate")).first()[0]

    df = df.withColumn(
        "loan_int_rate",
        F.when(
            F.col("loan_int_rate").isNull(),
            F.lit(avg_rate),
        ).otherwise(F.col("loan_int_rate")),
    )

    df = df.withColumn(
        "loan_status_label",
        F.when(
            F.col("loan_status") == 1,
            F.lit("default"),
        ).otherwise(F.lit("paid")),
    )

    df = df.withColumn(
        "risk_bucket",
        F.when(
            F.col("loan_percent_income") < 0.2,
            F.lit("low"),
        )
        .when(
            F.col("loan_percent_income") < 0.4,
            F.lit("medium"),
        )
        .otherwise(F.lit("high")),
    )

    return df


def main():
    print("ETL script started", flush=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created", flush=True)

    raw_df = spark.read.csv(
        RAW_PATH,
        header=True,
        inferSchema=True,
    )

    print(f"Raw row count: {raw_df.count()}", flush=True)

    clean_df = clean_and_transform(raw_df)

    print(f"Cleaned row count: {clean_df.count()}", flush=True)

    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("loan_grade")
    # Partition by loan grade to improve downstream query performance.
        .parquet(PROCESSED_PATH)
    )

    print(
        f"Wrote cleaned data to {PROCESSED_PATH}",
        flush=True,
    )

    spark.stop()


if __name__ == "__main__":
    main()