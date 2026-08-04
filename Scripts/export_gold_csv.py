import os

from pyspark.sql import SparkSession


SPARK_ENV = os.environ.get("SPARK_ENV", "cluster")

S3_ENDPOINT = (
    "http://localhost:4566"
    if SPARK_ENV == "local"
    else "http://localstack:4566"
)

GOLD_PATH = "s3a://data/loan/"
EXPORT_PATH = "/opt/project/Gold"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ExportGold")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
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


def main() -> None:
    print("Starting Gold CSV export...", flush=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading Gold data from {GOLD_PATH}", flush=True)

    gold_df = spark.read.parquet(GOLD_PATH)

    print(f"Gold row count: {gold_df.count()}", flush=True)

    gold_df.show(20, truncate=False)

    (
        gold_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(EXPORT_PATH)
    )

    print(f"Gold CSV written to {EXPORT_PATH}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()