from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ecommerce-behavior-analysis") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.7")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    spark.stop()