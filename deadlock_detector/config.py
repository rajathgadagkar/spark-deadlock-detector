"""Configuration constants and SparkContext factory."""

APP_NAME = "SparkDeadlockDetector"
MAX_CYCLE_LENGTH = 25
DEFAULT_TOP_K = 5


def create_spark_context(num_cores: int, app_name: str = APP_NAME):
    """Create and configure a local SparkContext with the specified core count."""
    if num_cores < 1:
        raise ValueError("Number of cores must be a positive integer.")

    from pyspark import SparkConf, SparkContext

    conf = (
        SparkConf()
        .setAppName(app_name)
        .setMaster(f"local[{num_cores}]")
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    return sc
