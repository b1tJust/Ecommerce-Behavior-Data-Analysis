import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "2019-Oct.csv")
POSTGRES_HOST = "postgres" if os.environ.get("RUNNING_IN_DOCKER") else "localhost"

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/mydb"
JDBC_PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver",
}