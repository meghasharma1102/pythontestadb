from fastapi import FastAPI
import os

app = FastAPI()


@app.get("/")
def root():
    return {"message": "FastAPI app is running"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-test")
def db_test():
    try:
        from databricks import sql

        host = os.getenv("DATABRICKS_HOST")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        token = os.getenv("DATABRICKS_TOKEN")

        if not host or not http_path or not token:
            return {
                "status": "failed",
                "error": "Missing one or more app settings: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN"
            }

        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()

        return {
            "status": "success",
            "message": "Databricks connection successful",
            "result": row[0] if row else None
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }