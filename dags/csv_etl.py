from airflow.decorators import dag, task
from datetime import datetime 
import csv , os

FOLDER = os.environ.get("AIRFLOW_HOME", ".")

@dag(schedule="*/2 * * * *",
     start_date=datetime(2026,11,2),
     catchup = False)


def csv_etl():

    @task
    def extract():
        with open(f"{FOLDER}/sales.csv") as file:
            return list(csv.DictReader(file))

    @task
    def transform(rows):
        for r in rows:
            r["total"] = int(r["quantity"]) * int(r["price"]) 
        return rows

    @task
    def load(rows):
        with open(f"{FOLDER}/sales_output.txt", "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["name","quantity","price","total"])
            writer.writeheader()
            writer.writerows(rows)


    load(transform(extract()))


csv_etl()