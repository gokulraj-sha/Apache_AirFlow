from airflow.decorators import dag, task
from datetime import datetime 

@dag(schedule="*/2 * * * *",
     start_date=datetime(2026,11,2),
     catchup = False)


def etl_demo():

    @task
    def extract():
        return [10,20,40,30]

    @task
    def transform(data):
        return [x*2 for x in data]

    @task
    def load(data):
        with open("etl_output.txt", "a") as file:
            file.write(f"{datetime.now()}:{data}\n")

    load(transform(extract()))


etl_demo()