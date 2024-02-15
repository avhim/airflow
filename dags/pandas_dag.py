import datetime
import pandas as pd

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


path_to_csv = Variable.get("path_to_csv", deserialize_json=True)
row_number = Variable.get("row_number", default_var=0)

with DAG(
    dag_id = "pandas_read_csv_and_print_to_console",
    start_date = datetime.datetime(2024, 2, 15),
    schedule = datetime.timedelta(minutes=1) #@hourly
):
    def read_from_csv(path: str) -> pd.DataFrame:
        csv_df = pd.read_csv(path)
        return csv_df

    def print_first_row(df: pd.DataFrame, row_number: int) -> None:
        print(df.iloc[row_number])


    task_read_csv = PythonOperator(task_id="pandas_read_csv", python_callable=read_from_csv, op_args=[path_to_csv["variable"]])
    task_print_to_console = PythonOperator(task_id="print_to_console", python_callable=print_first_row, op_args=[read_from_csv, row_number])

task_read_csv>>task_print_to_console
