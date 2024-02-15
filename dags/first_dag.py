import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

var1 = "jnsdjn jsfbgsjvm kk"

# Normal call style
# foo = Variable.get("key_1")

# Auto-deserializes a JSON value
bar = Variable.get("key_1", deserialize_json=True)


with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2024, 2, 10),
    schedule="@once"
):
    def function1():
        print("hello world")

    def function2(string1):
        print(f"hello world {string1}")



    task1 = EmptyOperator(task_id="task")

    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    task4 = EmptyOperator(task_id="task4")

    task5 = PythonOperator(task_id="python_task", python_callable=function1)
    task6 = PythonOperator(task_id="task6", python_callable=function2, op_args=[bar["variable"]])


task2>>(task1, task3, task6)>>task4>>task5