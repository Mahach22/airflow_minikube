from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def generate_pascal_triangle(rows):
    triangle = []
    for i in range(rows):
        row = [None for _ in range(i + 1)]
        row[0], row[-1] = 1, 1
        for j in range(1, len(row) - 1):
            row[j] = triangle[i - 1][j - 1] + triangle[i - 1][j]
        triangle.append(row)
    return triangle

def print_pascal_triangle(triangle):
    for row in triangle:
        print(" ".join(str(num) for num in row).center(len(triangle[-1]) * 2))

def generate_and_print_pascal_triangle():
    pascal_triangle = generate_pascal_triangle(10)
    print_pascal_triangle(pascal_triangle)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

dag = DAG(
    'pascal',
    default_args=default_args,
    schedule_interval='44 11 * * *')

run_pascal_triangle = PythonOperator(
    task_id='run_pascal_triangle',
    python_callable=generate_and_print_pascal_triangle,
    dag=dag
)