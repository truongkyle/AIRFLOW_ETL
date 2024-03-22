from airflow import DAG

from datetime import datetime

from airflow.operators.python import PythonOperator

import random

dag = DAG(
    dag_id='playing_dice',
    start_date= datetime(2024, 3, 19),
    schedule_interval=None
)

def play(ti):
    sum = 0
    for i in range(5):
        for j in range(3):
            sum += random.randint(1,6)

    ti.xcom_push(key='who_play', value=sum)

def result(ti):
    total = sum([val for val in ti.xcom_pull(key='who_play', task_ids=['player_ky', 'player_dang'])])
    return total

anh_Ky_play = PythonOperator(
    task_id = 'player_ky',
    python_callable = play,
    dag = dag
)

anh_Dang_play = PythonOperator(
    task_id = 'player_dang',
    python_callable = play,
    dag = dag
)

ketqua = PythonOperator(
    task_id = 'result',
    python_callable = result,
    dag = dag
)

[anh_Ky_play, anh_Dang_play] >> ketqua
