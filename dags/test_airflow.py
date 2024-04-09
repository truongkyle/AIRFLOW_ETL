from airflow.decorators import dag, task
from datetime import datetime

# Định nghĩa một hàm khác cùng cấp với example_dag
def helper_function():
    """
    Hàm trợ giúp
    """
    print("Helper function executed")
test_list = [1,2,3,4]
# Khai báo example_dag và helper_function trong cùng không gian tên (namespace)
@dag(schedule_interval=None, 
     start_date=datetime(2022, 1, 1), 
     catchup=False, 
     tags=["example"]
     )

def example_dag():

    @task
    def task1(i):
        """
        Task 1
        """
        
        return (1 +i)

    @task
    def task2(a):
        """
        Task 2
        """
        print("task2 executed")
        return (a +2)

    @task
    def task3(a, b):
        """
        Task 3
        """
        print(f"Task 3 executed{a}, {b}")

    @task
    def task4():
        """
        Task 3
        """
        print("Task 4 executed")
    # @task
    # def task5():
    #     @task
    #     def get_task(a):
    #         return (1+a)
        
    #     @task
    #     def load_task(a):
    #         return (2+a)
        
    #     list_a = [1,1,2,3]
    #     for i in list_a:
    #         b = get_task(i)
    #         d = load_task(b)
    # task5()
    # Gọi helper_function trong example_dag
    # helper_function()
    # Định nghĩa dependency giữa các tasks
    list_result = {}
    for i in test_list:
        a = task1(i)
        b = task2(a)
        list_result[f"{i}"] = b
    [task3(list_result.get("1"), list_result.get("2")), task3(list_result.get("3"), list_result.get("4"))] >> task4()

example = example_dag()
