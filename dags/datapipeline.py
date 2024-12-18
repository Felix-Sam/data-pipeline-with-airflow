import requests
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(dag_id='Agricutural_Chatbot',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def fetch_data():
        url = 'https://official-joke-api.appspot.com/random_joke'
        try:
            response = requests.get(url)  # Use GET instead of POST for this API
            response.raise_for_status()
            data = response.json()
            print(data)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the data: {e}")
            data = {}
        return data  # Returning the data to pass it to the next task

    @task()
    def transform_data(data):
        if 'setup' in data and 'punchline' in data:
            data['word_count'] = len((data['setup'] + data['punchline']).replace(" ", ""))
        else:
            data['word_count'] = 0  # Handle unexpected data structure
        print(f"Transformed Data: {data}")
        return data  # You can store or further process this data

    # Define task dependencies
    data = fetch_data()
    transform_data(data)
