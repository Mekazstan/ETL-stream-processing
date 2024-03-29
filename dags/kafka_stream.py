import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG Arguments        
default_args = {
    'owner': 'mekazstan',
    'start_date': dt.datetime(2024, 1, 24, 10, 00),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Function to fetch the data
def get_data():
    """
    This function fetches the data & returns the JSON response object.
    """
    import requests
    
    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    
    return response


def format_data(response):
    """
    Here the json data being fetched is pre-processed to collect only the necessary fields.
    It returns the formatted data.
    """
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} " \
                    f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']
    
    return data
    

# Function to stream the data & produce data
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    
    # Getting the data
    response = get_data()
    
    # Formatting the response
    response = format_data(response)
    # print(json.dumps(response, indent=3))
    
    # Initializing the Producer class
    # [Using bootstrap_servers=['localhost:9092'] because Kafka broker is running on the same machine where the Docker container is located.]
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    # [Using bootstrap_servers=['broker:9092'] producer finds the Kafka broker running on the same Docker network.]
    # producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
    
    producer.send('users_created', json.dumps(response).encode('utf-8'))
    
    

with DAG('user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api', 
        python_callable=stream_data)
    
    
# Order Sequence

print(stream_data())