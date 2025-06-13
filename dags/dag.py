from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

CITIES = {
    'Lviv': (49.8397, 24.0297),
    'Kyiv': (50.4501, 30.5234),
    'Kharkiv': (49.9935, 36.2304),
    'Odesa': (46.4825, 30.7233),
    'Zhmerynka': (49.0395, 28.0417),
}

def check_api():
    key = Variable.get("OPENWEATHER_API_KEY")
    resp = requests.get(
        "https://api.openweathermap.org/data/3.0/onecall",
        params={'lat': 49.8397, 'lon': 24.0297, 'appid': key, 'exclude': 'minutely,hourly,daily,alerts'}
    )
    resp.raise_for_status()

def fetch_weather(city, lat, lon, **ctx):
    key = Variable.get("OPENWEATHER_API_KEY")
    exec_date = ctx['ds']
    dt = int(datetime.fromisoformat(exec_date).timestamp())

    resp = requests.get(
        "https://api.openweathermap.org/data/3.0/onecall/timemachine",
        params={
            'lat': lat,
            'lon': lon,
            'dt': dt,
            'appid': key,
        }
    )
    resp.raise_for_status()
    result = resp.json()
    records = result.get('data', [])

    if not records:
        raise ValueError(f"No historical weather data returned for {city} on {exec_date}")

    # Use the first record
    record = records[0]

    return {
        'city': city,
        'execution_date': exec_date,
        'temp': record['temp'],
        'humidity': record['humidity'],
        'clouds': record['clouds'],
        'wind_speed': record['wind_speed'],
    }


def store_to_pg(**ctx):
    records = ctx['ti'].xcom_pull(key=None, task_ids=[f"fetch_{city}" for city in CITIES.keys()])
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute("""
                    INSERT INTO weather(city, date, temperature, humidity, cloudiness, wind_speed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city, date) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        cloudiness = EXCLUDED.cloudiness,
                        wind_speed = EXCLUDED.wind_speed;
                """, (
                    r['city'], r['execution_date'], r['temp'],
                    r['humidity'], r['clouds'], r['wind_speed']
                ))
        conn.commit()

with DAG(
    dag_id="historical_weather_openweathermap",
    start_date=datetime(2023, 3, 16),
    schedule="@daily",
    catchup=True,
    max_active_runs=3,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT,
            date DATE,
            temperature DOUBLE PRECISION,
            humidity INT,
            cloudiness INT,
            wind_speed DOUBLE PRECISION,
            PRIMARY KEY(city, date)
        );
        """
    )

    check_api_alive = PythonOperator(task_id="check_api_alive", python_callable=check_api)

    fetch_tasks = []
    for city, (lat, lon) in CITIES.items():
        task = PythonOperator(
            task_id=f"fetch_{city}",
            python_callable=fetch_weather,
            op_kwargs={'city': city, 'lat': lat, 'lon': lon},
            do_xcom_push=True
        )
        fetch_tasks.append(task)

    store = PythonOperator(task_id="store_to_pg", python_callable=store_to_pg)

    # DAG dependencies
    create_table >> check_api_alive >> fetch_tasks >> store
