
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Import the necessary libraries
import os, sys
from dotenv import load_dotenv
load_dotenv()

# sys.path.append('c:/users/admin/weather_data/venv/lib/site-packages')

# initializing default arguments to be used in our dag object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# define PostgreSQL database connection credentials and details 

db_config = {
    'db_name': os.getenv('db_name'),
    'db_user': os.getenv('db_user'),
    'db_password': os.getenv('db_password'),
    # Use the service name from docker-compose as the hostname
    'db_host': os.getenv('db_host'),
    'port': os.getenv('port')
}
# importing our three modules for extracting, transforming and loading data
from scripts.extract_data import extract_openweather_data
from scripts.transform_data import transform_weather_data
from scripts.load_data import load_data_to_postgresql


# connecting to hive metastore database by adding JDBC driver JAR file and configuring spark
def create_spark_session():
        return SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.jars", "//C/Users/Admin/weather_data/postgresql-42.7.5.jar") \
        .config("spark.driver.extraClassPath", "//C/Users/Admin/weather_data/postgresql-42.7.5.jar") \
        .config("spark.executor.extraClassPath", "//C/Users/Admin/weather_data/postgresql-42.7.5.jar") \
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:postgresql://{db_config['db_host']}:{db_config['port']}/{db_config['db_name']}") \
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", db_config['db_user']) \
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", db_config['db_password']) \
        .master("local[*]") \
        .getOrCreate()
# function for the ETL process
def etl_process():
    # need to import for session creation 
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["spark_python"] = os.getenv('SPARK_HOME') + "\\python"
    os.environ["py4j"] = os.getenv('SPARK_HOME') + "\\python\\lib\\py4j-0.10.9.7-src.zip"
    # Retrieve the values from the environment variables
    spark_python_path = os.environ["spark_python"]
    py4j_zip_path = os.environ["py4j"]
    # Add the paths to sys.path
    for path in [spark_python_path, py4j_zip_path]:
        if path not in sys.path:
            sys.path.append(path)
    # Verify that the paths have been added to sys.path
    print("sys.path:", sys.path)

    spark = None
    try:
        spark = create_spark_session()
        raw_data = extract_openweather_data()
        if not raw_data:
            raise ValueError("No data fetched from the API")
        dimension_df, master_df = transform_weather_data(spark, raw_data)
       

        load_data_to_postgresql(dimension_df, master_df)
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


# Creating DAG Object
dag = DAG(
    dag_id ='weather_data_etl',
    default_args = default_args, # refers to the default_args dictionary
    description = 'A DAG for weather data ETL process using PySpark',
    start_date = datetime(2025, 4, 25), # datetime() class requires three parameters to create a date: year, month, day
    schedule_interval = timedelta(days=1), # takes cron(* * * * * ) or timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0) values
    catchup = False, # tells scheduler not to backfill all missed DAG runs between the current date and the start date when the DAG is unpaused.
)
# Python operator for the etl task
etl_task = PythonOperator(
        task_id='weather_etl_process',
        python_callable= etl_process,
        dag = dag,
)

etl_task
