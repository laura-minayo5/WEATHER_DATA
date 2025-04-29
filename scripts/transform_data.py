# importing sqlalchemy neccessary for establishing connection with postgresql db using a defined engine object
from sqlalchemy import create_engine 

from pyspark.sql.functions import col, to_date, to_timestamp, round, dense_rank
from pyspark.sql.window import Window
import logging

# Import the necessary libraries for sys path
import os, sys

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

# Function to convert temperature in kelvin to fahrenheit
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit
# function to convert speed from m/s to km/h
def m_s_to_km_h(speed_in_m_s):
    speed_in_km_h = speed_in_m_s * 3.6
    return speed_in_km_h
# function to transform weather data using Pyspark
def transform_weather_data(spark, raw_data):
    df = spark.createDataFrame(raw_data)
    
    df = df.withColumn("datetime", to_timestamp(col("datetime")))
    df = df.withColumn("date", to_date(col("datetime")))
    df = df.withColumn("time", to_timestamp(col("datetime")))
    df = df.withColumn("temperature", kelvin_to_fahrenheit(col("temperature"))) 
    df = df.withColumn("temp_feels_like", kelvin_to_fahrenheit(col("feels_like_farenheit")))
    df = df.withColumn("wind_speed", round(m_s_to_km_h(col("wind_speed")), 2))
    # Replace 0 for null for all integer columns
    df.fillna(value = 0)

    # creating a window
    # partition of dataframe
    window = Window.partitionBy("latitude").orderBy("city_name", "country")
    df = df.withColumn("city_id", dense_rank().over(window))

    
    dimension_columns = ['date', 'time', 'city_id', 'temperature', 'humidity', 'pressure', 'wind_speed']
    dimension_df = df.select(dimension_columns)
    
    master_columns = ['city_id', 'city_name', 'country', 'latitude', 'longitude']
    master_df = df.select(master_columns).dropDuplicates()
    
    return dimension_df, master_df

if __name__ == "__main__": # running this python file as a  standalone script
    from pyspark.sql import SparkSession
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
    # creating spark session
    spark = SparkSession.builder.appName("transform_weather_data").getOrCreate()

    test_data = [
        {
            'city_name': 'Nairobi',
            'country': 'KE',
            'latitude': -1.2833,
            'longitude': 36.8167,
            'temperature': 294.73,
            'feels_like_farenheit': 294.67,
            'humidity': 66,
            'pressure': 1010,
            'wind_speed': 3.95,
            'datetime': '2025-04-14T10:00:00+03:00'
            
        }
    ]
    # calling the transform_weather_data function and testing it on our test_data
    dimension_df, master_df = transform_weather_data(spark, test_data)
    print(f"Major DataFrame:")
    dimension_df.show()
    print(f"\nDimension DataFrame:")
    master_df.show()
   

    spark.stop()
