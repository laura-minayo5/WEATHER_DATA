from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import logging
import os, sys
from dotenv import load_dotenv
# Description: Importing modules from sub directory

load_dotenv()

# load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)



# define PostgreSQL database connection credentials and details 
db_config = {
    'db_name': os.getenv('db_name'),
    'db_user': os.getenv('db_user'),
    'db_password': os.getenv('db_password'),
    # Use the service name from docker-compose as the hostname
    'db_host': os.getenv('db_host'),
    'port': os.getenv('port')
}
# Creating a database configuration file using jdbc driver
# db.url=jdbc:postgresql://localhost:5432/salesdb.username=Yourusernamedb.password=YourPassword
JDBC_URL = f"jdbc:postgresql://{db_config['db_host']}:{db_config['port']}/{db_config['db_name']}"



# establishing a connection with the postgresql database using sqlalchemy by defining conn. engine
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
engine = create_engine(f"postgresql+psycopg2://{db_config['db_user']}:{db_config['db_password']}@{db_config['db_host']}/{db_config['db_name']}")

# function for loading our data into our db created in postgresql server
def load_data_to_postgresql(dimension_df, master_df):
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option
    # data is being written to an already created table cities and measurements under the schema waether_data
    logger.info("Starting data load process")
    try:
        dimension_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "weather_data.cities") \
            .option("user", db_config["db_user"] ) \
            .option("password", db_config["db_password"]) \
            .mode("overwrite") \
            .save()
        logger.info("cities data loaded successfully")
    

        master_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "weather_data.measurements") \
            .option("user", db_config["db_user"]) \
            .option("password", db_config["db_password"]) \
            .mode("append") \
            .save()
        logger.info("weather measurements data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

    logger.info("Data loading process completed")



if __name__ == "__main__":
      
      load_data_to_postgresql()
      



      

    




    