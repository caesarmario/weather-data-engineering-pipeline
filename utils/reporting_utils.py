####
## Utility file for reporting purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import psycopg2

from utils.logging_utils import logger

# Functions for reporting purposes
def get_weather_alerts_data(exec_date, postgres_creds: dict):
    """
    Query weather alert data from PostgreSQL based on a specific date.

    Parameters:
        exec_date (str): Execution date in 'YYYY-MM-DD' format.
        postgres_creds (dict): Dictionary containing credentials.

    Returns:
        List[dict]: List of weather alert records, each as a dictionary.
    """
    try:
        conn = create_postgre_conn(postgres_creds)
        with conn.cursor() as cursor:
            query = """
                SELECT location_id, date, maxtemp_c, totalprecip_mm, maxwind_kph, alert_extreme_heat, alert_storm
                FROM dwh.fact_weather_alerts
                WHERE date >= %s
                ORDER BY location_id, date
            """

            cursor.execute(query, (exec_date,))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()
            
            return result
        
    except Exception as e:
        logger.error(f"Failed to fetch weather alerts for {exec_date}: {str(e)}")
        raise


def create_postgre_conn(postgres_creds):
    """
    Initialize and return a pyscopg2 engine for Postgres connections.

    Args:
        postgres_creds (dict): Dictionary containing connection parameters:

    Returns:
        psycopg2 conn: A psycopg2 connection connected to the specified Postgres database.
    """
    try:
        user           = postgres_creds["POSTGRES_USER"]
        password       = postgres_creds["POSTGRES_PASSWORD"]
        host           = postgres_creds["POSTGRES_HOST"]
        port           = postgres_creds["POSTGRES_PORT"]
        dbname         = postgres_creds["POSTGRES_DB"]

        conn = psycopg2.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            logger.info("Successfully connected to PostgreSQL database.")
        return conn

    except Exception as e:
        logger.error(f"!! Creating postgres connection failed: {e}")
        raise