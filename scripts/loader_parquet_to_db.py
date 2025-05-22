####
## Script to load cleaned Parquet data into Postgres raw schema
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from sqlalchemy import create_engine, text
from utils.logging_utils import logger

import os
import pandas as pd
import s3fs
import argparse

class ParquetLoader:
    def __init__(self, table: str, exec_date: str):
        """
        Initialize loader with table name and execution date.

        Args:
            table (str): Target table in schema 'raw'.
            exec_date (str): Date string in 'YYYY-MM-DD'.

        Returns:
            None
        """
        self.table     = table
        self.exec_date = exec_date
        # Parse date for partition path and deletion filter
        self.year, self.month, self.day = exec_date.split('-')

        # Setup MinIO filesystem
        self.fs = s3fs.S3FileSystem(
            key             = os.getenv('MINIO_ROOT_USER'),
            secret          = os.getenv('MINIO_ROOT_PASSWORD'),
            client_kwargs   = {
                'endpoint_url': f"http://{os.getenv('MINIO_ENDPOINT')}"
            }
        )

        # Postgres connection
        user        = os.getenv('POSTGRES_USER')
        password    = os.getenv('POSTGRES_PASSWORD')
        host        = os.getenv('POSTGRES_HOST')
        port        = os.getenv('POSTGRES_PORT')
        dbname      = os.getenv('POSTGRES_DB')
        dsn         = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(dsn)
        logger.info(f"Initialized ParquetLoader for {self.table} @ {self.exec_date}")

    def load(self):
        """
        Load Parquet file from staging bucket into Postgres raw.<table>.

        Returns:
            None
        """
        # Build S3 path for Parquet
        path = (
            f"staging/{self.table}/"
            f"{self.year}/{self.month}/{self.day}/"
            f"{self.table}_{self.day}.parquet"
        )
        logger.info(f"Reading Parquet from {path}")

        # Read cleaned Parquet
        df = pd.read_parquet(path, filesystem=self.fs)
        logger.info(f"Loaded {len(df)} rows into DataFrame")

        # Write into Postgres: idempotent partition overwrite
        with self.engine.begin() as conn:
            # Delete existing partition for exec_date
            delete_sql = text(
                f"DELETE FROM raw.{self.table} WHERE date = :d"
            )
            conn.execute(delete_sql, {'d': self.exec_date})
            logger.info(f"Deleted existing rows for {self.exec_date}")

            # Insert new data
            df.to_sql(
                name=self.table,
                con=conn,
                schema='raw',
                if_exists='append',
                index=False
            )
            logger.info(f"Inserted {len(df)} rows into raw.{self.table}")

def main():
    """
    CLI entry point: parse arguments and trigger load.

    Args:
        --table (str): Table name (current, location, forecast).
        --exec_date (str): Date in 'YYYY-MM-DD' for partition.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Load cleaned Parquet to Postgres raw schema'
    )
    parser.add_argument(
        '--table', required=True,
        help='Target table in raw schema'
    )
    parser.add_argument(
        '--exec_date', required=True,
        help='Execution date in YYYY-MM-DD'
    )
    args = parser.parse_args()

    try:
        loader = ParquetLoader(args.table, args.exec_date)
        loader.load()
    except Exception as e:
        logger.error(f"Failed to load Parquet for {args.table} @ {args.exec_date}: {e}")
        raise

if __name__ == '__main__':
    main()
