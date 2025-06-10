####
## Script to load cleaned Parquet data into Postgres raw schema
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.logging_utils import logger

import os
import pandas as pd
import argparse

class ParquetLoader:
    def __init__(self, table: str, minio_creds: dict, postgres_creds: dict, exec_date: str):
        """
        Initialize loader with table name and execution date.

        Args:
            table (str): Target table in schema 'raw'.
            minio_creds: MinIO credentials.
            postgres_creds: Postgres credentials.
            exec_date (str): Date string in 'YYYY-MM-DD'.

        Returns:
            None
        """
        # Load helper
        self.helper    = ETLHelper()

        # MinIO filesystem
        self.fs        = self.helper.get_minio_s3fs(minio_creds)
        self.bucket    = minio_creds["MINIO_BUCKET_STAGING"]

        # Postgres connection
        self.engine    = self.helper.create_postgre_conn()

        self.table     = table
        self.exec_date = exec_date

        # Postgres connection
        logger.info(f"Initialized ParquetLoader for {self.table} @ {self.exec_date}")

    def load(self):
        """
        Load Parquet file from staging bucket into Postgres raw.<table>.

        Returns:
            None
        """
        # Read Parquet from S3
        logger.info(f"Reading Parquet from {self.bucket}, table {self.table}, exec date {self.exec_date}")
        df = self.helper.read_parquet_from_s3(self.bucket, self.table, self.exec_date, self.fs)

################## TO DO 20250610
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
