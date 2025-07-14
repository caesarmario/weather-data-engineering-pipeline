-- ##############################################
-- SQL Initialization Script for PostgreSQL
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

-- Create database for Apache Airflow
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_database WHERE datname = 'airflow'
    ) THEN
        CREATE DATABASE airflow;
        RAISE NOTICE '✅ Database "airflow" created.';
    ELSE
        RAISE NOTICE 'ℹ️ Database "airflow" already exists. Skipping...';
    END IF;
END
$$;

-- Create database for Metabase
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_database WHERE datname = 'metabase'
    ) THEN
        CREATE DATABASE metabase;
        RAISE NOTICE '✅ Database "metabase" created.';
    ELSE
        RAISE NOTICE 'ℹ️ Database "metabase" already exists. Skipping...';
    END IF;
END
$$;
