-- ##############################################
-- SQL Initialization Script for PostgreSQL
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

-- Create database for Apache Airflow
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec

-- Create database for Metabase
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase') \gexec
