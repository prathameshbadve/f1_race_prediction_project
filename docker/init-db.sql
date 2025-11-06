-- Create database for MLflow
CREATE DATABASE f1_mlflow;

-- Create database for Dagster
CREATE DATABASE dagster;

-- Grant privileges (optional, since we're using the postgres superuser)
GRANT ALL PRIVILEGES ON DATABASE f1_mlflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE dagster TO postgres;