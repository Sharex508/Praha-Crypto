import psycopg2
import requests
try:
    connection = psycopg2.connect(
        user="postgres",  # Your master username
        password="Harsha508",  # The master password for your RDS instance
        host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",  # Your RDS endpoint
        port="5432",  # Default PostgreSQL port
        database="Harshacry"  # Your database name
    )
    print("Connection to RDS PostgreSQL successful")
except Exception as e:
    print(f"Error while connecting to RDS: {e}")
