import psycopg2
from psycopg2 import Error
import psycopg2
from psycopg2 import Error
  
conn = psycopg2.connect(
   database="postgres", user='postgres', password='Harsha508', host='prahacrypto08.cf0e8ug6ynu6.ap-south-1.rds.amazonaws.com', port= '5432'
)
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Preparing query to create a database  
sql = '''CREATE database crypto''';

#Creating a database
cursor.execute(sql)
print("Database created successfully........")

#Closing the connection
conn.close()