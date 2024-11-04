import psycopg2
import logging

def update_coinnumber():
    try:
        # Connect to your PostgreSQL database
        connection = psycopg2.connect(
            user="postgres",
            password="Harsha508",      # Replace with your actual password
            host="harshacry.c3cca44au3xf.ap-south-1.rds.amazonaws.com",
            port="5432",
            database="HarshaCry"
        )

        cursor = connection.cursor()
        # Specify the sfid and the new values
        sfid = '1234'
        new_margin3count = 10
        new_margin5count = 10
        new_margin10count = 10
        new_margin20count = 0
        new_amount = 5

        # Get the number of records before the update
        cursor.execute("SELECT COUNT(*) FROM Coinnumber;")
        record_count_before = cursor.fetchone()[0]
        print(f"Number of records before update: {record_count_before}")

        # Execute the UPDATE statement with a WHERE clause
        update_query = '''
        UPDATE Coinnumber
        SET margin3count = %s,
            margin5count = %s,
            margin10count = %s,
            margin20count = %s,
            amount = %s
        WHERE sfid = %s;
        '''
        cursor.execute(update_query, (
            new_margin3count,
            new_margin5count,
            new_margin10count,
            new_margin20count,
            new_amount,
            sfid
        ))
        connection.commit()

        # Get the number of records updated
        records_updated = cursor.rowcount
        print(f"Coinnumber table updated successfully. Number of records updated: {records_updated}")

        # Get the number of records after the update
        cursor.execute("SELECT COUNT(*) FROM Coinnumber;")
        record_count_after = cursor.fetchone()[0]
        print(f"Number of records after update: {record_count_after}")

        # Check if the record was found and updated
        if records_updated == 0:
            print(f"No record found with sfid = '{sfid}'.")
            # Optionally, insert the record if it doesn't exist
            insert_query = '''
            INSERT INTO Coinnumber (sfid, margin3count, margin5count, margin10count, margin20count, amount)
            VALUES (%s, %s, %s, %s, %s, %s);
            '''
            cursor.execute(insert_query, (
                sfid,
                new_margin3count,
                new_margin5count,
                new_margin10count,
                new_margin20count,
                new_amount
            ))
            connection.commit()
            print(f"Record with sfid = '{sfid}' inserted successfully.")

            cursor.execute("SELECT COUNT(*) FROM Coinnumber;")
            record_count_after = cursor.fetchone()[0]
            print(f"Number of records after update: {record_count_after}")
    except (Exception, psycopg2.Error) as error:
        print("Error while updating PostgreSQL table:", error)
    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection is closed.")

if __name__ == "__main__":
    update_coinnumber()
