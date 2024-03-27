import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

class DBAccess:
    def __init__(self, host, database, user, password):
        """
        Initialize database connection details from environment variables.
        """
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_DATABASE')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')

    def connect(self):
        """
        Establish a database connection.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                print("MySQL database connection successful.")
        except Error as e:
            print(f"Error connecting to MySQL database: {e}")
            self.connection = None

    def fetch_collectors(self):
        """
        Fetch collectors' names from the database.
        """
        if not self.connection:
            print("Not connected to database.")
            return []

        query = """
        SELECT FirstName, LastName, MiddleInitial
        FROM collectors
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            records = cursor.fetchall()
            names = []
            for row in records:
                names.append(f"{row[0].strip()},{row[2].strip()},{row[1].strip()}")

            return names
        except Error as e:
            print(f"Failed to fetch collectors: {e}")
            return []
        finally:
            cursor.close()

    def close(self):
        """
        Close the database connection.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed.")


