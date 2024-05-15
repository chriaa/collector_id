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
        self.host = host
        self.database = database
        self.user = user
        self.password = password

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
            print(f"Errorjkkm, connecting to MySQL database: {e}")
            self.connection = None

    def fetch_collectors(self):
        """
        Fetch collectors' names from the database.
        """
        if not self.connection:
            print("Not connected to database.")
            return []

        query = """
 SELECT
    #CONCAT(a.FirstName, ' ', COALESCE(a.MiddleInitial, ''), ' ', a.LastName) AS FullName,
    a.FirstName as FirstName,
    a.MiddleInitial as MiddleInitial,
    a.LastName as LastName,
    a.Title,
    a.AgentID
FROM
    casbotany.agent a
LEFT JOIN
    casbotany.collector col ON a.AgentID = col.AgentID
WHERE
     a.FirstName IS NOT NULL AND TRIM(a.FirstName) <> ''
GROUP BY
    a.FirstName,
    a.MiddleInitial,
    a.LastName,
    a.Title,
    a.AgentID
LIMIT 20;


        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            records = cursor.fetchall()
            names = []
            return records
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
