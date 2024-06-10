import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()


class DBAccess:
    def __init__(self, host, database, user, password, port):
        """
        Initialize database connection details from environment variables.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port

    def connect(self):
        """
        Establish a database connection.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            if self.connection.is_connected():
                print("MySQL database connection successful.")
                return self.connection
        except Error as e:
            print(f"Error, connecting to MySQL database: {e}")
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
            LIMIT 100;
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

    def initialize_target_database(self):

        if not self.connection:
            print("Not connected to database.")
            return []

        try:
            cursor = self.connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS CollectorID;")
            cursor.execute("USE CollectorID;")
            cursor.execute("DROP TABLE IF EXISTS collectors;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collectors (
                    agent_id INT,
                    full_name VARCHAR(255),
                    target_name VARCHAR(255),
                    best_match_source_field VARCHAR(255),
                    orcid_id VARCHAR(255),
                    match_confidence VARCHAR(255),
                    PRIMARY KEY (agent_id,target_name)
                );
            """)
            self.connection.commit()
        except Error as e:
            print(f"Error executing SQL commands: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
            self.connection.close()

def insert_collector_record(record):
    try:
        # Establish a new connection for each record
        conn = mysql.connector.connect(
            host=os.getenv("DB_TARGET_HOST"),
            user=os.getenv("DB_TARGET_USER"),
            password=os.getenv("DB_TARGET_PASSWORD"),
            database='CollectorID',
            port=os.getenv("DB_TARGET_PORT")
        )
        cursor = conn.cursor()
        agent_id, best_match_source_field, full_name, match_confidence, orcid_id, target_name = record
        cursor.execute("""
            INSERT INTO collectors (agent_id, full_name, target_name, best_match_source_field, orcid_id, match_confidence)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (agent_id, full_name, target_name, best_match_source_field, orcid_id, match_confidence))
        conn.commit()
    except Error as e:
        print(f"Error while inserting into MySQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()