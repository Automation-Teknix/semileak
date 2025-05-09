import mysql.connector
from mysql.connector import Error
import logging
import time
import requests
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_communication.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class DatabaseCommunication:
    def __init__(self):
        """Establishes a connection to the MySQL database and checks connectivity."""
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "leakapp"
        }
        self.connection = None
        self.cursor = None
        self.previous_prodstatus = None
        self.public_ip = self.get_public_ip()
        self.connect_db()

    def connect_db(self):
        """Connects to the database and initializes the cursor."""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True  # Enable autocommit to reflect changes immediately
            logging.info("Database connection established successfully!")
        except Error as e:
            logging.error(f"Database connection failed: {e}")
            self.connection = None
            self.cursor = None

    def reconnect(self):
        """Reconnects to the database in case of failure."""
        logging.info("Attempting to reconnect to the database...")
        self.close_connection()
        self.connect_db()

    def close_connection(self):
        """Closes the database connection properly."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed.")

    def check_connection(self):
        """Checks if the cursor and connection are valid."""
        if self.connection is None or self.cursor is None:
            logging.warning("No database connection or cursor. Attempting to reconnect...")
            self.reconnect()

    def fetch_prodstatus(self):
        """Fetches the prodstatus value from myplclog table."""
        try:
            self.check_connection()  # Ensure the connection is valid
            query = "SELECT prodstatus FROM myplclog LIMIT 1"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                current_prodstatus = result[0]
                logging.info(f"Fetched current prodstatus: {current_prodstatus}")
                return current_prodstatus
            else:
                logging.error("Failed to fetch prodstatus.")
                return None
        except Error as e:
            logging.error(f"Error fetching prodstatus: {e}")
            return None
        
    def delete_previous_records(self):
        """Deletes previous records from foi_tbl."""
        try:
            self.check_connection()
            delete_query = "DELETE FROM foi_tbl"
            self.cursor.execute(delete_query)
            self.connection.commit()
            logging.info("🗑️ Previous records deleted from foi_tbl after prodstatus transition from 0 to 1.")
        except Error as e:
            logging.error(f"Error deleting records: {e}")

    def fetch_max_filter_values(self):
        """Fetches the max value for each FilterNo (AI1 to AI16) from foi_tbl."""
        filter_values = {}
        try:
            self.check_connection()
            for i in range(1, 17):
                filter_no = f"AI{i}"
                query = f"SELECT MAX(Filter_values) FROM foi_tbl WHERE filter_no = %s"
                self.cursor.execute(query, (filter_no,))
                result = self.cursor.fetchone()
                value = result[0] if result and result[0] is not None else 0
                filter_values[filter_no] = value
                logging.info(f"Max value for {filter_no}: {value}")
            return filter_values
        except Error as e:
            logging.error(f"Error fetching filter values: {e}")
            return {}

    def get_public_ip(self):
        """Fetches the public IP address of the machine."""
        try:
            response = requests.get("https://api.ipify.org?format=json", timeout=5)
            if response.status_code == 200:
                ip = response.json().get("ip")
                logging.info(f"Fetched public IP: {ip}")
                return ip
            else:
                logging.warning("Failed to fetch public IP, status code:", response.status_code)
                return "UNKNOWN_IP"
        except Exception as e:
            logging.error(f"Error fetching public IP: {e}")
            return "UNKNOWN_IP"

    def send_post_request(self, data):
        """Sends a POST request with the collected data."""
        try:
            post_data = {
                'apikey': self.public_ip,
                'field1': "LEAK_APP",
                'field2': "NA",
                'field3': "AUTO LEAK TESTING",
                'test': data
            }

            print(post_data)

            url = "https://irouteinspapi.fleetguard-filtrum.com/ThirdPartyAPI/Update"
            headers = {"Content-Type": "application/json"}
            response = requests.post(url, data=json.dumps(post_data), headers=headers)

            if response.status_code == 200:
                logging.info("Data successfully posted to the external service.")
                print("Response from httpbin:", response.json())
            else:
                logging.error(f"Failed to post data. HTTP Status Code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error while sending POST request: {e}")

if __name__ == "__main__":
    db = DatabaseCommunication()
    
    while True:
        try:
            # Fetch the current prodstatus
            current_prodstatus = db.fetch_prodstatus()

            if current_prodstatus is not None:
                if db.previous_prodstatus is None:
                    db.previous_prodstatus = current_prodstatus
                    logging.info(f"First iteration: Set previous_prodstatus to {current_prodstatus}")

                # Case 1: From 1 to 0 transition, send data
                if db.previous_prodstatus == 1 and current_prodstatus == 0:
                    logging.info("Transition from prodstatus 1 to 0 detected. Sending data...")
                    filter_values = db.fetch_max_filter_values()
                    if filter_values:
                        data = {f"AI{i}": filter_values.get(f"AI{i}") if filter_values.get(f"AI{i}") is not None else 0 for i in range(1, 17)}
                        db.send_post_request(data)
                        db.delete_previous_records()
                    else:
                        logging.warning("No valid filter values found.")
                
                # Case 2: No data sent if the transition is from 2 to 0
                elif db.previous_prodstatus == 2 and current_prodstatus == 0:
                    logging.info("Transition from prodstatus 2 to 0 detected. Skipping data posting.")

                # Update the previous_prodstatus only if current_prodstatus has changed
                if current_prodstatus != db.previous_prodstatus:
                    db.previous_prodstatus = current_prodstatus
                    logging.info(f"Updated previous_prodstatus to {db.previous_prodstatus}")
            else:
                logging.warning("Could not fetch prodstatus. Skipping this cycle.")
            time.sleep(0.1)
        except Exception as e:
            logging.error(f"An error occurred during the main loop: {e}")
            time.sleep(0.1)