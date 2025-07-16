import socket
import time
import json
import datetime
import mysql.connector
from mysql.connector import Error as MySQLError
import logging
from threading import Thread, RLock as Lock
from typing import List, Dict, Any, Optional, Tuple
import sys

# Configure logging with UTF-8
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose logging
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('iot_driver.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("IoTDatabaseDriver")

class DatabaseConnection:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.lock = Lock()
        self.connect()

    def connect(self) -> None:
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                autocommit=True,
                charset='utf8mb4',
                connection_timeout=30,
                buffered=True
            )
            logger.info("Successfully connected to MySQL database")
            print("[DB] Connected successfully.")
        except MySQLError as e:
            logger.exception(f"Failed to connect to MySQL database: {e}")
            print(f"[DB ERROR] Connection failed: {e}")
            self.connection = None

    def ensure_connection(self) -> None:
        with self.lock:
            try:
                if self.connection is None or not self.connection.is_connected():
                    logger.info("Reconnecting to MySQL database...")
                    print("[DB] Reconnecting...")
                    self.connect()
                else:
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
            except MySQLError as e:
                logger.exception(f"Error checking MySQL connection: {e}")
                print(f"[DB ERROR] Connection check failed: {e}")
                try:
                    if self.connection:
                        self.connection.close()
                except:
                    pass
                self.connection = None
                self.connect()

    def execute_query(self, query: str, params: tuple = None) -> Optional[Any]:
        print("Inside execute_query")
        with self.lock:
            print("Inside lock")
            self.ensure_connection()
            print("After ensure_connection")
            if not self.connection or not self.connection.is_connected():
                logger.error("Failed to execute query: No MySQL database connection")
                print("[DB ERROR] No active database connection.")
                return None
            try:
                print(f"[DB] Preparing to execute: {query} | Params: {params}")
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute(query, params)
                print("[DB] Query executed")
                
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    cursor.close()
                    print("[DB] Fetched rows:", result)
                    return result
                else:
                    affected = cursor.rowcount
                    cursor.close()
                    print("[DB] Query OK. Rows affected:", affected)
                    return affected
            except Exception as e:
                print(" EXCEPTION in execute_query:", e)
                raise  # Force crash to see actual error in thread



    def close(self) -> None:
        with self.lock:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("MySQL connection closed")
                print("[DB] Connection closed.")


class IoTDevice:
    def __init__(self, device_id: str, ip: str, port: int, ai_range: Tuple[int, int]):
        self.device_id = device_id
        self.port = port
        self.ai_start, self.ai_end = ai_range
        self.server_socket = None
        self.client_socket = None
        self.connected = False
        self.lock = Lock()

    def start_server(self) -> None:
        with self.lock:
            if self.server_socket:
                return
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind(("0.0.0.0", self.port))
                self.server_socket.listen(1)
                logger.info(f"Listening for IoT device on port {self.port}")
            except Exception as e:
                logger.error(f"Failed to start server for {self.device_id} on port {self.port}: {e}")
                self.server_socket = None

    def accept_connection(self) -> bool:
        with self.lock:
            if not self.server_socket:
                self.start_server()
            if self.connected:
                return True
            try:
                logger.info(f"Waiting for IoT device to connect on port {self.port}")
                self.client_socket, addr = self.server_socket.accept()
                self.connected = True
                logger.info(f"IoT device {self.device_id} connected from {addr}")
                return True
            except Exception as e:
                logger.error(f"Failed to accept connection for {self.device_id}: {e}")
                self.client_socket = None
                self.connected = False
                return False

    def disconnect(self) -> None:
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.close()
                except:
                    pass
                finally:
                    self.client_socket = None
                    self.connected = False
            if self.server_socket:
                try:
                    self.server_socket.close()
                except:
                    pass
                finally:
                    self.server_socket = None

    def is_connected(self) -> bool:
        with self.lock:
            return self.connected

    def receive_data(self) -> Optional[List[str]]:
        with self.lock:
            if not self.connected or not self.client_socket:
                return None
            try:
                buffer = b""
                while True:
                    data = self.client_socket.recv(1024)
                    if not data:
                        self.connected = False
                        return None
                    buffer += data
                    if b'\n' in buffer:
                        break
                lines = buffer.decode().strip().splitlines()
                return [line for line in lines if line.strip()]
            except Exception as e:
                logger.error(f"Error receiving from device {self.device_id}: {e}")
                self.connected = False
                return None

class IoTDatabaseDriver:
    def __init__(self, db_config: Dict[str, str]):
        self.db = DatabaseConnection(**db_config)
        self.devices = {}
        self.running = True
        self.device_threads = []

    def setup_devices(self):
        query = """
            SELECT iot_gateway1_ip, iot_gateway1_port,
                iot_gateway2_ip, iot_gateway2_port,
                iot_gateway3_ip, iot_gateway3_port
            FROM iot_gateway_connection
            ORDER BY id DESC LIMIT 1
        """
        result = self.db.execute_query(query)
        if not result:
            logger.error("No IoT gateway configuration found in database.")
            return

        row = result[0]

        device_configs = [
            {"device_id": "device_1", "ip": row["iot_gateway1_ip"], "port": int(row["iot_gateway1_port"]), "ai_range": (1, 6)},
            {"device_id": "device_2", "ip": row["iot_gateway2_ip"], "port": int(row["iot_gateway2_port"]), "ai_range": (7, 12)},
            {"device_id": "device_3", "ip": row["iot_gateway3_ip"], "port": int(row["iot_gateway3_port"]), "ai_range": (13, 18)},
        ]

        for config in device_configs:
            if config["ip"] and config["port"]:
                self.devices[config["device_id"]] = IoTDevice(**config)
                logger.info(f"Configured {config['device_id']} with {config['ip']}:{config['port']}")
            else:
                logger.warning(f"Incomplete config for {config['device_id']} - skipping")


    def get_calibration_values(self):
        """Get calibration values with better error handling"""
        try:
            c_query = "SELECT * FROM c_values ORDER BY id DESC LIMIT 1"
            m_query = "SELECT * FROM m_values ORDER BY id DESC LIMIT 1"
            print("Inside get_calibration_values")
            c_result = self.db.execute_query(c_query)
            print("C -values:", c_result)
            m_result = self.db.execute_query(m_query)
            print("M -values:", m_result)
            
            logger.debug(f"Calibration c_result: {c_result}")
            logger.debug(f"Calibration m_result: {m_result}")
            
            c_values = {}
            m_values = {}
            
            if c_result and len(c_result) > 0:
                for i in range(1, 19):
                    col_name = f"cValue{i}"
                    if col_name in c_result[0]:
                        c_values[f"AI{i}"] = float(c_result[0][col_name]) if c_result[0][col_name] is not None else 0.0
                    else:
                        logger.warning(f"Column {col_name} not found in c_values table")
                        c_values[f"AI{i}"] = 0.0
            else:
                logger.warning("No calibration c_values found, using defaults")
                c_values = {f"AI{i}": 0.0 for i in range(1, 19)}
            
            if m_result and len(m_result) > 0:
                for i in range(1, 19):
                    col_name = f"mValue{i}"
                    if col_name in m_result[0]:
                        m_values[f"AI{i}"] = float(m_result[0][col_name]) if m_result[0][col_name] is not None else 1.0
                    else:
                        logger.warning(f"Column {col_name} not found in m_values table")
                        m_values[f"AI{i}"] = 1.0
            else:
                logger.warning("No calibration m_values found, using defaults")
                m_values = {f"AI{i}": 1.0 for i in range(1, 19)}
            
            logger.info(f"Loaded calibration values - C: {len(c_values)} values, M: {len(m_values)} values")
            return c_values, m_values
            
        except Exception as e:
            logger.exception(f"Error getting calibration values: {e}")
            # Return default values
            c_values = {f"AI{i}": 0.0 for i in range(1, 19)}
            m_values = {f"AI{i}": 1.0 for i in range(1, 19)}
            return c_values, m_values

    def get_production_status(self):
        """Get production status with better error handling"""
        try:
            result = self.db.execute_query("SELECT prodstatus FROM myplclog ORDER BY id DESC LIMIT 1")
            logger.debug(f"Production status result: {result}")
            
            if result and len(result) > 0:
                prod_status = int(result[0]['prodstatus']) if result[0]['prodstatus'] is not None else 0
                logger.info(f"Production status: {prod_status}")
                return prod_status
            else:
                logger.warning("No production status found, defaulting to 0")
                return 0
        except Exception as e:
            logger.exception(f"Error getting production status: {e}")
            return 0

    def get_part_number_id(self):
        """Get part number ID with better error handling"""
        print("Inside get_part_number_id")
        try:
            result = self.db.execute_query("SELECT part_number_id FROM myplclog ORDER BY id DESC LIMIT 1")
            logger.debug(f"Part number ID result: {result}")
            
            if result and len(result) > 0:
                part_id = int(result[0]['part_number_id']) if result[0]['part_number_id'] is not None else None
                logger.info(f"Part number ID: {part_id}")
                return part_id
            else:
                logger.warning("No part number ID found")
                return None
        except Exception as e:
            logger.exception(f"Error getting part number ID: {e}")
            return None

    def process_iot_data(self, device_id, data_str):
        """Process IoT data with enhanced debugging and full trace"""
        try:
            logger.info(f"Processing data from {device_id}: {data_str}")
            
            # Parse JSON data
            try:
                data = json.loads(data_str)
                logger.debug(f"Parsed JSON data: {data}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from {device_id}: {e}")
                return
                        # Get part number id
            print("Calling get_part_number_id()")
            part_id = self.get_part_number_id()
            print(f"Got part_id = {part_id}")

            if part_id is None:
                logger.error("Missing part_number_id - skipping data processing")
                print("ERROR: part_number_id is None. Skipping insert.")
                return
            
            print("Inside process_iot_data: got raw data")

            # Get calibration values
            print("Calling get_calibration_values()")
            c_vals, m_vals = self.get_calibration_values()
            print(f"Got calibration values: C({len(c_vals)}), M({len(m_vals)})")

            # Get production status
            print("Calling get_production_status()")
            prod_status = self.get_production_status()
            print(f"Got prod_status = {prod_status}")

            # Determine table
            table = "leakapp_result_tbl" if prod_status == 0 else "foi_tbl"
            logger.info(f"Using table: {table} (prod_status: {prod_status})")

            now = datetime.datetime.now()

            for k, v in data.items():
                print(f"Processing key: {k}, value: {v}")
                logger.debug(f"Processing key: {k}, value: {v}")

                if k.startswith("AI"):
                    try:
                        m_val = m_vals.get(k, 1.0)
                        c_val = c_vals.get(k, 0.0)
                        cal_val = m_val * float(v) + c_val

                        logger.debug(f"AI calc {k}: {m_val} * {v} + {c_val} = {cal_val}")
                        print(f"Calculated AI value for {k}: {cal_val}")

                        query = f"""
                            INSERT INTO {table} (filter_no, filter_values, batch_counter, date, part_number_id)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        params = (k, cal_val, 0, now, part_id)

                        logger.info(f"Executing AI insert: {params}")
                        print(f"Executing DB insert: {params}")
                        result = self.db.execute_query(query, params)

                        if result is not None and result > 0:
                            logger.info(f"Inserted AI data for {k}: {cal_val}")
                            print(f" Inserted AI {k}")
                        else:
                            logger.error(f"Insert failed for AI {k}")
                            print(f" Insert failed for AI {k}")

                    except Exception as e:
                        logger.exception(f"Error processing AI {k}: {e}")
                        print(f" Exception during AI processing: {e}")

                elif k.startswith("DI"):
                    try:
                        update_query = "UPDATE di_values SET di_value=%s, log_time=%s WHERE di_name=%s"
                        update_params = (v, now, k)
                        print(f"Updating DI value: {update_params}")
                        update_count = self.db.execute_query(update_query, update_params)

                        if update_count is None or update_count == 0:
                            logger.warning(f"DI {k} not found in database — skipping insert.")
                            print(f" DI {k} not found — skipped insert.")
                        else:
                            logger.info(f" Updated DI {k}: {v}")
                            print(f" Updated DI {k}")

                    except Exception as e:
                        logger.exception(f"Error processing DI {k}: {e}")
                        print(f"Exception during DI processing: {e}")

                else:
                    logger.warning(f"Unknown key format: {k}")
                    print(f" Unknown key: {k}")

        except Exception as e:
            logger.exception(f"Critical error in process_iot_data: {e}")
            print(f"Critical exception in processing: {e}")

    def device_worker(self, device_id):
        device = self.devices[device_id]
        reconnect_delay = 1
        max_reconnect_delay = 1

        device.start_server()
        while self.running:
            if not device.is_connected():
                logger.info(f"Waiting for IoT device to connect to server for {device_id}")
                if device.accept_connection():
                    reconnect_delay = 2  # Reset delay on successful connection
                else:
                    logger.warning(f"Failed to accept connection for {device_id}, retrying in {reconnect_delay}s")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    continue

            data_list = device.receive_data()
            if data_list:
                logger.info(f"Received {len(data_list)} data packets from {device_id}")
                for data_str in data_list:
                    if data_str.strip():  # Only process non-empty strings
                        self.process_iot_data(device_id, data_str.strip())
                        logger.info("Data processing completed")
            else:
                logger.warning(f"No data received from {device_id}, disconnecting")
                device.disconnect()
                time.sleep(1)  # Brief pause before reconnection attempt

    def start(self):
        logger.info("Starting IoT Database Driver")
        self.setup_devices()
        
        for device_id in self.devices:
            thread = Thread(target=self.device_worker, args=(device_id,))
            thread.daemon = True
            thread.start()
            self.device_threads.append(thread)
            logger.info(f"Started worker thread for {device_id}")

    def stop(self):
        logger.info("Stopping IoT Database Driver")
        self.running = False
        
        for t in self.device_threads:
            t.join(timeout=5)
            
        for d in self.devices.values():
            d.disconnect()
            
        self.db.close()
        logger.info("IoT Database Driver stopped")

if __name__ == "__main__":
    config = {
        'host': 'localhost', 
        'user': 'root', 
        'password': '', 
        'database': 'leakapp',
        'port': 3306
    }
    
    driver = IoTDatabaseDriver(config)
    try:
        driver.start()
        logger.info("Driver started successfully. Press Ctrl+C to stop.")
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        driver.stop()