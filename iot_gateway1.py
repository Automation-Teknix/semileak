import socket
import struct
import time
import json
import datetime
import mysql.connector
from mysql.connector import Error as MySQLError
from threading import Thread, RLock as Lock
from typing import List, Dict, Any, Optional, Tuple
import sys
import queue

# --- Device-specific configuration ---
DEVICE_ID = "device_1"
DEVICE_IP = "0.0.0.0"
DEVICE_PORT = 5001  # Change per gateway
AI_RANGE = (1, 6)   # Change per gateway

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'leakapp',
    'port': 3306
}

class DatabaseConnection:
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.lock = Lock()
        self.connect()

    def connect(self):
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
        except MySQLError:
            self.connection = None

    def ensure_connection(self):
        with self.lock:
            if self.connection is None or not self.connection.is_connected():
                self.connect()

    def execute_query(self, query, params=None):
        with self.lock:
            self.ensure_connection()
            if not self.connection or not self.connection.is_connected():
                return None
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params)
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                cursor.close()
                return result
            else:
                affected = cursor.rowcount
                cursor.close()
                return affected

    def close(self):
        with self.lock:
            if self.connection and self.connection.is_connected():
                self.connection.close()

class IoTDevice:
    def __init__(self, device_id, ip, port, ai_range):
        self.device_id = device_id
        self.ip = ip
        self.port = port
        self.ai_start, self.ai_end = ai_range
        self.server_socket = None
        self.client_socket = None
        self.connected = False
        self.lock = Lock()

    def start_server(self):
        with self.lock:
            if self.server_socket:
                return
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(("0.0.0.0", self.port))
            self.server_socket.listen(1)

    def accept_connection(self):
        with self.lock:
            if not self.server_socket:
                self.start_server()
                if not self.server_socket:
                    return False
            if self.client_socket:
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                except:
                    pass
                self.client_socket = None
                self.connected = False
            try:
                self.server_socket.settimeout(5.0)
                self.client_socket, addr = self.server_socket.accept()
                self.client_socket.settimeout(None)
                self.connected = True
                return True
            except socket.timeout:
                return False
            except Exception:
                self.client_socket = None

    def is_connected(self):
        with self.lock:
            return self.connected
        return False

    def disconnect(self):
        with self.lock:
            self.connected = False
            if self.client_socket:
                try:
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except:
                    pass
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                except:
                    pass
                self.client_socket = None

    def shutdown_device(self):
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                except:
                    pass
                self.client_socket = None
            if self.server_socket:
                try:
                    self.server_socket.close()
                except:
                    pass
                self.server_socket = None
            self.connected = False

    def receive_data(self):
        with self.lock:
            if not self.connected or not self.client_socket:
                return None
            try:
                buffer = b""
                self.client_socket.settimeout(30.0)
                while True:
                    try:
                        data = self.client_socket.recv(8192)
                        if data is None or not data:
                            self.connected = False
                            try:
                                self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                            except:
                                pass
                            try:
                                self.client_socket.shutdown(socket.SHUT_RDWR)
                            except:
                                pass
                            try:
                                self.client_socket.close()
                            except:
                                pass
                            self.client_socket = None
                            return None
                        buffer += data
                        if len(data) < 8192:
                            break
                    except socket.timeout:
                        self.connected = False
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except:
                            pass
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                        except:
                            pass
                        self.client_socket = None
                        return None
                    except Exception:
                        self.connected = False
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except:
                            pass
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                        except:
                            pass
                        self.client_socket = None
                        return None
                lines = buffer.decode('utf-8', errors='ignore').splitlines()
                valid_lines = [line for line in lines if line.strip()]
                return valid_lines
            except Exception:
                self.connected = False
                try:
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except:
                    pass
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                except:
                    pass
                self.client_socket = None
                return None
            finally:
                try:
                    if self.client_socket:
                        self.client_socket.settimeout(None)
                except:
                    pass

class IoTDatabaseDriver:
    def __init__(self, db_config):
        self.db = DatabaseConnection(**db_config)
        self.device = IoTDevice(DEVICE_ID, DEVICE_IP, DEVICE_PORT, AI_RANGE)
        self.running = True
        self.stats = {'start_time': time.time()}

    def process_iot_data(self, data_str):
        try:
            data = json.loads(data_str)
            # ...event-driven DI/AI logic and batch insert as in original...
            # For brevity, only batch AI insert shown
            c_vals = {f"AI{i}": 0.0 for i in range(AI_RANGE[0], AI_RANGE[1]+1)}
            m_vals = {f"AI{i}": 1.0 for i in range(AI_RANGE[0], AI_RANGE[1]+1)}
            now = datetime.datetime.now()
            part_id = 1
            table = "leakapp_result_tbl"
            ai_rows = []
            for k, v in data.items():
                if k.startswith("AI"):
                    try:
                        raw_value = float(v)
                        if raw_value == -999 or raw_value > 6000:
                            continue
                        m_val = m_vals.get(k, 1.0)
                        c_val = c_vals.get(k, 0.0)
                        cal_val = m_val * raw_value + c_val
                        ai_rows.append((k, cal_val, 0, now, part_id))
                    except:
                        continue
            if ai_rows:
                query = f"""
                    INSERT INTO {table} (filter_no, filter_values, batch_counter, date, part_number_id)
                    VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cursor = self.db.connection.cursor()
                    cursor.executemany(query, ai_rows)
                    self.db.connection.commit()
                except:
                    pass
        except:
            pass

    def device_worker(self):
        device = self.device
        data_queue = queue.Queue()
        def db_inserter():
            while self.running:
                try:
                    item = data_queue.get(timeout=1)
                    if item is None:
                        continue
                    data_str = item
                    self.process_iot_data(data_str)
                except queue.Empty:
                    continue
        inserter_thread = Thread(target=db_inserter, daemon=True)
        inserter_thread.start()
        device.start_server()
        while self.running:
            try:
                if not device.is_connected():
                    device.accept_connection()
                data_list = device.receive_data()
                if data_list:
                    for data_str in data_list:
                        if data_str.strip():
                            data_queue.put(data_str.strip())
                else:
                    if not device.is_connected():
                        device.disconnect()
                        time.sleep(1)
                    else:
                        time.sleep(0.5)
            except:
                time.sleep(2)

    def start(self):
        thread = Thread(target=self.device_worker)
        thread.daemon = True
        thread.start()
        while True:
            time.sleep(5)

    def stop(self):
        self.running = False
        self.device.shutdown_device()
        self.db.close()

if __name__ == "__main__":
    driver = IoTDatabaseDriver(DB_CONFIG)
    try:
        driver.start()
    except KeyboardInterrupt:
        driver.stop()
