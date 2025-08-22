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
import logging
from pathlib import Path

class DatabaseConnection:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.logger = logging.getLogger('database.connection')
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.lock = Lock()
        
        self.logger.info(f"Initializing database connection to {host}:{port}/{database}")
        self.connect()

    def connect(self) -> None:
        try:
            self.logger.debug(f"Attempting to connect to MySQL database at {self.host}:{self.port}")
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
            self.logger.info("Database connection established successfully")
        except MySQLError as e:
            self.logger.error(f"Database connection failed: {e}")
            logging.getLogger('errors').error(f"Database connection failed: {e}")
            self.connection = None

    def ensure_connection(self) -> None:
        with self.lock:
            try:
                if self.connection is None or not self.connection.is_connected():
                    self.logger.warning("Database connection lost, attempting to reconnect")
                    self.connect()
                else:
                    # Test connection with ping
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    self.logger.debug("Database connection is healthy")
            except MySQLError as e:
                self.logger.error(f"Connection health check failed: {e}")
                logging.getLogger('errors').error(f"Database connection health check failed: {e}")
                try:
                    if self.connection:
                        self.connection.close()
                        self.logger.debug("Closed unhealthy connection")
                except Exception as close_error:
                    self.logger.warning(f"Error closing connection: {close_error}")
                self.connection = None
                self.connect()

    def execute_query(self, query: str, params: tuple = None) -> Optional[Any]:
        self.logger.debug(f"Preparing to execute query: {query[:100]}{'...' if len(query) > 100 else ''}")
        
        with self.lock:
            self.ensure_connection()
            if not self.connection or not self.connection.is_connected():
                self.logger.error("No active database connection available")
                return None
                
            try:
                self.logger.debug(f"Executing query with params: {params}")
                cursor = self.connection.cursor(dictionary=True)
                start_time = time.time()
                cursor.execute(query, params)
                execution_time = time.time() - start_time
                
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    cursor.close()
                    self.logger.debug(f"SELECT query returned {len(result)} rows in {execution_time:.3f}s")
                    if len(result) > 0:
                        self.logger.debug(f"Sample result: {str(result[0])[:200]}{'...' if len(str(result[0])) > 200 else ''}")
                    return result
                else:
                    affected = cursor.rowcount
                    cursor.close()
                    self.logger.debug(f"Query executed successfully, {affected} rows affected in {execution_time:.3f}s")
                    return affected
                    
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}")
                self.logger.error(f"Query: {query}")
                self.logger.error(f"Params: {params}")
                logging.getLogger('errors').error(f"Database query execution failed: {e} | Query: {query} | Params: {params}")
                raise

    def close(self) -> None:
        with self.lock:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.logger.info("Database connection closed")


class IoTDevice:
    def __init__(self, device_id: str, ip: str, port: int, ai_range: Tuple[int, int]):
        self.logger = logging.getLogger(f'iot_device.{device_id}')
        self.device_id = device_id
        self.ip = ip
        self.port = port
        self.ai_start, self.ai_end = ai_range
        self.server_socket = None
        self.client_socket = None
        self.connected = False
        self.lock = Lock()
        
        self.logger.info(f"Initialized IoT device {device_id} - IP: {ip}, Port: {port}, AI Range: {ai_range}")

    def start_server(self) -> None:
        with self.lock:
            if self.server_socket:
                self.logger.debug("Server socket already exists")
                return
            try:
                self.logger.info(f"Starting server on port {self.port}")
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind(("0.0.0.0", self.port))
                self.server_socket.listen(1)
                self.logger.info(f"Server started successfully on port {self.port}")
            except Exception as e:
                self.logger.error(f"Failed to start server on port {self.port}: {e}")
                logging.getLogger('errors').error(f"IoT device {self.device_id} failed to start server: {e}")
                self.server_socket = None

    def accept_connection(self) -> bool:
        with self.lock:
            if not self.server_socket:
                self.logger.debug("No server socket available, starting server")
                self.start_server()
                if not self.server_socket:
                    return False

            # Always close previous client socket before accepting new connection
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
                self.logger.info("Waiting for client connection...")
                # Set socket timeout to avoid hanging indefinitely
                self.server_socket.settimeout(5.0)  # 5 second timeout
                self.client_socket, addr = self.server_socket.accept()
                # Reset to blocking mode for data operations
                self.client_socket.settimeout(None)
                self.connected = True
                self.logger.info(f"Client connected from {addr[0]}:{addr[1]}")
                return True
            except socket.timeout:
                self.logger.debug("Connection accept timeout")
                return False
            except Exception as e:
                self.logger.warning(f"Failed to accept connection: {e}")
                self.client_socket = None

    def is_connected(self) -> bool:
        with self.lock:
            return self.connected
        return False

    def disconnect(self) -> None:
        with self.lock:
            was_connected = self.connected
            self.connected = False  # Set this first to prevent race conditions

            if self.client_socket:
                try:
                    # Force socket to close immediately
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except Exception as linger_err:
                    self.logger.debug(f"SO_LINGER set failed: {linger_err}")
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass  # Socket might already be closed by peer
                try:
                    self.client_socket.close()
                    self.logger.info("Client connection closed properly (forced)")
                except Exception as e:
                    self.logger.warning(f"Error closing client socket: {e}")
                finally:
                    self.client_socket = None

            # Don't close server socket here - keep it listening
            # Only close on full shutdown
            if was_connected:
                self.logger.info(f"Device {self.device_id} disconnected, ready for new connection")

    def shutdown_device(self) -> None:
        """Properly shutdown the device and close all sockets"""
        with self.lock:
            self.logger.info(f"Shutting down device {self.device_id}")
            
            # Close client connection first
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
                
            # Close server socket
            if self.server_socket:
                try:
                    self.server_socket.close()
                    self.logger.info("Server socket closed")
                except Exception as e:
                    self.logger.warning(f"Error closing server socket: {e}")
                finally:
                    self.server_socket = None
                    
            self.connected = False

    def receive_data(self) -> Optional[List[str]]:
        import struct
        with self.lock:
            print(f"[RECEIVE] Starting receive_data for {self.device_id}, connected={self.connected}")
            if not self.connected or not self.client_socket:
                print(f"[RECEIVE] No active client connection for {self.device_id}")
                return None
            try:
                buffer = b""
                start_time = time.time()
                self.client_socket.settimeout(30.0)  # 30 second timeout
                print(f"[RECEIVE] Entering receive loop for {self.device_id}")

                while True:
                    try:
                        data = self.client_socket.recv(8192)
                        print(f"[RECEIVE] recv called for {self.device_id}, got {len(data) if data else 'None'} bytes")
                        if data is None:
                            print(f"[RECEIVE] Data is None for {self.device_id}, breaking loop")
                            break
                        if not data:
                            print(f"[RECEIVE] Peer closed connection for {self.device_id}")
                            self.connected = False
                            # Force socket close
                            try:
                                self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                            except Exception as linger_err:
                                print(f"[RECEIVE] SO_LINGER set failed: {linger_err}")
                            try:
                                self.client_socket.shutdown(socket.SHUT_RDWR)
                            except:
                                pass
                            try:
                                self.client_socket.close()
                                print(f"[RECEIVE] Client socket closed (forced) for {self.device_id}")
                            except Exception as e:
                                print(f"[RECEIVE] Error closing client socket: {e}")
                            self.client_socket = None
                            return None
                        buffer += data
                        if len(data) < 8192:
                            print(f"[RECEIVE] Less than buffer size received for {self.device_id}, breaking loop")
                            break
                    except socket.timeout:
                        print(f"[RECEIVE] Timeout for {self.device_id}")
                        self.connected = False
                        # Force socket close
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e:
                            print(f"[RECEIVE] Error closing client socket: {e}")
                        self.client_socket = None
                        return None
                    except ConnectionResetError:
                        print(f"[RECEIVE] Connection reset by peer for {self.device_id}")
                        self.connected = False
                        # Force socket close
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e:
                            print(f"[RECEIVE] Error closing client socket: {e}")
                        self.client_socket = None
                        return None
                    except Exception as e:
                        print(f"[RECEIVE] Socket error for {self.device_id}: {e}")
                        self.connected = False
                        # Force socket close
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e2:
                            print(f"[RECEIVE] Error closing client socket: {e2}")
                        self.client_socket = None
                        return None

                receive_time = time.time() - start_time
                lines = buffer.decode('utf-8', errors='ignore').splitlines()
                valid_lines = [line for line in lines if line.strip()]
                print(f"[RECEIVE] Finished for {self.device_id}: {len(buffer)} bytes, {len(valid_lines)} valid lines, time={receive_time:.3f}s")
                if valid_lines:
                    print(f"[RECEIVE] Sample data for {self.device_id}: {valid_lines[0][:100]}{'...' if len(valid_lines[0]) > 100 else ''}")

                return valid_lines

            except Exception as e:
                print(f"[RECEIVE] Error receiving data for {self.device_id}: {e}")
                self.connected = False
                # Force socket close
                try:
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except Exception as linger_err:
                    print(f"[RECEIVE] SO_LINGER set failed: {linger_err}")
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                    print(f"[RECEIVE] Client socket closed (forced) for {self.device_id}")
                except Exception as e2:
                    print(f"[RECEIVE] Error closing client socket: {e2}")
                self.client_socket = None
                return None
            finally:
                try:
                    if self.client_socket:
                        self.client_socket.settimeout(None)
                except:
                    pass


class IoTDatabaseDriver:
    def __init__(self, db_config: Dict[str, str]):
        self.logger = logging.getLogger('iot_driver')
        self.data_logger = logging.getLogger('data_processing')
        
        self.logger.info("Initializing IoT Database Driver")
        self.logger.info(f"Database config: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        
        self.db = DatabaseConnection(**db_config)
        self.devices = {}
        self.running = True
        self.device_threads = []
        
        # Statistics
        self.stats = {
            'start_time': time.time(),
            'total_messages_processed': 0,
            'successful_db_operations': 0,
            'failed_db_operations': 0,
            'device_reconnections': 0
        }
        # Event-driven DI/AI state tracking
        self.di_states = {}  # {di_name: {'active': False, 'start_time': None, 'first_checked': False, 'first_result': None, 'ai_buffer': [], 'skip_second': False}}

    def setup_devices(self):
        self.logger.info("Setting up IoT devices from database configuration")
        
        query = """
            SELECT iot_gateway1_ip, iot_gateway1_port,
                iot_gateway2_ip, iot_gateway2_port,
                iot_gateway3_ip, iot_gateway3_port
            FROM iot_gateway_connection
            ORDER BY id DESC LIMIT 1
        """
        
        try:
            result = self.db.execute_query(query)
            if not result:
                self.logger.error("No device configuration found in database")
                return

            row = result[0]
            self.logger.debug(f"Retrieved device configuration: {row}")

            device_configs = [
                {"device_id": "device_1", "ip": row["iot_gateway1_ip"], "port": int(row["iot_gateway1_port"]) if row["iot_gateway1_port"] else None, "ai_range": (1, 6)},
                {"device_id": "device_2", "ip": row["iot_gateway2_ip"], "port": int(row["iot_gateway2_port"]) if row["iot_gateway2_port"] else None, "ai_range": (7, 12)},
                {"device_id": "device_3", "ip": row["iot_gateway3_ip"], "port": int(row["iot_gateway3_port"]) if row["iot_gateway3_port"] else None, "ai_range": (13, 18)},
            ]

            for config in device_configs:
                if config["ip"] and config["port"]:
                    self.devices[config["device_id"]] = IoTDevice(**config)
                    self.logger.info(f"Created device {config['device_id']}: {config['ip']}:{config['port']}")
                else:
                    self.logger.warning(f"Incomplete config for {config['device_id']} - skipping (IP: {config['ip']}, Port: {config['port']})")
                    
            self.logger.info(f"Successfully configured {len(self.devices)} IoT devices")
            
        except Exception as e:
            self.logger.error(f"Failed to setup devices: {e}")
            logging.getLogger('errors').error(f"Device setup failed: {e}")

    def get_calibration_values(self):
        """Get calibration values with comprehensive logging"""
        self.data_logger.debug("Retrieving calibration values from database")
        
        try:
            c_query = "SELECT * FROM c_values ORDER BY id DESC LIMIT 1"
            m_query = "SELECT * FROM m_values ORDER BY id DESC LIMIT 1"
            
            c_result = self.db.execute_query(c_query)
            m_result = self.db.execute_query(m_query)
            
            c_values = {}
            m_values = {}
            
            if c_result and len(c_result) > 0:
                self.data_logger.debug("Processing C values from database")
                for i in range(1, 19):
                    col_name = f"cValue{i}"
                    if col_name in c_result[0]:
                        c_values[f"AI{i}"] = float(c_result[0][col_name]) if c_result[0][col_name] is not None else 0.0
                    else:
                        c_values[f"AI{i}"] = 0.0
                        self.data_logger.warning(f"Missing column {col_name} in c_values table")
            else:
                self.data_logger.warning("No C values found in database, using defaults")
                c_values = {f"AI{i}": 0.0 for i in range(1, 19)}
            
            if m_result and len(m_result) > 0:
                self.data_logger.debug("Processing M values from database")
                for i in range(1, 19):
                    col_name = f"mValue{i}"
                    if col_name in m_result[0]:
                        m_values[f"AI{i}"] = float(m_result[0][col_name]) if m_result[0][col_name] is not None else 1.0
                    else:
                        m_values[f"AI{i}"] = 1.0
                        self.data_logger.warning(f"Missing column {col_name} in m_values table")
            else:
                self.data_logger.warning("No M values found in database, using defaults")
                m_values = {f"AI{i}": 1.0 for i in range(1, 19)}
            
            self.data_logger.info(f"Retrieved calibration values: {len(c_values)} C values, {len(m_values)} M values")
            return c_values, m_values
            
        except Exception as e:
            self.data_logger.error(f"Failed to get calibration values: {e}")
            logging.getLogger('errors').error(f"Calibration values retrieval failed: {e}")
            c_values = {f"AI{i}": 0.0 for i in range(1, 19)}
            m_values = {f"AI{i}": 1.0 for i in range(1, 19)}
            return c_values, m_values

    def get_production_status(self):
        """Get production status with comprehensive logging"""
        self.data_logger.debug("Retrieving production status from database")
        
        try:
            result = self.db.execute_query("SELECT prodstatus FROM myplclog ORDER BY id DESC LIMIT 1")            
            if result and len(result) > 0:
                prod_status = int(result[0]['prodstatus']) if result[0]['prodstatus'] is not None else 0
                self.data_logger.debug(f"Retrieved production status: {prod_status}")
                return prod_status
            else:
                self.data_logger.warning("No production status found in database, using default (0)")
                return 0
        except Exception as e:
            self.data_logger.error(f"Failed to get production status: {e}")
            logging.getLogger('errors').error(f"Production status retrieval failed: {e}")
            return 0

    def get_part_number_id(self):
        """Get part number ID with comprehensive logging"""
        self.data_logger.debug("Retrieving part number ID from database")
        
        try:
            result = self.db.execute_query("SELECT part_number_id FROM myplclog ORDER BY id DESC LIMIT 1")
            
            if result and len(result) > 0:
                part_id = int(result[0]['part_number_id']) if result[0]['part_number_id'] is not None else None
                self.data_logger.debug(f"Retrieved part number ID: {part_id}")
                return part_id
            else:
                self.data_logger.warning("No part number ID found in database")
                return None
        except Exception as e:
            self.data_logger.error(f"Failed to get part number ID: {e}")
            logging.getLogger('errors').error(f"Part number ID retrieval failed: {e}")
            return None

    def log_statistics(self):
        """Log current system statistics"""
        uptime = time.time() - self.stats['start_time']
        uptime_str = str(datetime.timedelta(seconds=int(uptime)))
        
        self.logger.info("=== SYSTEM STATISTICS ===")
        self.logger.info(f"Uptime: {uptime_str}")
        self.logger.info(f"Total messages processed: {self.stats['total_messages_processed']}")
        self.logger.info(f"Successful DB operations: {self.stats['successful_db_operations']}")
        self.logger.info(f"Failed DB operations: {self.stats['failed_db_operations']}")
        self.logger.info(f"Device reconnections: {self.stats['device_reconnections']}")
        self.logger.info(f"Active devices: {len([d for d in self.devices.values() if d.is_connected()])}/{len(self.devices)}")
        
        # Messages per hour
        if uptime > 0:
            msg_per_hour = (self.stats['total_messages_processed'] / uptime) * 3600
            self.logger.info(f"Message rate: {msg_per_hour:.1f} messages/hour")

    def process_iot_data(self, device_id, data_str):
        """Process IoT data with comprehensive logging and error handling"""
        self.data_logger.debug(f"Processing data from {device_id}: {data_str[:100]}{'...' if len(data_str) > 100 else ''}")
        
        try:
            self.stats['total_messages_processed'] += 1
            
            # Parse JSON data
            try:
                data = json.loads(data_str)
                self.data_logger.debug(f"Successfully parsed JSON data with {len(data)} keys")
            except json.JSONDecodeError as e:
                self.data_logger.error(f"Invalid JSON data from {device_id}: {e}")
                return

            # Get part number id
            part_id = self.get_part_number_id()
            if part_id is None:
                self.data_logger.error("Part number ID is None, cannot process data")
                self.stats['failed_db_operations'] += 1
                return

            # Get calibration values
            c_vals, m_vals = self.get_calibration_values()

            # Get production status
            prod_status = self.get_production_status()

            # Determine table
            table = "leakapp_result_tbl" if prod_status == 0 else "foi_tbl"
            self.data_logger.debug(f"Using table: {table} (prod_status: {prod_status})")

            now = datetime.datetime.now()
            processed_count = 0
            skipped_count = 0

            # --- Event-driven DI/AI logic (per-DI/AI pair) ---
            di_transitions = {}
            for k, v in data.items():
                if k.startswith("DI"):
                    di_name = k
                    di_val = int(float(v))
                    di_transitions[di_name] = di_val
                    if di_name not in self.di_states:
                        self.di_states[di_name] = {
                            'active': False,
                            'start_time': None,
                            'ai_window': [],
                            'first_check_done': False,
                            'first_check_time': None,
                            'first_check_status': None,
                            'skip_second': False,
                            'second_window': [],
                        }
            for di_name, di_val in di_transitions.items():
                state = self.di_states[di_name]
                ai_name = f"AI{di_name[2:]}"  # Map DIx to AIx
                # DI rising edge (0->1)
                if di_val == 1 and not state['active']:
                    state['active'] = True
                    state['start_time'] = time.time()
                    state['ai_window'] = []
                    state['first_check_done'] = False
                    state['first_check_time'] = state['start_time'] + 5
                    state['first_check_status'] = None
                    state['skip_second'] = False
                    state['second_window'] = []
                    self.data_logger.info(f"{di_name} became 1: starting 5s window for {ai_name} first value check")
                # DI active
                if di_val == 1 and state['active']:
                    # Collect only the corresponding AI value for this DI, apply calibration
                    if ai_name in data:
                        raw_val = float(data[ai_name])
                        m_val = m_vals.get(ai_name, 1.0)
                        c_val = c_vals.get(ai_name, 0.0)
                        cal_val = m_val * raw_val + c_val
                        state['ai_window'].append((ai_name, cal_val, time.time()))
                    # First 5s check
                    if not state['first_check_done'] and time.time() >= state['first_check_time']:
                        window_vals = [x for x in state['ai_window'] if x[2] - state['start_time'] <= 5 and x[1] != -999 and x[1] <= 5000]
                        if window_vals:
                            highest = max(window_vals, key=lambda x: x[1])
                            ai_name, ai_val, ts = highest
                            setpoint_query = "SELECT setpoint1 FROM leakapp_masterdata WHERE part_number = %s"
                            setpoint_res = self.db.execute_query(setpoint_query, (part_id,))
                            setpoint1 = setpoint_res[0]['setpoint1'] if setpoint_res and 'setpoint1' in setpoint_res[0] else 70
                            status = "OK" if ai_val <= setpoint1 else "NOK"
                            state['first_check_status'] = status
                            state['first_check_done'] = True
                            if status == "NOK":
                                state['skip_second'] = True
                            check_query = "SELECT COUNT(*) as cnt FROM leakapp_test WHERE filter_no = %s"
                            check_res = self.db.execute_query(check_query, (ai_name,))
                            if check_res and check_res[0]['cnt'] > 0:
                                update_query = "UPDATE leakapp_test SET filter_values = %s, highest_value = %s, date = %s, part_number_id = %s, status = %s, shift_id = %s WHERE filter_no = %s"
                                self.db.execute_query(update_query, (ai_val, ai_val, now, part_id, status, 1, ai_name))
                            else:
                                insert_query = "INSERT INTO leakapp_test (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                                self.db.execute_query(insert_query, (ai_name, ai_val, now, ai_val, part_id, status, 1))
                            insert_report = "INSERT INTO leakapp_show_report (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            self.db.execute_query(insert_report, (ai_name, ai_val, now, ai_val, part_id, status, 1))
                            self.data_logger.info(f"First 5s check for {ai_name}: highest={ai_val}, status={status}")
                # DI falling edge (1->0)
                if di_val == 0 and state['active']:
                    if not state['skip_second']:
                        window_vals = [x for x in state['ai_window'] if x[2] - state['start_time'] > 5 and x[1] != -999 and x[1] <= 5000]
                        if window_vals:
                            highest = max(window_vals, key=lambda x: x[1])
                            ai_name, ai_val, ts = highest
                            setpoint_query = "SELECT setpoint2 FROM leakapp_masterdata WHERE part_number = %s"
                            setpoint_res = self.db.execute_query(setpoint_query, (part_id,))
                            setpoint2 = setpoint_res[0]['setpoint2'] if setpoint_res and 'setpoint2' in setpoint_res[0] else 8
                            status = "OK" if ai_val < setpoint2 else "NOK"
                            check_query = "SELECT COUNT(*) as cnt FROM leakapp_test WHERE filter_no = %s"
                            check_res = self.db.execute_query(check_query, (ai_name,))
                            if check_res and check_res[0]['cnt'] > 0:
                                update_query = "UPDATE leakapp_test SET filter_values = %s, highest_value = %s, date = %s, part_number_id = %s, status = %s, shift_id = %s WHERE filter_no = %s"
                                self.db.execute_query(update_query, (ai_val, ai_val, now, part_id, status, 1, ai_name))
                            else:
                                insert_query = "INSERT INTO leakapp_test (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                                self.db.execute_query(insert_query, (ai_name, ai_val, now, ai_val, part_id, status, 1))
                            insert_report = "INSERT INTO leakapp_show_report (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            self.db.execute_query(insert_report, (ai_name, ai_val, now, ai_val, part_id, status, 1))
                            self.data_logger.info(f"Second cycle for {ai_name}: highest={ai_val}, status={status}")
                    state['active'] = False
                    state['start_time'] = None
                    state['ai_window'] = []
                    state['first_check_done'] = False
                    state['first_check_time'] = None
                    state['first_check_status'] = None
                    state['skip_second'] = False
                    state['second_window'] = []

            # Batch insert for AI values
            ai_rows = []
            for k, v in data.items():
                self.data_logger.debug(f"Processing data point: {k} = {v}")
                if k.startswith("AI"):
                    try:
                        raw_value = float(v)
                        if raw_value == -999 or raw_value > 6000:
                            self.data_logger.warning(f"Skipping AI {k} due to invalid value: {raw_value}")
                            skipped_count += 1
                            continue
                        m_val = m_vals.get(k, 1.0)
                        c_val = c_vals.get(k, 0.0)
                        cal_val = m_val * raw_value + c_val
                        self.data_logger.debug(f"AI {k}: raw={raw_value}, calibrated={cal_val} (m={m_val}, c={c_val})")
                        ai_rows.append((k, cal_val, 0, now, part_id))
                    except Exception as e:
                        self.data_logger.error(f"Exception processing AI {k}: {e}")
                        logging.getLogger('errors').error(f"AI processing error for {k}: {e}")
                        self.stats['failed_db_operations'] += 1
            if ai_rows:
                query = f"""
                    INSERT INTO {table} (filter_no, filter_values, batch_counter, date, part_number_id)
                    VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cursor = self.db.connection.cursor()
                    cursor.executemany(query, ai_rows)
                    self.db.connection.commit()
                    processed_count += len(ai_rows)
                    self.stats['successful_db_operations'] += len(ai_rows)
                    self.data_logger.debug(f"Batch inserted {len(ai_rows)} AI values for {device_id}")
                except Exception as e:
                    self.data_logger.error(f"Batch insert failed for AI values: {e}")
                    self.stats['failed_db_operations'] += len(ai_rows)

            # Process DI and other keys
            for k, v in data.items():
                if k.startswith("DI"):
                    try:
                        # Get previous value
                        select_query = "SELECT di_value FROM di_values WHERE di_name=%s ORDER BY log_time DESC LIMIT 1"
                        prev_result = self.db.execute_query(select_query, (k,))
                        prev_value = int(float(prev_result[0]['di_value'])) if prev_result and prev_result[0]['di_value'] is not None else None
                        new_value = int(float(v))
                        self.data_logger.debug(f"DI {k}: previous={prev_value}, new={new_value}")
                        # Only update if values are different
                        if prev_value is not None:
                            if prev_value != new_value:
                                update_query = "UPDATE di_values SET di_value=%s, log_time=%s WHERE di_name=%s"
                                update_params = (new_value, now, k)
                                update_count = self.db.execute_query(update_query, update_params)
                                if update_count is None or update_count == 0:
                                    self.data_logger.warning(f"DI {k} not found in database")
                                else:
                                    self.data_logger.debug(f"Successfully updated DI {k}")
                                    processed_count += 1
                                    self.stats['successful_db_operations'] += 1
                            else:
                                self.data_logger.debug(f"DI {k}: Value unchanged, skipping update")
                                skipped_count += 1
                    except Exception as e:
                        self.data_logger.error(f"Exception processing DI {k}: {e}")
                        logging.getLogger('errors').error(f"DI processing error for {k}: {e}")
                        self.stats['failed_db_operations'] += 1
                elif not k.startswith("AI"):
                    self.data_logger.debug(f"Unknown data key: {k}")
                    skipped_count += 1

                elif k.startswith("DI"):
                    try:
                        # Get previous value
                        select_query = "SELECT di_value FROM di_values WHERE di_name=%s ORDER BY log_time DESC LIMIT 1"
                        prev_result = self.db.execute_query(select_query, (k,))
                        prev_value = int(float(prev_result[0]['di_value'])) if prev_result and prev_result[0]['di_value'] is not None else None
                        new_value = int(float(v))
                        
                        self.data_logger.debug(f"DI {k}: previous={prev_value}, new={new_value}")

                        # Only update if values are different
                        if prev_value is not None:
                            if prev_value != new_value:
                                update_query = "UPDATE di_values SET di_value=%s, log_time=%s WHERE di_name=%s"
                                update_params = (new_value, now, k)
                                
                                update_count = self.db.execute_query(update_query, update_params)

                                if update_count is None or update_count == 0:
                                    self.data_logger.warning(f"DI {k} not found in database")
                                else:
                                    self.data_logger.debug(f"Successfully updated DI {k}")
                                    processed_count += 1
                                    self.stats['successful_db_operations'] += 1
                            else:
                                self.data_logger.debug(f"DI {k}: Value unchanged, skipping update")
                                skipped_count += 1

                    except Exception as e:
                        self.data_logger.error(f"Exception processing DI {k}: {e}")
                        logging.getLogger('errors').error(f"DI processing error for {k}: {e}")
                        self.stats['failed_db_operations'] += 1

                else:
                    self.data_logger.debug(f"Unknown data key: {k}")
                    skipped_count += 1

            self.data_logger.info(f"Data processing complete for {device_id}: {processed_count} processed, {skipped_count} skipped")

        except Exception as e:
            self.data_logger.error(f"Critical exception in data processing from {device_id}: {e}")
            logging.getLogger('errors').error(f"Critical data processing error from {device_id}: {e}")
            self.stats['failed_db_operations'] += 1

    def device_worker(self, device_id):
        """Device worker thread with comprehensive logging"""
        worker_logger = logging.getLogger(f'iot_device.{device_id}.worker')
        device = self.devices[device_id]
        reconnect_delay = 1
        max_reconnect_delay = 2  # Reduce max delay for faster reconnection

        # Create a queue for async DB insertion
        data_queue = queue.Queue()

        def db_inserter():
            while self.running:
                try:
                    item = data_queue.get(timeout=1)
                    if item is None:
                        continue
                    dev_id, data_str = item
                    try:
                        self.process_iot_data(dev_id, data_str)
                    except Exception as e:
                        print(f"[ERROR] DB insert error: {e}")
                except queue.Empty:
                    continue

        inserter_thread = Thread(target=db_inserter, daemon=True)
        inserter_thread.start()

        # Map device_id to myplclog column
        connection_column = None
        if device_id == "device_1":
            connection_column = "server_connection_1"
        elif device_id == "device_2":
            connection_column = "server_connection_2"
        elif device_id == "device_3":
            connection_column = "server_connection_3"

        worker_logger.info(f"Starting device worker thread for {device_id}")
        device.start_server()

        while self.running:
            try:
                if not device.is_connected():
                    worker_logger.debug("Device not connected, attempting connection")
                    if device.accept_connection():
                        reconnect_delay = 1  # Reset delay on successful connection
                        self.stats['device_reconnections'] += 1
                        worker_logger.info(f"Successfully connected to {device_id}")
                        # Update myplclog table for this device only if value does not match
                        if connection_column:
                            try:
                                select_query = f"SELECT {connection_column} FROM myplclog WHERE id = (SELECT MAX(id) FROM myplclog)"
                                result = self.db.execute_query(select_query)
                                current_val = result[0][connection_column] if result and connection_column in result[0] else None
                                if current_val != 1:
                                    update_query = f"UPDATE myplclog SET {connection_column} = 1 WHERE id = (SELECT MAX(id) FROM myplclog)"
                                    affected = self.db.execute_query(update_query)
                                    worker_logger.debug(f"Updated {connection_column} to connected status, rows affected: {affected}")
                                    self.stats['successful_db_operations'] += 1
                                else:
                                    worker_logger.debug(f"{connection_column} already set to 1 (connected), no update needed")
                            except Exception as e:
                                worker_logger.error(f"Failed to update connection status: {e}")
                                self.stats['failed_db_operations'] += 1
                    else:
                        worker_logger.warning(f"Connection attempt failed, retrying in {reconnect_delay}s")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay + 1, max_reconnect_delay)
                        continue

                # Receive and process data
                data_list = device.receive_data()
                if data_list:
                    for data_str in data_list:
                        if data_str.strip():
                            # Queue data for async DB insertion
                            data_queue.put((device_id, data_str.strip()))
                else:
                    # If no data, keep connection alive unless peer closes it
                    if not device.is_connected():
                        worker_logger.warning("Device disconnected by peer or error, attempting immediate reconnect")
                        device.disconnect()
                        # Update myplclog table to show disconnected status only if value does not match
                        if connection_column:
                            try:
                                select_query = f"SELECT {connection_column} FROM myplclog WHERE id = (SELECT MAX(id) FROM myplclog)"
                                result = self.db.execute_query(select_query)
                                current_val = result[0][connection_column] if result and connection_column in result[0] else None
                                if current_val != 0:
                                    update_query = f"UPDATE myplclog SET {connection_column} = 0 WHERE id = (SELECT MAX(id) FROM myplclog)"
                                    affected = self.db.execute_query(update_query)
                                    worker_logger.debug(f"Updated {connection_column} to disconnected status, rows affected: {affected}")
                                    self.stats['successful_db_operations'] += 1
                                else:
                                    worker_logger.debug(f"{connection_column} already set to 0 (disconnected), no update needed")
                            except Exception as e:
                                worker_logger.error(f"Failed to update disconnection status: {e}")
                                self.stats['failed_db_operations'] += 1
                        time.sleep(1)  # Immediate retry
                    else:
                        # No data, but still connected: just keep waiting
                        worker_logger.debug("No data received, holding connection and waiting for next data...")
                        time.sleep(0.5)
            except Exception as e:
                worker_logger.error(f"Unexpected error in device worker: {e}")
                logging.getLogger('errors').error(f"Device worker {device_id} unexpected error: {e}")
                time.sleep(2)  # Shorter pause on unexpected errors
        worker_logger.info(f"Device worker thread for {device_id} shutting down")

    def start(self):
        """Start the IoT Database Driver system"""
        self.logger.info("Starting IoT Database Driver system")
        
        # Log system statistics every 5 minutes
        def stats_logger():
            while self.running:
                time.sleep(300)  # 5 minutes
                if self.running:
                    self.log_statistics()
        
        stats_thread = Thread(target=stats_logger, daemon=True)
        stats_thread.start()
        
        self.setup_devices()
        
        if not self.devices:
            self.logger.error("No devices configured, cannot start system")
            return
        
        self.logger.info(f"Starting {len(self.devices)} device worker threads")
        
        for device_id in self.devices:
            thread = Thread(target=self.device_worker, args=(device_id,))
            thread.daemon = True
            thread.start()
            self.device_threads.append(thread)
            self.logger.info(f"Started worker thread for {device_id}")
        
        self.logger.info("IoT Database Driver system started successfully")

    def stop(self):
        """Stop the IoT Database Driver system"""
        self.logger.info("Shutting down IoT Database Driver system")
        
        self.running = False
        
        self.logger.info("Waiting for worker threads to complete...")
        for i, t in enumerate(self.device_threads):
            t.join(timeout=5)
            if t.is_alive():
                self.logger.warning(f"Thread {i} did not shut down gracefully")
            else:
                self.logger.debug(f"Thread {i} shut down successfully")
        
        self.logger.info("Disconnecting all devices...")
        for device_id, device in self.devices.items():
            device.shutdown_device()  # Use new shutdown method
            self.logger.debug(f"Shutdown {device_id}")
        
        self.logger.info("Closing database connection...")
        self.db.close()
        
        # Final statistics
        self.log_statistics()
        self.logger.info("IoT Database Driver system shutdown complete")


if __name__ == "__main__":
    # Setup logging first
    # setup_logging()
    logger = logging.getLogger('main')
    
    config = {
        'host': 'localhost', 
        'user': 'root', 
        'password': '', 
        'database': 'leakapp',
        'port': 3306
    }
    
    logger.info("Starting IoT Database Driver application")
    logger.info(f"Configuration: {config['host']}:{config['port']}/{config['database']}")
    
    driver = IoTDatabaseDriver(config)
    
    try:
        driver.start()
        logger.info("System running... Press Ctrl+C to stop")
        
        while True:
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
        logging.getLogger('errors').error(f"Main loop error: {e}")
    finally:
        driver.stop()
        logger.info("Application shutdown complete")