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
        self.logger = logging.getLogger('database.connection.gateway2')
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.lock = Lock()
        
        self.logger.info(f"Gateway2: Initializing database connection to {host}:{port}/{database}")
        self.connect()

    def connect(self) -> None:
        try:
            self.logger.debug(f"Gateway2: Attempting to connect to MySQL database at {self.host}:{self.port}")
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
            self.logger.info("Gateway2: Database connection established successfully")
        except MySQLError as e:
            self.logger.error(f"Gateway2: Database connection failed: {e}")
            logging.getLogger('errors').error(f"Gateway2: Database connection failed: {e}")
            self.connection = None

    def ensure_connection(self) -> None:
        with self.lock:
            try:
                if self.connection is None or not self.connection.is_connected():
                    self.logger.warning("Gateway2: Database connection lost, attempting to reconnect")
                    self.connect()
                else:
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    self.logger.debug("Gateway2: Database connection is healthy")
            except MySQLError as e:
                self.logger.error(f"Gateway2: Connection health check failed: {e}")
                logging.getLogger('errors').error(f"Gateway2: Database connection health check failed: {e}")
                try:
                    if self.connection:
                        self.connection.close()
                        self.logger.debug("Gateway2: Closed unhealthy connection")
                except Exception as close_error:
                    self.logger.warning(f"Gateway2: Error closing connection: {close_error}")
                self.connection = None
                self.connect()

    def execute_query(self, query: str, params: tuple = None) -> Optional[Any]:
        self.logger.debug(f"Gateway2: Preparing to execute query: {query[:100]}{'...' if len(query) > 100 else ''}")
        
        with self.lock:
            self.ensure_connection()
            if not self.connection or not self.connection.is_connected():
                self.logger.error("Gateway2: No active database connection available")
                return None
                
            try:
                self.logger.debug(f"Gateway2: Executing query with params: {params}")
                cursor = self.connection.cursor(dictionary=True)
                start_time = time.time()
                cursor.execute(query, params)
                execution_time = time.time() - start_time
                
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    cursor.close()
                    self.logger.debug(f"Gateway2: SELECT query returned {len(result)} rows in {execution_time:.3f}s")
                    if len(result) > 0:
                        self.logger.debug(f"Gateway2: Sample result: {str(result[0])[:200]}{'...' if len(str(result[0])) > 200 else ''}")
                    return result
                else:
                    affected = cursor.rowcount
                    cursor.close()
                    self.logger.debug(f"Gateway2: Query executed successfully, {affected} rows affected in {execution_time:.3f}s")
                    return affected
                    
            except Exception as e:
                self.logger.error(f"Gateway2: Query execution failed: {e}")
                self.logger.error(f"Gateway2: Query: {query}")
                self.logger.error(f"Gateway2: Params: {params}")
                logging.getLogger('errors').error(f"Gateway2: Database query execution failed: {e} | Query: {query} | Params: {params}")
                raise

    def close(self) -> None:
        with self.lock:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.logger.info("Gateway2: Database connection closed")


class IoTGateway2:
    def __init__(self, device_id: str, ip: str, port: int, ai_range: Tuple[int, int]):
        self.logger = logging.getLogger(f'iot_gateway2.{device_id}')
        self.device_id = device_id
        self.ip = ip
        self.port = port
        self.ai_start, self.ai_end = ai_range
        self.server_socket = None
        self.client_socket = None
        self.connected = False
        self.lock = Lock()
        
        self.logger.info(f"Gateway2: Initialized IoT device {device_id} - IP: {ip}, Port: {port}, AI Range: {ai_range}")

    def start_server(self) -> None:
        with self.lock:
            if self.server_socket:
                self.logger.debug("Gateway2: Server socket already exists")
                return
            try:
                self.logger.info(f"Gateway2: Starting server on port {self.port}")
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind(("0.0.0.0", self.port))
                self.server_socket.listen(1)
                self.logger.info(f"Gateway2: Server started successfully on port {self.port}")
            except Exception as e:
                self.logger.error(f"Gateway2: Failed to start server on port {self.port}: {e}")
                logging.getLogger('errors').error(f"Gateway2: IoT device {self.device_id} failed to start server: {e}")
                self.server_socket = None

    def accept_connection(self) -> bool:
        with self.lock:
            if not self.server_socket:
                self.logger.debug("Gateway2: No server socket available, starting server")
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
                self.logger.info("Gateway2: Waiting for client connection...")
                self.server_socket.settimeout(5.0)
                self.client_socket, addr = self.server_socket.accept()
                self.client_socket.settimeout(None)
                self.connected = True
                self.logger.info(f"Gateway2: Client connected from {addr[0]}:{addr[1]}")
                return True
            except socket.timeout:
                self.logger.debug("Gateway2: Connection accept timeout")
                return False
            except Exception as e:
                self.logger.warning(f"Gateway2: Failed to accept connection: {e}")
                self.client_socket = None

    def is_connected(self) -> bool:
        with self.lock:
            return self.connected
        return False

    def disconnect(self) -> None:
        with self.lock:
            was_connected = self.connected
            self.connected = False

            if self.client_socket:
                try:
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except Exception as linger_err:
                    self.logger.debug(f"Gateway2: SO_LINGER set failed: {linger_err}")
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                    self.logger.info("Gateway2: Client connection closed properly (forced)")
                except Exception as e:
                    self.logger.warning(f"Gateway2: Error closing client socket: {e}")
                finally:
                    self.client_socket = None

            if was_connected:
                self.logger.info(f"Gateway2: Device {self.device_id} disconnected, ready for new connection")

    def shutdown_device(self) -> None:
        with self.lock:
            self.logger.info(f"Gateway2: Shutting down device {self.device_id}")
            
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
                    self.logger.info("Gateway2: Server socket closed")
                except Exception as e:
                    self.logger.warning(f"Gateway2: Error closing server socket: {e}")
                finally:
                    self.server_socket = None
                    
            self.connected = False

    def receive_data(self) -> Optional[List[str]]:
        import struct
        with self.lock:
            print(f"[GATEWAY2][RECEIVE] Starting receive_data for {self.device_id}, connected={self.connected}")
            if not self.connected or not self.client_socket:
                print(f"[GATEWAY2][RECEIVE] No active client connection for {self.device_id}")
                return None
            try:
                buffer = b""
                start_time = time.time()
                self.client_socket.settimeout(30.0)
                print(f"[GATEWAY2][RECEIVE] Entering receive loop for {self.device_id}")

                while True:
                    try:
                        data = self.client_socket.recv(8192)
                        print(f"[GATEWAY2][RECEIVE] recv called for {self.device_id}, got {len(data) if data else 'None'} bytes")
                        if data is None:
                            print(f"[GATEWAY2][RECEIVE] Data is None for {self.device_id}, breaking loop")
                            break
                        if not data:
                            print(f"[GATEWAY2][RECEIVE] Peer closed connection for {self.device_id}")
                            self.connected = False
                            try:
                                self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                            except Exception as linger_err:
                                print(f"[GATEWAY2][RECEIVE] SO_LINGER set failed: {linger_err}")
                            try:
                                self.client_socket.shutdown(socket.SHUT_RDWR)
                            except:
                                pass
                            try:
                                self.client_socket.close()
                                print(f"[GATEWAY2][RECEIVE] Client socket closed (forced) for {self.device_id}")
                            except Exception as e:
                                print(f"[GATEWAY2][RECEIVE] Error closing client socket: {e}")
                            self.client_socket = None
                            return None
                        buffer += data
                        if len(data) < 8192:
                            print(f"[GATEWAY2][RECEIVE] Less than buffer size received for {self.device_id}, breaking loop")
                            break
                    except socket.timeout:
                        print(f"[GATEWAY2][RECEIVE] Timeout for {self.device_id}")
                        self.connected = False
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[GATEWAY2][RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[GATEWAY2][RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e:
                            print(f"[GATEWAY2][RECEIVE] Error closing client socket: {e}")
                        self.client_socket = None
                        return None
                    except ConnectionResetError:
                        print(f"[GATEWAY2][RECEIVE] Connection reset by peer for {self.device_id}")
                        self.connected = False
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[GATEWAY2][RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[GATEWAY2][RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e:
                            print(f"[GATEWAY2][RECEIVE] Error closing client socket: {e}")
                        self.client_socket = None
                        return None
                    except Exception as e:
                        print(f"[GATEWAY2][RECEIVE] Socket error for {self.device_id}: {e}")
                        self.connected = False
                        try:
                            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                        except Exception as linger_err:
                            print(f"[GATEWAY2][RECEIVE] SO_LINGER set failed: {linger_err}")
                        try:
                            self.client_socket.shutdown(socket.SHUT_RDWR)
                        except:
                            pass
                        try:
                            self.client_socket.close()
                            print(f"[GATEWAY2][RECEIVE] Client socket closed (forced) for {self.device_id}")
                        except Exception as e2:
                            print(f"[GATEWAY2][RECEIVE] Error closing client socket: {e2}")
                        self.client_socket = None
                        return None

                receive_time = time.time() - start_time
                lines = buffer.decode('utf-8', errors='ignore').splitlines()
                valid_lines = [line for line in lines if line.strip()]
                print(f"[GATEWAY2][RECEIVE] Finished for {self.device_id}: {len(buffer)} bytes, {len(valid_lines)} valid lines, time={receive_time:.3f}s")
                if valid_lines:
                    print(f"[GATEWAY2][RECEIVE] Sample data for {self.device_id}: {valid_lines[0][:100]}{'...' if len(valid_lines[0]) > 100 else ''}")

                return valid_lines

            except Exception as e:
                print(f"[GATEWAY2][RECEIVE] Error receiving data for {self.device_id}: {e}")
                self.connected = False
                try:
                    self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except Exception as linger_err:
                    print(f"[GATEWAY2][RECEIVE] SO_LINGER set failed: {linger_err}")
                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.client_socket.close()
                    print(f"[GATEWAY2][RECEIVE] Client socket closed (forced) for {self.device_id}")
                except Exception as e2:
                    print(f"[GATEWAY2][RECEIVE] Error closing client socket: {e2}")
                self.client_socket = None
                return None
            finally:
                try:
                    if self.client_socket:
                        self.client_socket.settimeout(None)
                except:
                    pass


class IoTGateway2Driver:
    def __init__(self, db_config: Dict[str, str]):
        self.logger = logging.getLogger('iot_gateway2_driver')
        self.data_logger = logging.getLogger('gateway2_data_processing')
        
        self.logger.info("Gateway2: Initializing IoT Gateway 2 Driver")
        self.logger.info(f"Gateway2: Database config: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        
        self.db = DatabaseConnection(**db_config)
        self.device = None  # Single device for Gateway 2
        self.running = True
        self.device_thread = None
        
        # Statistics
        self.stats = {
            'start_time': time.time(),
            'total_messages_processed': 0,
            'successful_db_operations': 0,
            'failed_db_operations': 0,
            'device_reconnections': 0
        }
        # Event-driven DI/AI state tracking for AI7-AI12
        self.di_states = {}

    def setup_device(self):
        self.logger.info("Gateway2: Setting up IoT Gateway 2 device from database configuration")
        
        query = "SELECT iot_gateway2_ip, iot_gateway2_port FROM iot_gateway_connection ORDER BY id DESC LIMIT 1"
        
        try:
            result = self.db.execute_query(query)
            if not result:
                self.logger.error("Gateway2: No device configuration found in database")
                return

            row = result[0]
            self.logger.debug(f"Gateway2: Retrieved device configuration: {row}")

            if row["iot_gateway2_ip"] and row["iot_gateway2_port"]:
                self.device = IoTGateway2(
                    device_id="gateway_2", 
                    ip=row["iot_gateway2_ip"], 
                    port=int(row["iot_gateway2_port"]),
                    ai_range=(7, 12)  # AI7 to AI12 for Gateway 2
                )
                self.logger.info(f"Gateway2: Created device gateway_2: {row['iot_gateway2_ip']}:{row['iot_gateway2_port']}")
            else:
                self.logger.warning(f"Gateway2: Incomplete config - skipping (IP: {row['iot_gateway2_ip']}, Port: {row['iot_gateway2_port']})")
                    
            self.logger.info("Gateway2: Successfully configured IoT Gateway 2 device")
            
        except Exception as e:
            self.logger.error(f"Gateway2: Failed to setup device: {e}")
            logging.getLogger('errors').error(f"Gateway2: Device setup failed: {e}")

    def get_calibration_values(self):
        """Get calibration values for AI7-AI12 with comprehensive logging"""
        self.data_logger.debug("Gateway2: Retrieving calibration values from database")
        
        try:
            c_query = "SELECT * FROM c_values ORDER BY id DESC LIMIT 1"
            m_query = "SELECT * FROM m_values ORDER BY id DESC LIMIT 1"
            
            c_result = self.db.execute_query(c_query)
            m_result = self.db.execute_query(m_query)
            
            c_values = {}
            m_values = {}
            
            if c_result and len(c_result) > 0:
                self.data_logger.debug("Gateway2: Processing C values from database")
                for i in range(7, 13):  # AI7 to AI12 for Gateway 2
                    col_name = f"cValue{i}"
                    if col_name in c_result[0]:
                        c_values[f"AI{i}"] = float(c_result[0][col_name]) if c_result[0][col_name] is not None else 0.0
                    else:
                        c_values[f"AI{i}"] = 0.0
                        self.data_logger.warning(f"Gateway2: Missing column {col_name} in c_values table")
            else:
                self.data_logger.warning("Gateway2: No C values found in database, using defaults")
                c_values = {f"AI{i}": 0.0 for i in range(7, 13)}
            
            if m_result and len(m_result) > 0:
                self.data_logger.debug("Gateway2: Processing M values from database")
                for i in range(7, 13):  # AI7 to AI12 for Gateway 2
                    col_name = f"mValue{i}"
                    if col_name in m_result[0]:
                        m_values[f"AI{i}"] = float(m_result[0][col_name]) if m_result[0][col_name] is not None else 1.0
                    else:
                        m_values[f"AI{i}"] = 1.0
                        self.data_logger.warning(f"Gateway2: Missing column {col_name} in m_values table")
            else:
                self.data_logger.warning("Gateway2: No M values found in database, using defaults")
                m_values = {f"AI{i}": 1.0 for i in range(7, 13)}
            
            self.data_logger.info(f"Gateway2: Retrieved calibration values: {len(c_values)} C values, {len(m_values)} M values")
            return c_values, m_values
            
        except Exception as e:
            self.data_logger.error(f"Gateway2: Failed to get calibration values: {e}")
            logging.getLogger('errors').error(f"Gateway2: Calibration values retrieval failed: {e}")
            c_values = {f"AI{i}": 0.0 for i in range(7, 13)}
            m_values = {f"AI{i}": 1.0 for i in range(7, 13)}
            return c_values, m_values

    def get_production_status(self):
        """Get production status with comprehensive logging"""
        self.data_logger.debug("Gateway2: Retrieving production status from database")
        
        try:
            result = self.db.execute_query("SELECT prodstatus FROM myplclog ORDER BY id DESC LIMIT 1")            
            if result and len(result) > 0:
                prod_status = int(result[0]['prodstatus']) if result[0]['prodstatus'] is not None else 0
                self.data_logger.debug(f"Gateway2: Retrieved production status: {prod_status}")
                return prod_status
            else:
                self.data_logger.warning("Gateway2: No production status found in database, using default (0)")
                return 0
        except Exception as e:
            self.data_logger.error(f"Gateway2: Failed to get production status: {e}")
            logging.getLogger('errors').error(f"Gateway2: Production status retrieval failed: {e}")
            return 0

    def get_part_number_id(self):
        """Get part number ID with comprehensive logging"""
        self.data_logger.debug("Gateway2: Retrieving part number ID from database")
        
        try:
            result = self.db.execute_query("SELECT part_number_id FROM myplclog ORDER BY id DESC LIMIT 1")
            
            if result and len(result) > 0:
                part_id = int(result[0]['part_number_id']) if result[0]['part_number_id'] is not None else None
                self.data_logger.debug(f"Gateway2: Retrieved part number ID: {part_id}")
                return part_id
            else:
                self.data_logger.warning("Gateway2: No part number ID found in database")
                return None
        except Exception as e:
            self.data_logger.error(f"Gateway2: Failed to get part number ID: {e}")
            logging.getLogger('errors').error(f"Gateway2: Part number ID retrieval failed: {e}")
            return None

    def log_statistics(self):
        """Log current system statistics"""
        uptime = time.time() - self.stats['start_time']
        uptime_str = str(datetime.timedelta(seconds=int(uptime)))
        
        self.logger.info("Gateway2: === SYSTEM STATISTICS ===")
        self.logger.info(f"Gateway2: Uptime: {uptime_str}")
        self.logger.info(f"Gateway2: Total messages processed: {self.stats['total_messages_processed']}")
        self.logger.info(f"Gateway2: Successful DB operations: {self.stats['successful_db_operations']}")
        self.logger.info(f"Gateway2: Failed DB operations: {self.stats['failed_db_operations']}")
        self.logger.info(f"Gateway2: Device reconnections: {self.stats['device_reconnections']}")
        self.logger.info(f"Gateway2: Device connected: {self.device.is_connected() if self.device else False}")
        
        if uptime > 0:
            msg_per_hour = (self.stats['total_messages_processed'] / uptime) * 3600
            self.logger.info(f"Gateway2: Message rate: {msg_per_hour:.1f} messages/hour")

    def process_iot_data(self, device_id, data_str):
        """Process IoT data for AI7-AI12 with comprehensive logging and error handling"""
        self.data_logger.debug(f"Gateway2: Processing data from {device_id}: {data_str[:100]}{'...' if len(data_str) > 100 else ''}")
        
        try:
            self.stats['total_messages_processed'] += 1
            
            try:
                data = json.loads(data_str)
                self.data_logger.debug(f"Gateway2: Successfully parsed JSON data with {len(data)} keys")
            except json.JSONDecodeError as e:
                self.data_logger.error(f"Gateway2: Invalid JSON data from {device_id}: {e}")
                return

            part_id = self.get_part_number_id()
            if part_id is None:
                self.data_logger.error("Gateway2: Part number ID is None, cannot process data")
                self.stats['failed_db_operations'] += 1
                return

            c_vals, m_vals = self.get_calibration_values()
            prod_status = self.get_production_status()
            
            table = "leakapp_result_tbl" if prod_status == 0 else "foi_tbl"
            self.data_logger.debug(f"Gateway2: Using table: {table} (prod_status: {prod_status})")

            now = datetime.datetime.now()
            processed_count = 0
            skipped_count = 0

            # Event-driven DI/AI logic for DI7-DI12/AI7-AI12
            di_transitions = {}
            for k, v in data.items():
                if k.startswith("DI") and k in ["DI7", "DI8", "DI9", "DI10", "DI11", "DI12"]:
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
                
                if di_val == 1 and not state['active']:
                    state['active'] = True
                    state['start_time'] = time.time()
                    state['ai_window'] = []
                    state['first_check_done'] = False
                    state['first_check_time'] = state['start_time'] + 5
                    state['first_check_status'] = None
                    state['skip_second'] = False
                    state['second_window'] = []
                    self.data_logger.info(f"Gateway2: {di_name} became 1: starting 5s window for {ai_name} first value check")
                    
                if di_val == 1 and state['active']:
                    if ai_name in data:
                        raw_val = float(data[ai_name])
                        m_val = m_vals.get(ai_name, 1.0)
                        c_val = c_vals.get(ai_name, 0.0)
                        cal_val = m_val * raw_val + c_val
                        state['ai_window'].append((ai_name, cal_val, time.time()))
                        
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
                            self.data_logger.info(f"Gateway2: First 5s check for {ai_name}: highest={ai_val}, status={status}")
                
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
                            self.data_logger.info(f"Gateway2: Second cycle for {ai_name}: highest={ai_val}, status={status}")
                    state['active'] = False
                    state['start_time'] = None
                    state['ai_window'] = []
                    state['first_check_done'] = False
                    state['first_check_time'] = None
                    state['first_check_status'] = None
                    state['skip_second'] = False
                    state['second_window'] = []

            # Batch insert for AI values (AI7-AI12)
            ai_rows = []
            for k, v in data.items():
                self.data_logger.debug(f"Gateway2: Processing data point: {k} = {v}")
                if k.startswith("AI") and k in ["AI7", "AI8", "AI9", "AI10", "AI11", "AI12"]:
                    try:
                        raw_value = float(v)
                        if raw_value == -999 or raw_value > 6000:
                            self.data_logger.warning(f"Gateway2: Skipping AI {k} due to invalid value: {raw_value}")
                            skipped_count += 1
                            continue
                        m_val = m_vals.get(k, 1.0)
                        c_val = c_vals.get(k, 0.0)
                        cal_val = m_val * raw_value + c_val
                        self.data_logger.debug(f"Gateway2: AI {k}: raw={raw_value}, calibrated={cal_val} (m={m_val}, c={c_val})")
                        ai_rows.append((k, cal_val, 0, now, part_id))
                    except Exception as e:
                        self.data_logger.error(f"Gateway2: Exception processing AI {k}: {e}")
                        logging.getLogger('errors').error(f"Gateway2: AI processing error for {k}: {e}")
                        self.stats['failed_db_operations'] += 1
            
            if ai_rows:
                query = f"""
                    INSERT INTO {table} (filter_no, filter_values, batch_counter, date, part_number_id)
                    VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cursor = self.db.connection.cursor()
                    self.db.connection.autocommit = False
                    batch_size = 1000
                    for i in range(0, len(ai_rows), batch_size):
                        batch = ai_rows[i:i+batch_size]
                        cursor.executemany(query, batch)
                    self.db.connection.commit()
                    self.db.connection.autocommit = True
                    processed_count += len(ai_rows)
                    self.stats['successful_db_operations'] += len(ai_rows)
                    self.data_logger.debug(f"Gateway2: Batch inserted {len(ai_rows)} AI values")
                    cursor.close()
                except Exception as e:
                    self.data_logger.error(f"Gateway2: Batch insert failed for AI values: {e}")
                    self.stats['failed_db_operations'] += len(ai_rows)

            # Process DI values (DI7-DI12)
            for k, v in data.items():
                if k.startswith("DI") and k in ["DI7", "DI8", "DI9", "DI10", "DI11", "DI12"]:
                    try:
                        select_query = "SELECT di_value FROM di_values WHERE di_name=%s ORDER BY log_time DESC LIMIT 1"
                        prev_result = self.db.execute_query(select_query, (k,))
                        prev_value = int(float(prev_result[0]['di_value'])) if prev_result and prev_result[0]['di_value'] is not None else None
                        new_value = int(float(v))
                        
                        self.data_logger.debug(f"Gateway2: DI {k}: previous={prev_value}, new={new_value}")

                        if prev_value is not None:
                            if prev_value != new_value:
                                update_query = "UPDATE di_values SET di_value=%s, log_time=%s WHERE di_name=%s"
                                update_params = (new_value, now, k)
                                update_count = self.db.execute_query(update_query, update_params)

                                if update_count is None or update_count == 0:
                                    self.data_logger.warning(f"Gateway2: DI {k} not found in database")
                                else:
                                    self.data_logger.debug(f"Gateway2: Successfully updated DI {k}")
                                    processed_count += 1
                                    self.stats['successful_db_operations'] += 1
                            else:
                                self.data_logger.debug(f"Gateway2: DI {k}: Value unchanged, skipping update")
                                skipped_count += 1

                    except Exception as e:
                        self.data_logger.error(f"Gateway2: Exception processing DI {k}: {e}")
                        logging.getLogger('errors').error(f"Gateway2: DI processing error for {k}: {e}")
                        self.stats['failed_db_operations'] += 1

            self.data_logger.info(f"Gateway2: Data processing complete: {processed_count} processed, {skipped_count} skipped")

        except Exception as e:
            self.data_logger.error(f"Gateway2: Critical exception in data processing: {e}")
            logging.getLogger('errors').error(f"Gateway2: Critical data processing error: {e}")
            self.stats['failed_db_operations'] += 1

    def device_worker(self):
        """Device worker thread for Gateway 2"""
        worker_logger = logging.getLogger('iot_gateway2.worker')
        device = self.device
        reconnect_delay = 1
        max_reconnect_delay = 2

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
                        print(f"[GATEWAY2][ERROR] DB insert error: {e}")
                except queue.Empty:
                    continue

        inserter_thread = Thread(target=db_inserter, daemon=True)
        inserter_thread.start()

        worker_logger.info("Gateway2: Starting device worker thread")
        device.start_server()

        while self.running:
            try:
                if not device.is_connected():
                    worker_logger.debug("Gateway2: Device not connected, attempting connection")
                    if device.accept_connection():
                        reconnect_delay = 1
                        self.stats['device_reconnections'] += 1
                        worker_logger.info("Gateway2: Successfully connected")
                        try:
                            select_query = "SELECT server_connection_2 FROM myplclog WHERE id = (SELECT MAX(id) FROM myplclog)"
                            result = self.db.execute_query(select_query)
                            current_val = result[0]['server_connection_2'] if result and 'server_connection_2' in result[0] else None
                            if current_val != 1:
                                update_query = "UPDATE myplclog SET server_connection_2 = 1 WHERE id = (SELECT MAX(id) FROM myplclog)"
                                affected = self.db.execute_query(update_query)
                                worker_logger.debug(f"Gateway2: Updated server_connection_2 to connected, rows affected: {affected}")
                                self.stats['successful_db_operations'] += 1
                            else:
                                worker_logger.debug("Gateway2: server_connection_2 already set to 1, no update needed")
                        except Exception as e:
                            worker_logger.error(f"Gateway2: Failed to update connection status: {e}")
                            self.stats['failed_db_operations'] += 1
                    else:
                        worker_logger.warning(f"Gateway2: Connection attempt failed, retrying in {reconnect_delay}s")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay + 1, max_reconnect_delay)
                        continue

                data_list = device.receive_data()
                if data_list:
                    for data_str in data_list:
                        if data_str.strip():
                            data_queue.put(("gateway_2", data_str.strip()))
                else:
                    if not device.is_connected():
                        worker_logger.warning("Gateway2: Device disconnected by peer, attempting immediate reconnect")
                        device.disconnect()
                        try:
                            select_query = "SELECT server_connection_2 FROM myplclog WHERE id = (SELECT MAX(id) FROM myplclog)"
                            result = self.db.execute_query(select_query)
                            current_val = result[0]['server_connection_2'] if result and 'server_connection_2' in result[0] else None
                            if current_val != 0:
                                update_query = "UPDATE myplclog SET server_connection_2 = 0 WHERE id = (SELECT MAX(id) FROM myplclog)"
                                affected = self.db.execute_query(update_query)
                                worker_logger.debug(f"Gateway2: Updated server_connection_2 to disconnected, rows affected: {affected}")
                                self.stats['successful_db_operations'] += 1
                            else:
                                worker_logger.debug("Gateway2: server_connection_2 already set to 0, no update needed")
                        except Exception as e:
                            worker_logger.error(f"Gateway2: Failed to update disconnection status: {e}")
                            self.stats['failed_db_operations'] += 1
                        time.sleep(1)
                    else:
                        worker_logger.debug("Gateway2: No data received, holding connection...")
                        time.sleep(0.5)
            except Exception as e:
                worker_logger.error(f"Gateway2: Unexpected error in device worker: {e}")
                logging.getLogger('errors').error(f"Gateway2: Device worker unexpected error: {e}")
                time.sleep(2)
        worker_logger.info("Gateway2: Device worker thread shutting down")

    def start(self):
        """Start the IoT Gateway 2 Driver system"""
        self.logger.info("Gateway2: Starting IoT Gateway 2 Driver system")
        
        def stats_logger():
            while self.running:
                time.sleep(300)  # 5 minutes
                if self.running:
                    self.log_statistics()
        
        stats_thread = Thread(target=stats_logger, daemon=True)
        stats_thread.start()
        
        self.setup_device()
        
        if not self.device:
            self.logger.error("Gateway2: No device configured, cannot start system")
            return
        
        self.logger.info("Gateway2: Starting device worker thread")
        
        self.device_thread = Thread(target=self.device_worker)
        self.device_thread.daemon = True
        self.device_thread.start()
        
        self.logger.info("Gateway2: IoT Gateway 1 Driver system started successfully")

    def stop(self):
        """Stop the IoT Gateway 2 Driver system"""
        self.logger.info("Gateway2: Shutting down IoT Gateway 1 Driver system")
        
        self.running = False
        
        if self.device_thread:
            self.logger.info("Gateway2: Waiting for worker thread to complete...")
            self.device_thread.join(timeout=5)
            if self.device_thread.is_alive():
                self.logger.warning("Gateway2: Thread did not shut down gracefully")
            else:
                self.logger.debug("Gateway2: Thread shut down successfully")
        
        if self.device:
            self.logger.info("Gateway2: Disconnecting device...")
            self.device.shutdown_device()
            self.logger.debug("Gateway2: Device shutdown complete")
        
        self.logger.info("Gateway2: Closing database connection...")
        self.db.close()
        
        self.log_statistics()
        self.logger.info("Gateway2: IoT Gateway 1 Driver system shutdown complete")


if __name__ == "__main__":
    logger = logging.getLogger('Gateway2_main')
    
    config = {
        'host': 'localhost', 
        'user': 'root', 
        'password': '', 
        'database': 'semileak',
        'port': 3306
    }
    
    logger.info("Gateway2: Starting IoT Gateway 2 Driver application")
    logger.info(f"Gateway2: Configuration: {config['host']}:{config['port']}/{config['database']}")
    
    driver = IoTGateway2Driver(config)
    
    try:
        driver.start()
        logger.info("Gateway2: System running... Press Ctrl+C to stop")
        
        while True:
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Gateway2: Received shutdown signal")
    except Exception as e:
        logger.error(f"Gateway2: Unexpected error in main loop: {e}")
        logging.getLogger('errors').error(f"Gateway2: Main loop error: {e}")
    finally:
        driver.stop()
        logger.info("Gateway2: Application shutdown complete")