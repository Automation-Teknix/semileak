#!/usr/bin/env python3
"""
Gateway 2 IoT Data Processor - Handles AI7-AI12 and DI7-DI12
Optimized for high-performance processing of 10,000+ records
"""

import socket
import struct
import time
import json
import datetime
import mysql.connector
from mysql.connector import Error as MySQLError
from threading import Thread, RLock as Lock
import logging
import sys

class OptimizedDatabaseConnection:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.logger = logging.getLogger('gateway2.database')
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.lock = Lock()
        
        # Performance optimizations
        self.connection_config = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'port': self.port,
            'autocommit': False,  # Manual commit for batch operations
            'charset': 'utf8mb4',
            'connection_timeout': 30,
            'buffered': True,
            'use_unicode': True,
            'sql_mode': 'TRADITIONAL',
            'raise_on_warnings': False
        }
        
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.connection_config)
            # Optimize connection for bulk operations
            cursor = self.connection.cursor()
            cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
            cursor.execute("SET SESSION max_heap_table_size = 134217728")  # 128MB
            cursor.execute("SET SESSION tmp_table_size = 134217728")  # 128MB
            cursor.close()
            self.logger.info("Gateway 2 database connection established")
        except MySQLError as e:
            self.logger.error(f"Database connection failed: {e}")
            self.connection = None

    def execute_batch(self, query: str, data_list: list):
        """Optimized batch execution"""
        with self.lock:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            try:
                cursor = self.connection.cursor()
                cursor.executemany(query, data_list)
                self.connection.commit()
                affected = cursor.rowcount
                cursor.close()
                return affected
            except Exception as e:
                self.logger.error(f"Batch execution failed: {e}")
                self.connection.rollback()
                raise

    def execute_single(self, query: str, params: tuple = None):
        """Single query execution with auto-retry"""
        with self.lock:
            if not self.connection or not self.connection.is_connected():
                self.connect()
                
            try:
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute(query, params)
                
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    cursor.close()
                    return result
                else:
                    affected = cursor.rowcount
                    self.connection.commit()
                    cursor.close()
                    return affected
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}")
                self.connection.rollback()
                raise

class Gateway2Processor:
    def __init__(self, db_config: dict):
        self.logger = logging.getLogger('gateway2.processor')
        self.db = OptimizedDatabaseConnection(**db_config)
        
        # Gateway 2 specific configuration
        self.device_id = "gateway_2"
        self.ai_range = list(range(7, 13))  # AI7-AI12
        self.di_range = list(range(7, 13))  # DI7-DI12
        
        # Socket configuration
        self.ip = "0.0.0.0"
        self.port = None  # Will be loaded from database
        self.server_socket = None
        self.client_socket = None
        self.connected = False
        
        # Performance caching
        self.calibration_cache = {
            'c_vals': {},
            'm_vals': {},
            'last_update': 0,
            'cache_duration': 60  # Cache for 60 seconds
        }
        
        self.part_number_cache = {
            'part_id': None,
            'prod_status': 0,
            'last_update': 0,
            'cache_duration': 30  # Cache for 30 seconds
        }
        
        # Event-driven DI/AI state tracking (only for Gateway 2 DIs)
        self.di_states = {}
        for i in self.di_range:
            self.di_states[f"DI{i}"] = {
                'active': False,
                'start_time': None,
                'ai_window': [],
                'first_check_done': False,
                'first_check_time': None,
                'first_check_status': None,
                'skip_second': False
            }
        
        # Statistics
        self.stats = {
            'start_time': time.time(),
            'messages_processed': 0,
            'records_inserted': 0,
            'db_operations': 0,
            'errors': 0
        }
        
        self.running = True
        self.load_gateway_config()

    def load_gateway_config(self):
        """Load Gateway 2 specific configuration from database"""
        try:
            query = "SELECT iot_gateway2_ip, iot_gateway2_port FROM iot_gateway_connection ORDER BY id DESC LIMIT 1"
            result = self.db.execute_single(query)
            
            if result:
                self.ip = result[0]["iot_gateway2_ip"] or "0.0.0.0"
                self.port = int(result[0]["iot_gateway2_port"]) if result[0]["iot_gateway2_port"] else 8002
                self.logger.info(f"Gateway 2 configured: {self.ip}:{self.port}")
            else:
                self.port = 8002  # Default port
                self.logger.warning("No gateway config found, using defaults")
                
        except Exception as e:
            self.logger.error(f"Failed to load gateway config: {e}")
            self.port = 8002

    def get_cached_calibration_values(self):
        """Get calibration values with caching for performance"""
        current_time = time.time()
        
        if current_time - self.calibration_cache['last_update'] > self.calibration_cache['cache_duration']:
            try:
                # Get only AI7-AI12 calibration values
                c_query = "SELECT cValue7, cValue8, cValue9, cValue10, cValue11, cValue12 FROM c_values ORDER BY id DESC LIMIT 1"
                m_query = "SELECT mValue7, mValue8, mValue9, mValue10, mValue11, mValue12 FROM m_values ORDER BY id DESC LIMIT 1"
                
                c_result = self.db.execute_single(c_query)
                m_result = self.db.execute_single(m_query)
                
                c_vals = {}
                m_vals = {}
                
                # Process C values for AI7-AI12 only
                if c_result:
                    for i in self.ai_range:
                        col_name = f"cValue{i}"
                        c_vals[f"AI{i}"] = float(c_result[0][col_name]) if c_result[0].get(col_name) is not None else 0.0
                else:
                    c_vals = {f"AI{i}": 0.0 for i in self.ai_range}
                
                # Process M values for AI7-AI12 only
                if m_result:
                    for i in self.ai_range:
                        col_name = f"mValue{i}"
                        m_vals[f"AI{i}"] = float(m_result[0][col_name]) if m_result[0].get(col_name) is not None else 1.0
                else:
                    m_vals = {f"AI{i}": 1.0 for i in self.ai_range}
                
                self.calibration_cache['c_vals'] = c_vals
                self.calibration_cache['m_vals'] = m_vals
                self.calibration_cache['last_update'] = current_time
                
            except Exception as e:
                self.logger.error(f"Failed to get calibration values: {e}")
                # Use defaults if database fails
                self.calibration_cache['c_vals'] = {f"AI{i}": 0.0 for i in self.ai_range}
                self.calibration_cache['m_vals'] = {f"AI{i}": 1.0 for i in self.ai_range}
        
        return self.calibration_cache['c_vals'], self.calibration_cache['m_vals']

    def get_cached_production_data(self):
        """Get production status and part number with caching"""
        current_time = time.time()
        
        if current_time - self.part_number_cache['last_update'] > self.part_number_cache['cache_duration']:
            try:
                query = "SELECT prodstatus, part_number_id FROM myplclog ORDER BY id DESC LIMIT 1"
                result = self.db.execute_single(query)
                
                if result:
                    self.part_number_cache['prod_status'] = int(result[0]['prodstatus']) if result[0]['prodstatus'] is not None else 0
                    self.part_number_cache['part_id'] = int(result[0]['part_number_id']) if result[0]['part_number_id'] is not None else None
                else:
                    self.part_number_cache['prod_status'] = 0
                    self.part_number_cache['part_id'] = None
                    
                self.part_number_cache['last_update'] = current_time
                
            except Exception as e:
                self.logger.error(f"Failed to get production data: {e}")
                self.part_number_cache['prod_status'] = 0
                self.part_number_cache['part_id'] = None
        
        return self.part_number_cache['prod_status'], self.part_number_cache['part_id']

    def setup_socket(self):
        """Setup socket server for Gateway 2"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen(1)
            self.logger.info(f"Gateway 2 server listening on {self.ip}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to setup socket: {e}")
            raise

    def accept_connection(self):
        """Accept client connection with timeout"""
        try:
            if self.client_socket:
                try:
                    self.client_socket.close()
                except:
                    pass
                self.client_socket = None
                self.connected = False

            self.server_socket.settimeout(5.0)
            self.client_socket, addr = self.server_socket.accept()
            self.client_socket.settimeout(None)
            self.connected = True
            self.logger.info(f"Gateway 2 client connected from {addr}")
            return True
        except socket.timeout:
            return False
        except Exception as e:
            self.logger.warning(f"Connection accept failed: {e}")
            return False

    def receive_data(self):
        """Optimized data reception"""
        if not self.connected or not self.client_socket:
            return None
            
        try:
            buffer = b""
            self.client_socket.settimeout(30.0)
            
            while True:
                data = self.client_socket.recv(8192)
                if not data:
                    self.connected = False
                    return None
                    
                buffer += data
                if len(data) < 8192:
                    break
            
            lines = buffer.decode('utf-8', errors='ignore').splitlines()
            valid_lines = [line.strip() for line in lines if line.strip()]
            return valid_lines
            
        except Exception as e:
            self.logger.error(f"Data reception failed: {e}")
            self.connected = False
            return None

    def process_batch_data(self, data_batch):
        """Process multiple JSON messages efficiently"""
        if not data_batch:
            return
            
        # Get cached values once for entire batch
        c_vals, m_vals = self.get_cached_calibration_values()
        prod_status, part_id = self.get_cached_production_data()
        
        if part_id is None:
            self.logger.error("No part number ID available")
            return
        
        table = "leakapp_result_tbl" if prod_status == 0 else "foi_tbl"
        now = datetime.datetime.now()
        
        # Batch collections
        ai_batch = []
        di_updates = []
        test_inserts = []
        report_inserts = []
        
        for data_str in data_batch:
            try:
                data = json.loads(data_str)
                self.stats['messages_processed'] += 1
                
                # Process AI values (AI7-AI12 only)
                for i in self.ai_range:
                    ai_key = f"AI{i}"
                    if ai_key in data:
                        try:
                            raw_value = float(data[ai_key])
                            if raw_value != -999 and raw_value <= 6000:
                                m_val = m_vals.get(ai_key, 1.0)
                                c_val = c_vals.get(ai_key, 0.0)
                                cal_val = m_val * raw_value + c_val
                                ai_batch.append((ai_key, cal_val, 0, now, part_id))
                        except (ValueError, TypeError):
                            continue
                
                # Process DI values and event logic (DI7-DI12 only)
                for i in self.di_range:
                    di_key = f"DI{i}"
                    ai_key = f"AI{i}"
                    
                    if di_key in data:
                        try:
                            di_val = int(float(data[di_key]))
                            state = self.di_states[di_key]
                            
                            # DI rising edge (0->1)
                            if di_val == 1 and not state['active']:
                                state['active'] = True
                                state['start_time'] = time.time()
                                state['ai_window'] = []
                                state['first_check_done'] = False
                                state['first_check_time'] = state['start_time'] + 5
                                state['skip_second'] = False
                                
                            # DI active - collect AI data
                            if di_val == 1 and state['active'] and ai_key in data:
                                try:
                                    raw_val = float(data[ai_key])
                                    if raw_val != -999 and raw_val <= 5000:
                                        m_val = m_vals.get(ai_key, 1.0)
                                        c_val = c_vals.get(ai_key, 0.0)
                                        cal_val = m_val * raw_val + c_val
                                        state['ai_window'].append((ai_key, cal_val, time.time()))
                                except (ValueError, TypeError):
                                    pass
                            
                            # First 5s check
                            if (di_val == 1 and state['active'] and not state['first_check_done'] and 
                                time.time() >= state['first_check_time']):
                                
                                window_vals = [x for x in state['ai_window'] 
                                             if x[2] - state['start_time'] <= 5 and x[1] != -999 and x[1] <= 5000]
                                
                                if window_vals:
                                    highest = max(window_vals, key=lambda x: x[1])
                                    ai_name, ai_val, ts = highest
                                    
                                    # Get setpoint1 (cache this too if needed)
                                    setpoint_query = "SELECT setpoint1 FROM leakapp_masterdata WHERE part_number = %s"
                                    setpoint_res = self.db.execute_single(setpoint_query, (part_id,))
                                    setpoint1 = setpoint_res[0]['setpoint1'] if setpoint_res and 'setpoint1' in setpoint_res[0] else 70
                                    
                                    status = "OK" if ai_val <= setpoint1 else "NOK"
                                    state['first_check_status'] = status
                                    state['first_check_done'] = True
                                    
                                    if status == "NOK":
                                        state['skip_second'] = True
                                    
                                    # Add to batch inserts
                                    test_inserts.append((ai_name, ai_val, now, ai_val, part_id, status, 1))
                                    report_inserts.append((ai_name, ai_val, now, ai_val, part_id, status, 1))
                            
                            # DI falling edge (1->0)
                            if di_val == 0 and state['active']:
                                if not state['skip_second']:
                                    window_vals = [x for x in state['ai_window'] 
                                                 if x[2] - state['start_time'] > 5 and x[1] != -999 and x[1] <= 5000]
                                    
                                    if window_vals:
                                        highest = max(window_vals, key=lambda x: x[1])
                                        ai_name, ai_val, ts = highest
                                        
                                        setpoint_query = "SELECT setpoint2 FROM leakapp_masterdata WHERE part_number = %s"
                                        setpoint_res = self.db.execute_single(setpoint_query, (part_id,))
                                        setpoint2 = setpoint_res[0]['setpoint2'] if setpoint_res and 'setpoint2' in setpoint_res[0] else 8
                                        
                                        status = "OK" if ai_val < setpoint2 else "NOK"
                                        
                                        test_inserts.append((ai_name, ai_val, now, ai_val, part_id, status, 1))
                                        report_inserts.append((ai_name, ai_val, now, ai_val, part_id, status, 1))
                                
                                # Reset state
                                state['active'] = False
                                state['start_time'] = None
                                state['ai_window'] = []
                                state['first_check_done'] = False
                                state['skip_second'] = False
                            
                            # Add DI update to batch
                            di_updates.append((di_val, now, di_key))
                            
                        except (ValueError, TypeError):
                            continue
                
            except json.JSONDecodeError:
                continue
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                self.stats['errors'] += 1
                continue
        
        # Execute batch operations
        try:
            if ai_batch:
                ai_query = f"INSERT INTO {table} (filter_no, filter_values, batch_counter, date, part_number_id) VALUES (%s, %s, %s, %s, %s)"
                affected = self.db.execute_batch(ai_query, ai_batch)
                self.stats['records_inserted'] += affected
                self.stats['db_operations'] += 1
            
            if di_updates:
                di_query = "UPDATE di_values SET di_value=%s, log_time=%s WHERE di_name=%s"
                self.db.execute_batch(di_query, di_updates)
                self.stats['db_operations'] += 1
            
            if test_inserts:
                # Handle upsert for test table
                for test_data in test_inserts:
                    check_query = "SELECT COUNT(*) as cnt FROM leakapp_test WHERE filter_no = %s"
                    check_res = self.db.execute_single(check_query, (test_data[0],))
                    
                    if check_res and check_res[0]['cnt'] > 0:
                        update_query = "UPDATE leakapp_test SET filter_values=%s, highest_value=%s, date=%s, part_number_id=%s, status=%s, shift_id=%s WHERE filter_no=%s"
                        self.db.execute_single(update_query, test_data[1:] + (test_data[0],))
                    else:
                        insert_query = "INSERT INTO leakapp_test (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        self.db.execute_single(insert_query, test_data)
                
                self.stats['db_operations'] += len(test_inserts)
            
            if report_inserts:
                report_query = "INSERT INTO leakapp_show_report (filter_no, filter_values, date, highest_value, part_number_id, status, shift_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                self.db.execute_batch(report_query, report_inserts)
                self.stats['db_operations'] += 1
            
        except Exception as e:
            self.logger.error(f"Batch database operation failed: {e}")
            self.stats['errors'] += 1

    def update_connection_status(self, status):
        """Update connection status in myplclog"""
        try:
            # Check current value first
            select_query = "SELECT server_connection_2 FROM myplclog ORDER BY id DESC LIMIT 1"
            result = self.db.execute_single(select_query)
            
            current_val = result[0]['server_connection_2'] if result and 'server_connection_2' in result[0] else None
            
            if current_val != status:
                update_query = "UPDATE myplclog SET server_connection_2 = %s WHERE id = (SELECT * FROM (SELECT MAX(id) FROM myplclog) AS temp)"
                self.db.execute_single(update_query, (status,))
                self.logger.debug(f"Updated connection status to {status}")
        except Exception as e:
            self.logger.error(f"Failed to update connection status: {e}")

    def log_stats(self):
        """Log performance statistics"""
        uptime = time.time() - self.stats['start_time']
        rate = self.stats['messages_processed'] / uptime if uptime > 0 else 0
        
        self.logger.info(f"Gateway 2 Stats - Messages: {self.stats['messages_processed']}, "
                        f"Records: {self.stats['records_inserted']}, Rate: {rate:.1f}/sec, "
                        f"DB Ops: {self.stats['db_operations']}, Errors: {self.stats['errors']}")

    def run(self):
        """Main processing loop"""
        self.logger.info("Starting Gateway 2 Processor")
        
        self.setup_socket()
        
        # Stats logging thread
        def stats_thread():
            while self.running:
                time.sleep(300)  # Log every 5 minutes
                if self.running:
                    self.log_stats()
        
        stats_t = Thread(target=stats_thread, daemon=True)
        stats_t.start()
        
        reconnect_delay = 1
        
        while self.running:
            try:
                if not self.connected:
                    if self.accept_connection():
                        self.update_connection_status(1)
                        reconnect_delay = 1
                    else:
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay + 1, 5)
                        continue
                
                # Receive data
                data_list = self.receive_data()
                
                if data_list:
                    self.process_batch_data(data_list)
                else:
                    if not self.connected:
                        self.update_connection_status(0)
                        self.logger.warning("Gateway 2 disconnected")
                        time.sleep(1)
                
            except KeyboardInterrupt:
                self.logger.info("Shutdown signal received")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in main loop: {e}")
                self.stats['errors'] += 1
                time.sleep(2)
        
        self.shutdown()

    def shutdown(self):
        """Clean shutdown"""
        self.logger.info("Shutting down Gateway 2 Processor")
        self.running = False
        
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        self.log_stats()
        self.logger.info("Gateway 2 Processor shutdown complete")

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'gateway2_{datetime.datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'semileak',
        'port': 3306
    }
    
    processor = Gateway2Processor(db_config)
    
    try:
        processor.run()
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        processor.shutdown()