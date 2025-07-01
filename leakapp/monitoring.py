import mysql.connector
from mysql.connector import pooling
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict

# Set up logging with more detailed formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("di_monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'leakapp',
    'pool_name': 'leakapp_pool',
    'pool_size': 5  # Maintain 5 connections in the pool
}

class DIMonitoringService:
    def __init__(self, db_config):
        """Initialize the DI monitoring service with a connection pool"""
        self.db_config = db_config
        self.last_check_time = datetime.now() - timedelta(minutes=5)  # Start with a small look-back period
        self.persistent_connection = None
        # Set up the connection pool
        try:
            self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            logger.info(f"Connection pool created with {db_config['pool_size']} connections")
            print(f"Connection pool created with {db_config['pool_size']} connections")
        except mysql.connector.Error as err:
            logger.error(f"Failed to create connection pool: {err}")
            print(f"Failed to create connection pool: {err}")
            raise
        
        # Maintain a persistent connection for health checks
        self.persistent_connection = self.get_connection_from_pool()
        if not self.persistent_connection:
            raise Exception("Failed to create a persistent database connection.")
        
        # Track DI changes that need processing
        self.pending_di_changes = defaultdict(lambda: {
            "timestamp": None, 
            "processed_first_check": False,
            "processed_second_check": False,
            "monitoring_active": False
        })
        
        logger.info("DI Monitoring Service initialized")
        print("DI Monitoring Service initialized")
        
    def get_connection_from_pool(self):
        """Get a connection from the pool"""
        try:
            connection = self.cnx_pool.get_connection()
            logger.debug("Obtained connection from pool")
            return connection
        except mysql.connector.Error as err:
            logger.error(f"Failed to get connection from pool: {err}")
            print(f"Failed to get connection from pool: {err}")
            return None
            
    def check_connection_health(self):
        """Check if the persistent connection is still alive, reconnect if needed"""
        try:
            # Simple query to check connection
            if self.persistent_connection and self.persistent_connection.is_connected():
                cursor = self.persistent_connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                logger.debug("Persistent connection is healthy")
                return True
            else:
                logger.warning("Persistent connection lost, reconnecting...")
                print("Persistent connection lost, reconnecting...")
                self.persistent_connection = self.persistent_connection

                return self.persistent_connection is not None
        except mysql.connector.Error as err:
            logger.error(f"Connection health check failed: {err}")
            print(f"Connection health check failed: {err}")
            # Try to reconnect
            self.persistent_connection = self.persistent_connection

            return self.persistent_connection is not None
    def initialize_di_entries(self):
        """Ensure all DI entries from DI1 to DI16 exist in the database"""
        connection = self.persistent_connection

        if not connection:
            return False
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # First check which DIs already exist
            check_query = """
            SELECT DISTINCT di_name 
            FROM di_values 
            WHERE di_name BETWEEN 'DI1' AND 'DI16'
            """
            
            cursor.execute(check_query)
            existing_dis = {row['di_name'] for row in cursor.fetchall()}
            
            # Identify missing DIs
            all_dis = {f'DI{i}' for i in range(1, 17)}
            missing_dis = all_dis - existing_dis
            
            if missing_dis:
                logger.warning(f"Found {len(missing_dis)} missing DIs in database: {', '.join(sorted(missing_dis))}")
                print(f"Found {len(missing_dis)} missing DIs in database: {', '.join(sorted(missing_dis))}")
                
                # Initialize missing DIs with default value 0
                current_time = datetime.now()
                
                for di_name in missing_dis:
                    insert_query = """
                    INSERT INTO di_values (di_name, di_value, log_time)
                    VALUES (%s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (di_name, 0, current_time))
                    logger.info(f"Created initial record for missing {di_name} with value 0")
                    print(f"Created initial record for missing {di_name} with value 0")
                    
                connection.commit()
                return True
            else:
                logger.info("All DIs (DI1-DI16) already exist in database")
                print("All DIs (DI1-DI16) already exist in database")
                return False
                
        except mysql.connector.Error as err:
            connection.rollback()
            logger.error(f"Error initializing DI entries: {err}")
            print(f"Error initializing DI entries: {err}")
            return False
        finally:
            cursor.close()
            connection.close()
    def get_all_di_statuses(self):
        """Get current status of all DIs to detect changes"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Get status of all DIs
            query = """
            SELECT DISTINCT di_name, 
                   (SELECT di_value FROM di_values AS dv2 
                    WHERE dv2.di_name = dv1.di_name 
                    ORDER BY log_time DESC LIMIT 1) as current_value,
                   (SELECT log_time FROM di_values AS dv2 
                    WHERE dv2.di_name = dv1.di_name 
                    ORDER BY log_time DESC LIMIT 1) as last_update
            FROM di_values AS dv1
            WHERE di_name BETWEEN 'DI1' AND 'DI16'
            """
            
            cursor.execute(query)
            statuses = cursor.fetchall()
            
            logger.info(f"Retrieved current status for {len(statuses)} DI inputs")
            print(f"Retrieved current status for {len(statuses)} DI inputs")
                
            return statuses
        except mysql.connector.Error as err:
            logger.error(f"Error fetching DI statuses: {err}")
            print(f"Error fetching DI statuses: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool
    
    def get_di_changes(self):
        """Enhanced approach to fetch DI value changes from 0 to 1 since last check time"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            formatted_last_check = self.last_check_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Improved query to detect 0->1 transitions, focusing specifically on problem DIs
            query = """
            SELECT di.di_name, di.di_value, di.log_time
            FROM di_values di
            WHERE di.log_time > %s
            AND di.di_name BETWEEN 'DI1' AND 'DI16'
            AND di.di_value = 1
            AND (
                di.di_name BETWEEN 'DI2' AND 'DI9'  -- Focus on problem DIs
                OR di.di_name = 'DI1'
                OR di.di_name BETWEEN 'DI10' AND 'DI16'
            )
            """
            
            cursor.execute(query, (formatted_last_check,))
            active_di_values = cursor.fetchall()
            
            # Process each active DI value found
            changes = []
            for val in active_di_values:
                di_name = val['di_name']
                log_time = val['log_time']
                
                # Check if we were already at '1' for this DI
                prev_value = self.get_historical_di_state(di_name, log_time)
                
                # If previous value was 0 or nonexistent, we consider this a true 0->1 transition
                if prev_value is None or prev_value == 0:
                    changes.append(val)
                    logger.info(f"DETECTED TRANSITION: {di_name} changed to 1 at {log_time}")
                    print(f"DETECTED TRANSITION: {di_name} changed to 1 at {log_time}")
            
            # Update last check time
            self.last_check_time = datetime.now()
                
            return changes
        except mysql.connector.Error as err:
            logger.error(f"Error fetching DI changes: {err}")
            print(f"Error fetching DI changes: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def check_problem_dis(self):
        """Check all DIs (DI1-DI16) for active status - modified to check all DIs"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Get a list of all DIs that should exist
            all_dis = [f'DI{i}' for i in range(1, 17)]
            
            # Modified to check ALL DIs by name
            query = """
            SELECT di_name, di_value, MAX(log_time) as latest_time
            FROM di_values
            WHERE di_name = %s
            GROUP BY di_name, di_value
            """
            
            results = []
            missing_dis = []
            
            # Check each DI individually
            for di_name in all_dis:
                cursor.execute(query, (di_name,))
                di_data = cursor.fetchone()
                
                if di_data:
                    di_value = di_data['di_value']
                    log_time = di_data['latest_time']
                    
                    logger.info(f"DI check: {di_name} current value: {di_value} at {log_time}")
                    print(f"DI check: {di_name} current value: {di_value} at {log_time}")
                    
                    # Process if value is 1 and not already monitoring
                    if di_value == 1 and not self.pending_di_changes[di_name]["monitoring_active"]:
                        logger.info(f"SPECIAL CHECK: {di_name} has value 1 at {log_time}")
                        print(f"SPECIAL CHECK: {di_name} has value 1 at {log_time}")
                        self.process_di_change(di_name, log_time)
                        
                    results.append(di_data)
                else:
                    missing_dis.append(di_name)
                    logger.warning(f"DI check: {di_name} NOT FOUND in database")
                    print(f"DI check: {di_name} NOT FOUND in database")
            
            if missing_dis:
                logger.warning(f"Missing DIs: {', '.join(missing_dis)}")
                print(f"Missing DIs: {', '.join(missing_dis)}")
                
            return results
        except mysql.connector.Error as err:
            logger.error(f"Error in problem DI check: {err}")
            print(f"Error in problem DI check: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def get_ai_values_by_time(self, filter_no, start_time, end_time):
        """Get AI values within a specified time range"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
            SELECT filter_no, filter_values, date, part_number_id, shift_id
            FROM leakapp_result_tbl
            WHERE filter_no = %s
            AND date >= %s
            AND date <= %s
            ORDER BY date
            """
            
            cursor.execute(query, (filter_no, start_time, end_time))
            values = cursor.fetchall()
            
            count_available = len(values)
            if count_available > 0:
                logger.info(f"Found {count_available} values for {filter_no} between {start_time} and {end_time}")
                print(f"Found {count_available} values for {filter_no} between {start_time} and {end_time}")
                
                # Print highest value
                if values:
                    highest_value = max(values, key=lambda x: x['filter_values'])
                    logger.info(f"Highest value in time range: {highest_value['filter_values']} at {highest_value['date']}")
                    print(f"Highest value in time range: {highest_value['filter_values']} at {highest_value['date']}")
            else:
                logger.info(f"No values found for {filter_no} in time range")
                print(f"No values found for {filter_no} in time range")
                
            return values
        except mysql.connector.Error as err:
            logger.error(f"Error fetching AI values by time: {err}")
            print(f"Error fetching AI values by time: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def get_current_shift(self, current_time):
        """Determine the current shift based on time"""
        connection = self.persistent_connection

        if not connection:
            return 1  # Default to first shift
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Format current time to match database time format (HH:MM:SS)
            current_time_str = current_time.strftime('%H:%M:%S')
            
            query = """
            SELECT id, shift_name 
            FROM shift_tbl 
            WHERE TIME(start_time) <= %s AND TIME(end_time) >= %s
            """
            
            cursor.execute(query, (current_time_str, current_time_str))
            result = cursor.fetchone()
            
            if result:
                logger.info(f"Current shift: {result['shift_name']} (ID: {result['id']})")
                print(f"Current shift: {result['shift_name']} (ID: {result['id']})")
                return result['id']
            else:
                # Try overnight shift logic
                query_overnight = """
                SELECT id, shift_name 
                FROM shift_tbl 
                WHERE TIME(start_time) <= %s AND TIME(end_time) < TIME(start_time)
                OR TIME(end_time) >= %s AND TIME(start_time) > TIME(end_time)
                """
                
                cursor.execute(query_overnight, (current_time_str, current_time_str))
                result = cursor.fetchone()
                
                if result:
                    logger.info(f"Current shift (overnight): {result['shift_name']} (ID: {result['id']})")
                    print(f"Current shift (overnight): {result['shift_name']} (ID: {result['id']})")
                    return result['id']
                else:
                    logger.warning("No shift found for current time, using default")
                    print("No shift found for current time, using default")
                    return 1  # Default to first shift
        except mysql.connector.Error as err:
            logger.error(f"Error getting current shift: {err}")
            print(f"Error getting current shift: {err}")
            return 1  # Default to first shift on error
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def get_setpoints(self, part_number_id):
        """Get setpoints from leakapp_masterdata for a specific part_number"""
        connection = self.persistent_connection

        if not connection:
            return None
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
            SELECT part_number, setpoint1, setpoint2 
            FROM leakapp_masterdata
            WHERE part_number = %s
            """
            
            cursor.execute(query, (part_number_id,))
            setpoint_data = cursor.fetchone()
            
            if setpoint_data:
                logger.info(f"Found setpoints for part number {part_number_id}: SP1={setpoint_data['setpoint1']}, SP2={setpoint_data['setpoint2']}")
                print(f"Found setpoints for part number {part_number_id}: SP1={setpoint_data['setpoint1']}, SP2={setpoint_data['setpoint2']}")
                return setpoint_data
            else:
                logger.warning(f"No setpoints found for part number {part_number_id}")
                print(f"No setpoints found for part number {part_number_id}")
                return None
        except mysql.connector.Error as err:
            logger.error(f"Error fetching setpoints: {err}")
            print(f"Error fetching setpoints: {err}")
            return None
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def update_test_tables(self, filter_no, highest_value, date, part_number_id, shift_id, status):
        """Update both leakapp_test and leakapp_show_report tables with the highest value"""
        connection = self.persistent_connection

        if not connection:
            return False
            
        cursor = connection.cursor()
        
        try:
            # First check if entry exists in leakapp_test
            check_query = """
            SELECT COUNT(*) FROM leakapp_test WHERE filter_no = %s
            """
            cursor.execute(check_query, (filter_no,))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # Update existing record
                update_query = """
                UPDATE leakapp_test 
                SET filter_values = %s, 
                    highest_value = %s, 
                    date = %s, 
                    part_number_id = %s, 
                    shift_id = %s, 
                    status = %s
                WHERE filter_no = %s
                """
                cursor.execute(update_query, (
                    highest_value, 
                    highest_value, 
                    date, 
                    part_number_id, 
                    shift_id, 
                    status,
                    filter_no
                ))
                logger.info(f"Updated leakapp_test for {filter_no} with value {highest_value}, status {status}")
                print(f"Updated leakapp_test for {filter_no} with value {highest_value}, status {status}")
            else:
                # Insert new record
                insert_query = """
                INSERT INTO leakapp_test 
                (filter_no, filter_values, date, highest_value, part_number_id, shift_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    filter_no, 
                    highest_value, 
                    date, 
                    highest_value, 
                    part_number_id, 
                    shift_id, 
                    status
                ))
                logger.info(f"Inserted new record into leakapp_test for {filter_no}")
                print(f"Inserted new record into leakapp_test for {filter_no}")
            
            # Always insert into leakapp_show_report
            insert_report_query = """
            INSERT INTO leakapp_show_report
            (filter_no, filter_values, date, highest_value, part_number_id, shift_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_report_query, (
                filter_no, 
                highest_value, 
                date, 
                highest_value, 
                part_number_id, 
                shift_id, 
                status
            ))
            
            logger.info(f"Inserted into leakapp_show_report for {filter_no}")
            print(f"Inserted into leakapp_show_report for {filter_no}")
            
            connection.commit()
            return True
        except mysql.connector.Error as err:
            connection.rollback()
            logger.error(f"Error updating test tables: {err}")
            print(f"Error updating test tables: {err}")
            return False
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def process_di_change(self, di_name, log_time):
        """Process a single DI change to value 1 using time-based approach"""
        try:
            # Map DI name to corresponding AI filter number
            ai_filter_no = f"AI{di_name[2:]}"  # e.g., DI1 -> AI1
            logger.info(f"========== Processing {di_name} at {log_time}, corresponding to {ai_filter_no} ==========")
            print(f"========== Processing {di_name} at {log_time}, corresponding to {ai_filter_no} ==========")
            
            # Get current shift
            shift_id = self.get_current_shift(log_time)
            
            # Convert log_time to datetime if it's a string
            if isinstance(log_time, str):
                log_time = datetime.strptime(log_time, '%Y-%m-%d %H:%M:%S')
            
            # First check: Check the highest value in the first 5 seconds after DI change
            current_time = datetime.now()
            time_diff = current_time - log_time
            
            # Check if we need to do the first check (5 seconds)
            if not self.pending_di_changes[di_name]["processed_first_check"]:
                # Only perform first check if at least 5 seconds have passed since the DI change
                if time_diff >= timedelta(seconds=5):
                    # Get values in the first 5 seconds after DI change
                    first_check_end = log_time + timedelta(seconds=5)
                    values_for_first_check = self.get_ai_values_by_time(
                        ai_filter_no, 
                        log_time, 
                        first_check_end
                    )
                    
                    if values_for_first_check:
                        # Find the highest value and corresponding part number
                        highest_value_record = max(values_for_first_check, key=lambda x: x['filter_values'])
                        highest_val = highest_value_record['filter_values']
                        part_number_id = highest_value_record['part_number_id']
                        
                        logger.info(f"HIGHEST VALUE FOR FIRST CHECK (5s): {highest_val} at {highest_value_record['date']}")
                        print(f"HIGHEST VALUE FOR FIRST CHECK (5s): {highest_val} at {highest_value_record['date']}")
                        
                        # Get setpoints for the part number
                        setpoint_data = self.get_setpoints(part_number_id)
                        if not setpoint_data:
                            logger.warning(f"No setpoints found for part {part_number_id}, using defaults")
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}  # Default values
                        
                        # Check against setpoint1 and determine status
                        status = "OK" if highest_val <= setpoint_data['setpoint1'] else "NOK"
                        logger.info(f"Status based on SP1: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint1']})")
                        print(f"Status based on SP1: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint1']})")
                        
                        # Update tables with SP1 information
                        self.update_test_tables(
                            ai_filter_no,
                            highest_val,
                            highest_value_record['date'],
                            part_number_id,
                            shift_id,
                            status
                        )
                        
                        # Update monitoring state
                        self.pending_di_changes[di_name]["processed_first_check"] = True
                        self.pending_di_changes[di_name]["timestamp"] = log_time
                        self.pending_di_changes[di_name]["monitoring_active"] = True
                    else:
                        logger.info(f"No values found for first check (5s) yet.")
                        print(f"No values found for first check (5s) yet.")
                else:
                    logger.info(f"Waiting for 5 seconds to pass for first check. Current time diff: {time_diff.total_seconds()}s")
                    print(f"Waiting for 5 seconds to pass for first check. Current time diff: {time_diff.total_seconds()}s")
                    
                    # Set up monitoring if not already active
                    if not self.pending_di_changes[di_name]["monitoring_active"]:
                        self.pending_di_changes[di_name] = {
                            "timestamp": log_time,
                            "processed_first_check": False,
                            "processed_second_check": False,
                            "monitoring_active": True
                        }
            
            # Second check: Check for 15 seconds (including the first 5 and additional 10)
            if self.pending_di_changes[di_name]["processed_first_check"] and not self.pending_di_changes[di_name]["processed_second_check"]:
                # Only perform second check if at least 15 seconds have passed
                if time_diff >= timedelta(seconds=15):
                    # Get values for the full 15 seconds period
                    second_check_end = log_time + timedelta(seconds=15)
                    values_for_second_check = self.get_ai_values_by_time(
                        ai_filter_no, 
                        log_time, 
                        second_check_end
                    )
                    
                    if values_for_second_check:
                        # Find highest value among the 15 second period
                        highest_value_record = max(values_for_second_check, key=lambda x: x['filter_values'])
                        highest_val = highest_value_record['filter_values']
                        part_number_id = highest_value_record['part_number_id']
                        
                        logger.info(f"HIGHEST VALUE FOR SECOND CHECK (15s): {highest_val} at {highest_value_record['date']}")
                        print(f"HIGHEST VALUE FOR SECOND CHECK (15s): {highest_val} at {highest_value_record['date']}")
                        
                        # Get setpoints again (in case they changed)
                        setpoint_data = self.get_setpoints(part_number_id)
                        if not setpoint_data:
                            logger.warning(f"No setpoints found for part {part_number_id}, using defaults")
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}  # Default values
                        
                        # Check against setpoint2
                        status = "OK" if highest_val <= setpoint_data['setpoint2'] else "NOK"
                        logger.info(f"Status based on SP2: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint2']})")
                        print(f"Status based on SP2: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint2']})")
                        
                        # Update tables with SP2 information
                        self.update_test_tables(
                            ai_filter_no,
                            highest_val,
                            highest_value_record['date'],
                            part_number_id,
                            shift_id,
                            status
                        )
                        
                        # Mark second check as processed
                        self.pending_di_changes[di_name]["processed_second_check"] = True
                        logger.info(f"Completed processing both first and second checks for {di_name}")
                        print(f"Completed processing both first and second checks for {di_name}")
                    else:
                        logger.info(f"No values found for second check (15s) yet.")
                        print(f"No values found for second check (15s) yet.")
                else:
                    logger.info(f"Waiting for 15 seconds to pass for second check. Current time diff: {time_diff.total_seconds()}s")
                    print(f"Waiting for 15 seconds to pass for second check. Current time diff: {time_diff.total_seconds()}s")
            
            return True
        except Exception as e:
            logger.error(f"Error in process_di_change: {e}")
            print(f"Error in process_di_change: {e}")
            return False

    def process_pending_changes(self):
        """Process all pending DI changes that are being monitored"""
        for di_name, info in list(self.pending_di_changes.items()):
            if info["monitoring_active"]:
                if not info["processed_first_check"] or not info["processed_second_check"]:
                    logger.info(f"Processing pending DI change for {di_name} from {info['timestamp']}")
                    print(f"Processing pending DI change for {di_name} from {info['timestamp']}")
                    self.process_di_change(di_name, info["timestamp"])
                    
                # If both checks are processed, we can stop monitoring
                if info["processed_first_check"] and info["processed_second_check"]:
                    logger.info(f"Completed all processing for {di_name}, removing from monitoring")
                    print(f"Completed all processing for {di_name}, removing from monitoring")
                    info["monitoring_active"] = False

    def check_direct_di_values(self):
        """Direct method to check current DI values - alternative approach"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
            SELECT di_name, di_value, MAX(log_time) as latest_time
            FROM di_values
            WHERE di_name BETWEEN 'DI1' AND 'DI16'
            GROUP BY di_name
            HAVING di_value = 1
            """
            
            cursor.execute(query)
            active_dis = cursor.fetchall()
            
            if active_dis:
                logger.info(f"Found {len(active_dis)} active DIs (value=1)")
                print(f"Found {len(active_dis)} active DIs (value=1)")
                
                # Process each active DI
                for di in active_dis:
                    di_name = di['di_name']
                    log_time = di['latest_time']
                    
                    # Check if this DI is already being monitored
                    if not self.pending_di_changes[di_name]["monitoring_active"]:
                        logger.info(f"DIRECT DETECTION: {di_name} is active at {log_time}")
                        print(f"DIRECT DETECTION: {di_name} is active at {log_time}")
                        self.process_di_change(di_name, log_time)
                    else:
                        logger.info(f"{di_name} is already being monitored")
                        print(f"{di_name} is already being monitored")
            else:
                logger.info("No active DIs found")
                print("No active DIs found")
                
            return active_dis
        except mysql.connector.Error as err:
            logger.error(f"Error checking direct DI values: {err}")
            print(f"Error checking direct DI values: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def poll_for_changes(self):
        """Alternative method to poll for changes by checking latest values"""
        connection = self.persistent_connection

        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Query to find the latest DI value for each DI
            query = """
            SELECT dv1.di_name, dv1.di_value, dv1.log_time
            FROM di_values dv1
            INNER JOIN (
                SELECT di_name, MAX(log_time) as latest_time
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI16'
                GROUP BY di_name
            ) dv2 ON dv1.di_name = dv2.di_name AND dv1.log_time = dv2.latest_time
            """
            
            cursor.execute(query)
            latest_values = cursor.fetchall()
            
            changes_detected = 0
            for value in latest_values:
                di_name = value['di_name']
                di_value = value['di_value']
                log_time = value['log_time']  # This key exists in the result
                
                # Check for DIs with value 1
                if di_value == 1:
                    changes_detected += 1
                    logger.info(f"POLL: {di_name} has value 1 at {log_time}")
                    print(f"POLL: {di_name} has value 1 at {log_time}")
                    
                    # Process if not already monitoring
                    if not self.pending_di_changes[di_name]["monitoring_active"]:
                        self.process_di_change(di_name, log_time)
            
            if changes_detected > 0:
                logger.info(f"Polling found {changes_detected} DIs with value 1")
                print(f"Polling found {changes_detected} DIs with value 1")
            else:
                logger.info("Polling found no DIs with value 1")
                print("Polling found no DIs with value 1")
                
            return latest_values
        except mysql.connector.Error as err:
            logger.error(f"Error in poll_for_changes: {err}")
            print(f"Error in poll_for_changes: {err}")
            return []
        finally:
            cursor.close()
            connection.close()  # Return connection to pool

    def get_historical_di_state(self, di_name, before_time):
        """Get the DI state before a specific time"""
        connection = self.persistent_connection

        if not connection:
            return None
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
            SELECT di_value
            FROM di_values
            WHERE di_name = %s AND log_time < %s
            ORDER BY log_time DESC
            LIMIT 1
            """
            
            cursor.execute(query, (di_name, before_time))
            result = cursor.fetchone()
            
            if result:
                return result['di_value']
            else:
                return None
        except mysql.connector.Error as err:
            logger.error(f"Error getting historical DI state: {err}")
            print(f"Error getting historical DI state: {err}")
            return None
        finally:
            cursor.close()
            connection.close()

    def audit_di_data(self):
        """Audit the di_values table to see which DIs are present and active"""
        connection = self.persistent_connection

        if not connection:
            return
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Check which DI numbers exist in the database
            existence_query = """
            SELECT DISTINCT di_name 
            FROM di_values 
            WHERE di_name BETWEEN 'DI1' AND 'DI16'
            ORDER BY di_name
            """
            
            cursor.execute(existence_query)
            existing_dis = cursor.fetchall()
            
            logger.info("===== EXISTING DIs IN DATABASE =====")
            print("===== EXISTING DIs IN DATABASE =====")
            di_names = [di['di_name'] for di in existing_dis]
            logger.info(f"Found DIs: {', '.join(di_names)}")
            print(f"Found DIs: {', '.join(di_names)}")
            
            # Check for activity in the last hour for each DI
            activity_query = """
            SELECT di_name, COUNT(*) as records, 
                SUM(CASE WHEN di_value = 1 THEN 1 ELSE 0 END) as active_count
            FROM di_values
            WHERE log_time > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            AND di_name BETWEEN 'DI1' AND 'DI16'
            GROUP BY di_name
            ORDER BY di_name
            """
            
            cursor.execute(activity_query)
            activity = cursor.fetchall()
            
            logger.info("===== DI ACTIVITY (LAST HOUR) =====")
            print("===== DI ACTIVITY (LAST HOUR) =====")
            for di in activity:
                logger.info(f"{di['di_name']}: {di['records']} records, {di['active_count']} active (value=1)")
                print(f"{di['di_name']}: {di['records']} records, {di['active_count']} active (value=1)")
                
        except mysql.connector.Error as err:
            logger.error(f"Error in audit: {err}")
            print(f"Error in audit: {err}")
        finally:
            cursor.close()
            connection.close()
    def run_diagnostics(self):
        """Run complete diagnostics on DI monitoring system"""
        connection = self.persistent_connection

        if not connection:
            return
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            logger.info("=============== RUNNING COMPLETE DI DIAGNOSTICS ===============")
            print("=============== RUNNING COMPLETE DI DIAGNOSTICS ===============")
            
            # 1. Check which DIs exist in the database
            cursor.execute("SELECT DISTINCT di_name FROM di_values WHERE di_name BETWEEN 'DI1' AND 'DI16' ORDER BY di_name")
            existing_dis = cursor.fetchall()
            
            di_names = [di['di_name'] for di in existing_dis]
            logger.info(f"Found {len(di_names)} DIs in database: {', '.join(di_names)}")
            print(f"Found {len(di_names)} DIs in database: {', '.join(di_names)}")
            
            # 2. Check for missing DIs
            all_di_names = [f'DI{i}' for i in range(1, 17)]
            missing_dis = [di for di in all_di_names if di not in di_names]
            
            if missing_dis:
                logger.warning(f"Missing DIs: {', '.join(missing_dis)}")
                print(f"Missing DIs: {', '.join(missing_dis)}")
            else:
                logger.info("All DIs (DI1-DI16) exist in database")
                print("All DIs (DI1-DI16) exist in database")
            
            # 3. Check last values and times for each DI
            cursor.execute("""
                SELECT di_name, di_value, MAX(log_time) as latest_time
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI16'
                GROUP BY di_name, di_value
                ORDER BY di_name, latest_time DESC
            """)
            
            latest_values = cursor.fetchall()
            logger.info("Latest DI values:")
            print("Latest DI values:")
            for val in latest_values:
                logger.info(f"  {val['di_name']}: {val['di_value']} at {val['latest_time']}")
                print(f"  {val['di_name']}: {val['di_value']} at {val['latest_time']}")
            
            # 4. Check total record counts
            cursor.execute("""
                SELECT di_name, COUNT(*) as record_count
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI16'
                GROUP BY di_name
                ORDER BY di_name
            """)
            
            record_counts = cursor.fetchall()
            logger.info("Record counts for each DI:")
            print("Record counts for each DI:")
            for count in record_counts:
                logger.info(f"  {count['di_name']}: {count['record_count']} records")
                print(f"  {count['di_name']}: {count['record_count']} records")
                
            logger.info("=============== DIAGNOSTICS COMPLETED ===============")
            print("=============== DIAGNOSTICS COMPLETED ===============")
            
        except mysql.connector.Error as err:
            logger.error(f"Error in diagnostics: {err}")
            print(f"Error in diagnostics: {err}")
        finally:
            cursor.close()
            connection.close()
    def run(self):
        """Main monitoring loop with multiple detection methods"""
        print("Starting DI monitoring service...")
        logger.info("Starting DI monitoring service")
        
        # Run diagnostics first
        self.run_diagnostics()
        
        # Initialize any missing DIs
        self.initialize_di_entries()
        
        # Run diagnostics again to confirm initialization
        self.run_diagnostics()
        
        while True:
            try:
                # Check if the persistent connection is still valid
                if not self.check_connection_health():
                    logger.warning("Failed to maintain persistent connection, will try again")
                    print("Failed to maintain persistent connection, will try again")
                    time.sleep(2)
                    continue
                
                # Try multiple methods to detect DI changes
                logger.info("Checking for DI changes using multiple methods...")
                print("Checking for DI changes using multiple methods...")
                
                # Method 1: Get changes since last check
                di_changes = self.get_di_changes()
                
                # Method 2: Direct check for active DIs
                active_dis = self.check_direct_di_values()
                
                # Method 3: Poll for latest values
                latest_values = self.poll_for_changes()
                
                # Method 4: Special check for problem DIs
                problem_di_values = self.check_problem_dis()
                
                # Process any pending changes that need more data
                self.process_pending_changes()
                
                # Wait before checking again - use shorter interval for faster response
                logger.info("Checking completed. Sleeping for 0.2 seconds before next check...")
                print("Checking completed. Sleeping for 0.2 seconds before next check...")
                time.sleep(0.1)  # Reduced from 0.5 to 0.2 seconds
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                print(f"Error in main loop: {e}")
                time.sleep(0.5)  # Short delay before retrying

    def cleanup(self):
        """Clean up resources when shutting down"""
        try:
            if self.persistent_connection and self.persistent_connection.is_connected():
                self.persistent_connection.close()
                logger.info("Closed persistent connection")
                print("Closed persistent connection")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    # Create and run the service
    try:
        monitoring_service = DIMonitoringService(DB_CONFIG)
        monitoring_service.run()
    except KeyboardInterrupt:
        print("Service interrupted by user")
        logger.info("Service interrupted by user")
    except Exception as e:
        print(f"Service terminated due to error: {e}")
        logger.error(f"Service terminated due to error: {e}")
    finally:
        # Clean up if possible
        if 'monitoring_service' in locals():
            monitoring_service.cleanup()
