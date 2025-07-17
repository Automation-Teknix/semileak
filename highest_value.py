import mysql.connector
from mysql.connector import pooling
import time
from datetime import datetime, timedelta
from collections import defaultdict

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'leakapp',
    'pool_name': 'leakapp_pool',
    'pool_size': 20
}

class DIMonitoringService:
    def __init__(self, db_config):
        """Initialize the DI monitoring service with a connection pool"""
        self.db_config = db_config
        # Initialize to 1 minute ago to catch recent changes on startup
        self.last_check_time = datetime.now() - timedelta(minutes=1)
        
        # Set up the connection pool
        try:
            self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            print(f"Connection pool created with {db_config['pool_size']} connections")
        except mysql.connector.Error as err:
            print(f"Failed to create connection pool: {err}")
            raise

        # Track DI changes that need processing
        self.pending_di_changes = defaultdict(lambda: {
            "timestamp": None, 
            "processed_first_check": False,
            "processed_second_check": False,
            "monitoring_active": False
        })
        
        # Cache for last known DI states to avoid redundant processing
        self.last_di_states = {}
        
        print("DI Monitoring Service initialized")
        
    def get_connection_from_pool(self):
        """Get a connection from the pool"""
        try:
            connection = self.cnx_pool.get_connection()
            return connection
        except mysql.connector.Error as err:
            print(f"Failed to get connection from pool: {err}")
            return None
            
    def check_connection_health(self, connection):
        """Check if a connection is healthy"""
        if not connection:
            print("Failed to get connection from pool for health check.")
            return False
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except mysql.connector.Error as err:
            print(f"Connection health check failed: {err}")
            return False

    def initialize_di_entries(self, connection):
        """Ensure all DI entries from DI1 to DI18 exist in the database"""
        if not connection:
            return False
        cursor = connection.cursor(dictionary=True)
        try:
            all_dis = [f'DI{i}' for i in range(1, 19)]
            created = 0
            current_time = datetime.now()
            for di_name in all_dis:
                check_query = """
                SELECT 1 FROM di_values WHERE UPPER(TRIM(di_name)) = %s LIMIT 1
                """
                cursor.execute(check_query, (di_name.upper(),))
                exists = cursor.fetchone()
                if not exists:
                    insert_query = """
                    INSERT INTO di_values (di_name, di_value, log_time)
                    VALUES (%s, %s, %s)
                    """
                    cursor.execute(insert_query, (di_name, 0, current_time))
                    print(f"Created initial record for missing {di_name} with value 0")
                    created += 1
            if created:
                connection.commit()
                print(f"Inserted {created} missing DI(s)")
                return True
            else:
                print("All DIs (DI1-DI18) already exist in database")
                return False
        except mysql.connector.Error as err:
            connection.rollback()
            print(f"Error initializing DI entries: {err}")
            return False
        finally:
            cursor.close()

    def initialize_di_cache(self, connection):
        """Initialize the cache with current DI states"""
        if not connection:
            return
        
        cursor = connection.cursor(dictionary=True)
        try:
            # Get latest state for each DI
            query = """
            SELECT dv1.di_name, dv1.di_value
            FROM di_values dv1
            INNER JOIN (
                SELECT di_name, MAX(log_time) as latest_time
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI18'
                GROUP BY di_name
            ) dv2 ON dv1.di_name = dv2.di_name AND dv1.log_time = dv2.latest_time
            """
            
            cursor.execute(query)
            latest_states = cursor.fetchall()
            
            for state in latest_states:
                di_name = state['di_name']
                di_value = int(float(state['di_value'])) if state['di_value'] is not None else 0
                self.last_di_states[di_name] = di_value
            
            print(f"Initialized DI cache with {len(latest_states)} entries")
            
        except mysql.connector.Error as err:
            print(f"Error initializing DI cache: {err}")
        finally:
            cursor.close()

    def get_di_state_before_time(self, connection, di_name, before_time):
        """Get the DI state before a specific time"""
        if not connection:
            return None
        
        # Check cache first
        if di_name in self.last_di_states:
            return self.last_di_states[di_name]
        
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
                state = int(float(result['di_value'])) if result['di_value'] is not None else None
                self.last_di_states[di_name] = state
                return state
            else:
                # No previous state found, assume 0
                self.last_di_states[di_name] = 0
                return 0
        except mysql.connector.Error as err:
            print(f"Error getting DI state before time: {err}")
            return 0
        finally:
            cursor.close()

    def get_di_changes_since_last_check(self, connection):
        """Get only NEW DI changes since last check - optimized version"""
        if not connection:
            return []
        
        cursor = connection.cursor(dictionary=True)
        try:
            current_time = datetime.now()
            formatted_last_check = self.last_check_time.strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"Checking for DI changes since {formatted_last_check}")
            
            # Get all DI changes since last check
            query = """
            SELECT di_name, di_value, log_time 
            FROM di_values 
            WHERE log_time > %s 
            AND di_name BETWEEN 'DI1' AND 'DI18'
            ORDER BY di_name, log_time ASC
            """
            
            cursor.execute(query, (formatted_last_check,))
            recent_changes = cursor.fetchall()
            
            if not recent_changes:
                print("No DI changes found since last check")
                self.last_check_time = current_time
                return []
            
            print(f"Found {len(recent_changes)} DI changes since last check")
            
            # Process changes to find 0→1 transitions
            transitions = []
            
            # Group changes by DI name
            di_changes = defaultdict(list)
            for change in recent_changes:
                di_changes[change['di_name']].append(change)
            
            for di_name, changes in di_changes.items():
                # Get the state before our time window
                prev_state = self.get_di_state_before_time(connection, di_name, formatted_last_check)
                current_state = prev_state
                
                # Check each change for 0→1 transition
                for change in changes:
                    new_state = int(float(change['di_value'])) if change['di_value'] is not None else None
                    
                    if current_state == 0 and new_state == 1:
                        transitions.append({
                            'di_name': di_name,
                            'di_value': 1,
                            'log_time': change['log_time']
                        })
                        print(f"NEW TRANSITION: {di_name} changed from 0 to 1 at {change['log_time']}")
                    
                    current_state = new_state
                
                # Update cached state
                self.last_di_states[di_name] = current_state
            
            # Update last check time
            self.last_check_time = current_time
            return transitions
            
        except mysql.connector.Error as err:
            print(f"Error fetching DI changes: {err}")
            return []
        finally:
            cursor.close()

    def get_ai_values_by_time(self, connection, filter_no, start_time, end_time):
        """Get AI values within a specified time range"""
        if not connection:
            return []
        cursor = connection.cursor(dictionary=True)
        try:
            # Add a buffer of ±1 second to the time window
            buffer = timedelta(seconds=1)
            start_time_buffered = start_time - buffer
            end_time_buffered = end_time + buffer
            print(f"AI value search for {filter_no}: window {start_time_buffered} to {end_time_buffered}")
            query = """
            SELECT filter_no, filter_values, date, part_number_id, shift_id
            FROM leakapp_result_tbl
            WHERE filter_no = %s
            AND date >= %s
            AND date <= %s
            ORDER BY date
            """
            cursor.execute(query, (filter_no, start_time_buffered, end_time_buffered))
            values = cursor.fetchall()
            print(f"AI values found for {filter_no}: {[v['filter_values'] for v in values]}")
            count_available = len(values)
            if count_available > 0:
                print(f"Found {count_available} values for {filter_no} between {start_time_buffered} and {end_time_buffered}")
                if values:
                    highest_value = max(values, key=lambda x: x['filter_values'])
                    print(f"Highest value in time range: {highest_value['filter_values']} at {highest_value['date']}")
            else:
                print(f"No values found for {filter_no} in time range {start_time_buffered} to {end_time_buffered}")
            return values
        except mysql.connector.Error as err:
            print(f"Error fetching AI values by time: {err}")
            return []
        finally:
            cursor.close()

    def get_current_shift(self, connection, current_time):
        """Determine the current shift based on time"""
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
                    print(f"Current shift (overnight): {result['shift_name']} (ID: {result['id']})")
                    return result['id']
                else:
                    print("No shift found for current time, using default")
                    return 1  # Default to first shift
        except mysql.connector.Error as err:
            print(f"Error getting current shift: {err}")
            return 1  # Default to first shift on error
        finally:
            cursor.close()

    def get_setpoints(self, connection, part_number_id):
        """Get setpoints from leakapp_masterdata for a specific part_number"""
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
                print(f"Found setpoints for part number {part_number_id}: SP1={setpoint_data['setpoint1']}, SP2={setpoint_data['setpoint2']}")
                return setpoint_data
            else:
                print(f"No setpoints found for part number {part_number_id}")
                return None
        except mysql.connector.Error as err:
            print(f"Error fetching setpoints: {err}")
            return None
        finally:
            cursor.close()

    def update_test_tables(self, connection, filter_no, highest_value, date, part_number_id, shift_id, status):
        """Update both leakapp_test and leakapp_show_report tables with the highest value"""
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
                print(f"Inserted new record into leakapp_test for {filter_no}")

            # Always insert a new record into leakapp_show_report
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
            print(f"Inserted new record into leakapp_show_report for {filter_no}")

            connection.commit()
            return True
        except mysql.connector.Error as err:
            connection.rollback()
            print(f"Error updating test tables: {err}")
            return False
        finally:
            cursor.close()

    def process_di_change(self, connection, di_name, log_time):
        """Process a single DI change to value 1 using time-based approach"""
        try:
            # Map DI name to corresponding AI filter number
            ai_filter_no = f"AI{di_name[2:]}"  # e.g., DI1 -> AI1
            print(f"========== Processing {di_name} at {log_time}, corresponding to {ai_filter_no} ==========")
            
            # Get current shift
            shift_id = self.get_current_shift(connection, log_time)
            
            # Convert log_time to datetime if it's a string
            if isinstance(log_time, str):
                log_time = datetime.strptime(log_time, '%Y-%m-%d %H:%M:%S')
            
            # First check: Check the highest value in the first 5 seconds after DI change
            current_time = datetime.now()
            time_diff = current_time - log_time
            print(f"DI {di_name} transition at {log_time}, checking AI {ai_filter_no} for 5s window")
            
            # Check if we need to do the first check (5 seconds)
            if not self.pending_di_changes[di_name]["processed_first_check"]:
                # Only perform first check if at least 5 seconds have passed since the DI change
                if time_diff >= timedelta(seconds=5):
                    # Get values in the first 5 seconds after DI change
                    first_check_end = log_time + timedelta(seconds=5)
                    values_for_first_check = self.get_ai_values_by_time(
                        connection,
                        ai_filter_no, 
                        log_time, 
                        first_check_end
                    )
                    print(f"AI values for {ai_filter_no} after DI {di_name} transition: {values_for_first_check}")
                    if values_for_first_check:
                        # Find the highest value and corresponding part number
                        highest_value_record = max(values_for_first_check, key=lambda x: x['filter_values'])
                        highest_val = highest_value_record['filter_values']
                        part_number_id = highest_value_record['part_number_id']
                        print(f"HIGHEST VALUE FOR FIRST CHECK (5s): {highest_val} at {highest_value_record['date']}")
                        
                        setpoint_data = self.get_setpoints(connection, part_number_id)
                        if not setpoint_data:
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}  # Default values
                        
                        # Check against setpoint1 and determine status
                        status = "OK" if highest_val <= setpoint_data['setpoint1'] else "NOK"
                        print(f"Status based on SP1: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint1']})")
                        
                        # Update tables with SP1 information
                        self.update_test_tables(
                            connection,
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
                        print(f"No AI values found for {ai_filter_no} in first 5s after DI {di_name} transition.")
                else:
                    print(f"Waiting for 5 seconds to pass for first check. Current time diff: {time_diff.total_seconds()}s")
                    
                    # Set up monitoring if not already active
                    if not self.pending_di_changes[di_name]["monitoring_active"]:
                        self.pending_di_changes[di_name] = {
                            "timestamp": log_time,
                            "processed_first_check": False,
                            "processed_second_check": False,
                            "monitoring_active": True
                        }
            
            # Second check: 15 seconds after DI change
            if self.pending_di_changes[di_name]["processed_first_check"] and not self.pending_di_changes[di_name]["processed_second_check"]:
                # Only perform second check if at least 15 seconds have passed
                if time_diff >= timedelta(seconds=15):
                    # Get values for the full 15 seconds period
                    second_check_end = log_time + timedelta(seconds=15)
                    values_for_second_check = self.get_ai_values_by_time(
                        connection,
                        ai_filter_no, 
                        log_time, 
                        second_check_end
                    )
                    if values_for_second_check:
                        # Find highest value among the 15 second period
                        highest_value_record = max(values_for_second_check, key=lambda x: x['filter_values'])
                        highest_val = highest_value_record['filter_values']
                        part_number_id = highest_value_record['part_number_id']
                        print(f"HIGHEST VALUE FOR SECOND CHECK (15s): {highest_val} at {highest_value_record['date']}")
                        
                        # Get setpoints again (in case they changed)
                        setpoint_data = self.get_setpoints(connection, part_number_id)
                        if not setpoint_data:
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}  # Default values
                        
                        # Check against setpoint2
                        status = "OK" if highest_val <= setpoint_data['setpoint2'] else "NOK"
                        print(f"Status based on SP2: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint2']})")
                        
                        # Update tables with SP2 information
                        self.update_test_tables(
                            connection,
                            ai_filter_no,
                            highest_val,
                            highest_value_record['date'],
                            part_number_id,
                            shift_id,
                            status
                        )
                        
                        # Mark second check as processed
                        self.pending_di_changes[di_name]["processed_second_check"] = True
                        print(f"Completed processing both first and second checks for {di_name}")
                    else:
                        print(f"No values found for second check (15s) yet.")
                else:
                    print(f"Waiting for 15 seconds to pass for second check. Current time diff: {time_diff.total_seconds()}s")
            
            return True
        except Exception as e:
            print(f"Error in process_di_change: {e}")
            return False

    def process_pending_changes(self, connection):
        """Process all pending DI changes that are being monitored"""
        completed_changes = []
        
        for di_name, info in list(self.pending_di_changes.items()):
            if info["monitoring_active"]:
                if not info["processed_first_check"] or not info["processed_second_check"]:
                    print(f"Processing pending DI change for {di_name} from {info['timestamp']}")
                    self.process_di_change(connection, di_name, info["timestamp"])
                    
                # Mark as completed if both checks are done
                if info["processed_first_check"] and info["processed_second_check"]:
                    completed_changes.append(di_name)
        
        # Clean up completed changes
        for di_name in completed_changes:
            self.pending_di_changes[di_name]["monitoring_active"] = False
            print(f"Completed monitoring for {di_name}")

    def audit_di_data(self, connection):
        """Audit the di_values table to see which DIs are present and active"""
        if not connection:
            return
        cursor = connection.cursor(dictionary=True)
        try:
            # Check which DI numbers exist in the database
            existence_query = """
            SELECT DISTINCT di_name 
            FROM di_values 
            WHERE di_name BETWEEN 'DI1' AND 'DI18'
            ORDER BY di_name
            """
            
            cursor.execute(existence_query)
            existing_dis = cursor.fetchall()
            print("===== EXISTING DIs IN DATABASE =====")
            di_names = [di['di_name'] for di in existing_dis]
            print(f"Found DIs: {', '.join(di_names)}")
            
            # Check for activity in the last hour for each DI
            activity_query = """
            SELECT di_name, COUNT(*) as records, 
                SUM(CASE WHEN di_value = 1 THEN 1 ELSE 0 END) as active_count
            FROM di_values
            WHERE log_time > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            AND di_name BETWEEN 'DI1' AND 'DI18'
            GROUP BY di_name
            ORDER BY di_name
            """
            
            cursor.execute(activity_query)
            activity = cursor.fetchall()
            print("===== DI ACTIVITY (LAST HOUR) =====")
            for di in activity:
                print(f"{di['di_name']}: {di['records']} records, {di['active_count']} active (value=1)")
                
        except mysql.connector.Error as err:
            print(f"Error in audit: {err}")
        finally:
            cursor.close()

    def run_diagnostics(self, connection):
        """Run complete diagnostics on DI monitoring system"""
        if not connection:
            return
        cursor = connection.cursor(dictionary=True)
        try:
            print("=============== RUNNING COMPLETE DI DIAGNOSTICS ===============")
            
            cursor.execute("SELECT DISTINCT di_name FROM di_values WHERE di_name BETWEEN 'DI1' AND 'DI18' ORDER BY di_name")
            existing_dis = cursor.fetchall()
            
            di_names = [di['di_name'] for di in existing_dis]
            print(f"Found {len(di_names)} DIs in database: {', '.join(di_names)}")
            
            # Check for missing DIs
            all_di_names = [f'DI{i}' for i in range(1, 19)]
            missing_dis = [di for di in all_di_names if di not in di_names]
            
            if missing_dis:
                print(f"Missing DIs: {', '.join(missing_dis)}")
            else:
                print("All DIs (DI1-DI18) exist in database")
            
            # Check last values and times for each DI
            cursor.execute("""
                SELECT di_name, di_value, MAX(log_time) as latest_time
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI18'
                GROUP BY di_name, di_value
                ORDER BY di_name, latest_time DESC
            """)
            
            latest_values = cursor.fetchall()
            print("Latest DI values:")
            for val in latest_values:
                print(f"  {val['di_name']}: {val['di_value']} at {val['latest_time']}")
            
            # Check total record counts
            cursor.execute("""
                SELECT di_name, COUNT(*) as record_count
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI18'
                GROUP BY di_name
                ORDER BY di_name
            """)
            
            record_counts = cursor.fetchall()
            print("Record counts for each DI:")
            for count in record_counts:
                print(f"  {count['di_name']}: {count['record_count']} records")
                
            print("=============== DIAGNOSTICS COMPLETED ===============")
            
        except mysql.connector.Error as err:
            print(f"Error in diagnostics: {err}")
        finally:
            cursor.close()

    def run(self):
        """Optimized main monitoring loop"""
        print("Starting optimized DI monitoring service...")
        
        # Initialize connection and setup
        connection = self.get_connection_from_pool()
        if not connection:
            print("Failed to get initial connection")
            return
        
        try:
            # Run diagnostics first
            self.run_diagnostics(connection)
            
            # Initialize any missing DIs
            self.initialize_di_entries(connection)
            
            # Initialize DI cache
            self.initialize_di_cache(connection)
            
        finally:
            connection.close()
        
        print("Starting main monitoring loop...")
        
        while True:
            connection = self.get_connection_from_pool()
            if not connection:
                print("Failed to get connection, retrying...")
                time.sleep(1)
                continue
            
            try:
                # Check connection health
                if not self.check_connection_health(connection):
                    print("Connection health check failed, retrying...")
                    time.sleep(1)
                    continue
                
                # Only check for NEW changes since last check
                new_transitions = self.get_di_changes_since_last_check(connection)
                
                # Process any new transitions
                for transition in new_transitions:
                    print(f"Processing new transition: {transition['di_name']} at {transition['log_time']}")
                    self.process_di_change(connection, transition['di_name'], transition['log_time'])
                
                # Process any pending changes that need time-based checks
                self.process_pending_changes(connection)
                
                print("Monitoring cycle completed. Sleeping for 1 second...")
                
                # Sleep for a reasonable interval (1 second instead of 0.1)
                time.sleep(1)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(1)
            finally:
                if connection:
                    connection.close()

    def cleanup(self):
        """Clean up resources when shutting down"""
        print("Cleanup called. Closing connection pool...")
        if hasattr(self, 'cnx_pool'):
            try:
                # Close all connections in the pool
                self.cnx_pool.close()
                print("Connection pool closed successfully")
            except Exception as e:
                print(f"Error closing connection pool: {e}")

if __name__ == "__main__":
    # Create and run the service
    monitoring_service = DIMonitoringService(DB_CONFIG)
    try:
        monitoring_service.run()
    except KeyboardInterrupt:
        print("Service interrupted by user")
    except Exception as e:
        print(f"Service terminated due to error: {e}")
    finally:
        # Clean up if possible
        if 'monitoring_service' in locals():
            monitoring_service.cleanup()
