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
    'pool_size': 5  # Maintain 5 connections in the pool
}

class DIMonitoringService:
    def __init__(self, db_config):
        """Initialize the DI monitoring service with a connection pool"""
        self.db_config = db_config
        self.last_check_time = datetime.now() - timedelta(minutes=5)  # Start with a small look-back period
        
        # Set up the connection pool
        try:
            self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            print(f"Connection pool created with {db_config['pool_size']} connections")
        except mysql.connector.Error as err:
            print(f"Failed to create connection pool: {err}")
            raise
        
        # Maintain a persistent connection for health checks
        self.persistent_connection = self.get_connection_from_pool()
        
        # Track DI changes that need processing
        self.pending_di_changes = defaultdict(lambda: {
            "timestamp": None, 
            "processed_first_check": False,
            "processed_second_check": False,
            "monitoring_active": False
        })
        
        # Store last known DI states to detect transitions
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
            
    def check_connection_health(self):
        """Check if the persistent connection is still alive, reconnect if needed"""
        try:
            # Simple query to check connection
            if self.persistent_connection and self.persistent_connection.is_connected():
                cursor = self.persistent_connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
            else:
                print("Persistent connection lost, reconnecting...")
                self.persistent_connection = self.get_connection_from_pool()
                return self.persistent_connection is not None
        except mysql.connector.Error as err:
            print(f"Connection health check failed: {err}")
            # Try to reconnect
            self.persistent_connection = self.get_connection_from_pool()
            return self.persistent_connection is not None

    def initialize_di_entries(self):
        """Ensure all DI entries from DI1 to DI16 exist in the database - FIXED"""
        connection = self.get_connection_from_pool()
        if not connection:
            return False
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Check which DIs already exist by getting the latest record for each
            check_query = """
            SELECT di_name, MAX(log_time) as latest_time
            FROM di_values 
            WHERE di_name BETWEEN 'DI1' AND 'DI16'
            GROUP BY di_name
            """
            
            cursor.execute(check_query)
            existing_dis = {row['di_name']: row['latest_time'] for row in cursor.fetchall()}
            
            # Identify missing DIs
            all_dis = {f'DI{i}' for i in range(1, 17)}
            missing_dis = all_dis - set(existing_dis.keys())
            
            if missing_dis:
                print(f"Found {len(missing_dis)} missing DIs in database: {', '.join(sorted(missing_dis))}")
                
                # Initialize missing DIs with default value 0
                current_time = datetime.now()
                
                for di_name in missing_dis:
                    insert_query = """
                    INSERT INTO di_values (di_name, di_value, log_time)
                    VALUES (%s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (di_name, 0, current_time))
                    print(f"Created initial record for missing {di_name} with value 0")
                    
                connection.commit()
                return True
            else:
                print("All DIs (DI1-DI16) already exist in database")
                return False
                
        except mysql.connector.Error as err:
            connection.rollback()
            print(f"Error initializing DI entries: {err}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_current_di_states(self):
        """Get current states of all DIs to detect transitions - NEW METHOD"""
        connection = self.get_connection_from_pool()
        if not connection:
            return {}
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Get the latest value for each DI
            query = """
            SELECT dv1.di_name, dv1.di_value, dv1.log_time
            FROM di_values dv1
            INNER JOIN (
                SELECT di_name, MAX(log_time) as max_time
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI16'
                GROUP BY di_name
            ) dv2 ON dv1.di_name = dv2.di_name AND dv1.log_time = dv2.max_time
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            current_states = {}
            for row in results:
                current_states[row['di_name']] = {
                    'value': row['di_value'],
                    'timestamp': row['log_time']
                }
            
            return current_states
        except mysql.connector.Error as err:
            print(f"Error getting current DI states: {err}")
            return {}
        finally:
            cursor.close()
            connection.close()

    def detect_transitions(self):
        """Detect 0->1 transitions by comparing current states with last known states - NEW METHOD"""
        current_states = self.get_current_di_states()
        transitions = []
        
        for di_name, current_state in current_states.items():
            current_value = current_state['value']
            current_time = current_state['timestamp']
            
            # Check if we have a previous state
            if di_name in self.last_di_states:
                last_value = self.last_di_states[di_name]['value']
                last_time = self.last_di_states[di_name]['timestamp']
                
                # Detect 0->1 transition
                if last_value == 0 and current_value == 1 and current_time > last_time:
                    transitions.append({
                        'di_name': di_name,
                        'transition_time': current_time,
                        'previous_value': last_value,
                        'current_value': current_value
                    })
                    print(f"TRANSITION DETECTED: {di_name} changed from {last_value} to {current_value} at {current_time}")
            
            # Update last known state
            self.last_di_states[di_name] = current_state
        
        return transitions

    def get_di_changes(self):
        """Enhanced method to detect DI changes using transition detection"""
        try:
            # Use transition detection method
            transitions = self.detect_transitions()
            
            changes = []
            for transition in transitions:
                di_name = transition['di_name']
                transition_time = transition['transition_time']
                
                # Only process if not already monitoring
                if not self.pending_di_changes[di_name]["monitoring_active"]:
                    changes.append({
                        'di_name': di_name,
                        'di_value': 1,
                        'log_time': transition_time
                    })
                    
                    # Start monitoring this DI
                    self.pending_di_changes[di_name] = {
                        "timestamp": transition_time,
                        "processed_first_check": False,
                        "processed_second_check": False,
                        "monitoring_active": True
                    }
            
            return changes
        except Exception as e:
            print(f"Error in get_di_changes: {e}")
            return []

    def get_ai_values_by_time(self, filter_no, start_time, end_time):
        """Get AI values within a specified time range - FIXED to handle proper time comparison"""
        connection = self.get_connection_from_pool()
        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Convert datetime objects to strings for MySQL comparison
            if isinstance(start_time, datetime):
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                start_time_str = start_time
                
            if isinstance(end_time, datetime):
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_time_str = end_time
            
            query = """
            SELECT filter_no, filter_values, date, part_number_id, shift_id
            FROM leakapp_result_tbl
            WHERE filter_no = %s
            AND date >= %s
            AND date <= %s
            ORDER BY date
            """
            
            cursor.execute(query, (filter_no, start_time_str, end_time_str))
            values = cursor.fetchall()
            
            count_available = len(values)
            if count_available > 0:
                print(f"Found {count_available} values for {filter_no} between {start_time_str} and {end_time_str}")
                if values:
                    highest_value = max(values, key=lambda x: x['filter_values'])
                    print(f"Highest value in time range: {highest_value['filter_values']} at {highest_value['date']}")
            else:
                print(f"No values found for {filter_no} in time range {start_time_str} to {end_time_str}")
                
            return values
        except mysql.connector.Error as err:
            print(f"Error fetching AI values by time: {err}")
            return []
        finally:
            cursor.close()
            connection.close()

    def get_current_shift(self, current_time):
        """Determine the current shift based on time"""
        connection = self.get_connection_from_pool()
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
            connection.close()

    def get_setpoints(self, part_number_id):
        """Get setpoints from leakapp_masterdata for a specific part_number"""
        connection = self.get_connection_from_pool()
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
            connection.close()

    def update_test_tables(self, filter_no, highest_value, date, part_number_id, shift_id, status):
        """Update both leakapp_test and leakapp_show_report tables with the highest value"""
        connection = self.get_connection_from_pool()
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
            print(f"Inserted into leakapp_show_report for {filter_no}")
            
            connection.commit()
            return True
        except mysql.connector.Error as err:
            connection.rollback()
            print(f"Error updating test tables: {err}")
            return False
        finally:
            cursor.close()
            connection.close()

    def process_di_change(self, di_name, log_time):
        """Process a single DI change to value 1 using time-based approach - FIXED LOGIC"""
        try:
            # Map DI name to corresponding AI filter number
            ai_filter_no = f"AI{di_name[2:]}"  # e.g., DI1 -> AI1
            print(f"========== Processing {di_name} at {log_time}, corresponding to {ai_filter_no} ==========")
            
            # Get current shift
            shift_id = self.get_current_shift(log_time)
            
            # Convert log_time to datetime if it's a string
            if isinstance(log_time, str):
                log_time = datetime.strptime(log_time, '%Y-%m-%d %H:%M:%S')
            
            # Calculate time differences
            current_time = datetime.now()
            time_diff = current_time - log_time
            
            # FIXED: First check (5 seconds) - Only process if enough time has passed
            if not self.pending_di_changes[di_name]["processed_first_check"]:
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
                        print(f"FIRST CHECK (5s): Highest value = {highest_val} at {highest_value_record['date']}")
                        
                        # Get setpoints
                        setpoint_data = self.get_setpoints(part_number_id)
                        if not setpoint_data:
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}
                        
                        # Check against setpoint1
                        status = "OK" if highest_val <= setpoint_data['setpoint1'] else "NOK"
                        print(f"Status based on SP1: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint1']})")
                        
                        # Update tables with SP1 result
                        self.update_test_tables(
                            ai_filter_no,
                            highest_val,
                            highest_value_record['date'],
                            part_number_id,
                            shift_id,
                            status
                        )
                        
                        # Mark first check as processed
                        self.pending_di_changes[di_name]["processed_first_check"] = True
                    else:
                        print(f"No AI values found for first check (5s) for {ai_filter_no}")
                else:
                    print(f"Waiting for 5 seconds to pass for first check. Current wait: {time_diff.total_seconds():.1f}s")
            
            # FIXED: Second check (15 seconds) - Only process if enough time has passed AND first check is done
            if (self.pending_di_changes[di_name]["processed_first_check"] and 
                not self.pending_di_changes[di_name]["processed_second_check"]):
                
                if time_diff >= timedelta(seconds=15):
                    # Get values for the full 15 seconds period
                    second_check_end = log_time + timedelta(seconds=15)
                    values_for_second_check = self.get_ai_values_by_time(
                        ai_filter_no, 
                        log_time, 
                        second_check_end
                    )
                    
                    if values_for_second_check:
                        # Find highest value in the 15-second period
                        highest_value_record = max(values_for_second_check, key=lambda x: x['filter_values'])
                        highest_val = highest_value_record['filter_values']
                        part_number_id = highest_value_record['part_number_id']
                        
                        print(f"SECOND CHECK (15s): Highest value = {highest_val} at {highest_value_record['date']}")
                        
                        # Get setpoints
                        setpoint_data = self.get_setpoints(part_number_id)
                        if not setpoint_data:
                            print(f"No setpoints found for part {part_number_id}, using defaults")
                            setpoint_data = {'setpoint1': 70, 'setpoint2': 18}
                        
                        # Check against setpoint2
                        status = "OK" if highest_val <= setpoint_data['setpoint2'] else "NOK"
                        print(f"Status based on SP2: {status} (value: {highest_val}, setpoint: {setpoint_data['setpoint2']})")
                        
                        # Update tables with SP2 result
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
                        print(f"Completed both checks for {di_name}")
                    else:
                        print(f"No AI values found for second check (15s) for {ai_filter_no}")
                else:
                    print(f"Waiting for 15 seconds to pass for second check. Current wait: {time_diff.total_seconds():.1f}s")
            
            return True
        except Exception as e:
            print(f"Error in process_di_change: {e}")
            return False

    def process_pending_changes(self):
        """Process all pending DI changes that are being monitored"""
        completed_dis = []
        
        for di_name, info in self.pending_di_changes.items():
            if info["monitoring_active"]:
                # Continue processing until both checks are done
                if not info["processed_first_check"] or not info["processed_second_check"]:
                    self.process_di_change(di_name, info["timestamp"])
                
                # Mark as completed if both checks are done
                if info["processed_first_check"] and info["processed_second_check"]:
                    completed_dis.append(di_name)
        
        # Clean up completed DIs
        for di_name in completed_dis:
            print(f"Completed processing for {di_name}, removing from monitoring")
            self.pending_di_changes[di_name]["monitoring_active"] = False

    def run_diagnostics(self):
        """Run complete diagnostics on DI monitoring system"""
        connection = self.get_connection_from_pool()
        if not connection:
            return
            
        cursor = connection.cursor(dictionary=True)
        
        try:
            print("=============== RUNNING COMPLETE DI DIAGNOSTICS ===============")
            
            # Check existing DIs
            cursor.execute("SELECT DISTINCT di_name FROM di_values WHERE di_name BETWEEN 'DI1' AND 'DI16' ORDER BY di_name")
            existing_dis = cursor.fetchall()
            
            di_names = [di['di_name'] for di in existing_dis]
            print(f"Found {len(di_names)} DIs in database: {', '.join(di_names)}")
            
            # Check for missing DIs
            all_di_names = [f'DI{i}' for i in range(1, 17)]
            missing_dis = [di for di in all_di_names if di not in di_names]
            
            if missing_dis:
                print(f"Missing DIs: {', '.join(missing_dis)}")
            else:
                print("All DIs (DI1-DI16) exist in database")
            
            # Check current states
            current_states = self.get_current_di_states()
            print("Current DI states:")
            for di_name in sorted(current_states.keys()):
                state = current_states[di_name]
                print(f"  {di_name}: {state['value']} at {state['timestamp']}")
            
            print("=============== DIAGNOSTICS COMPLETED ===============")
            
        except mysql.connector.Error as err:
            print(f"Error in diagnostics: {err}")
        finally:
            cursor.close()
            connection.close()

    def run(self):
        """Main monitoring loop with enhanced transition detection"""
        print("Starting DI monitoring service...")
        
        # Run diagnostics first
        self.run_diagnostics()
        
        # Initialize any missing DIs
        self.initialize_di_entries()
        
        # Initialize current states
        self.last_di_states = self.get_current_di_states()
        print(f"Initialized with {len(self.last_di_states)} DI states")
        
        while True:
            try:
                # Check connection health
                if not self.check_connection_health():
                    print("Failed to maintain persistent connection, will try again")
                    time.sleep(2)
                    continue
                
                print("Checking for DI transitions...")
                
                # Main method: Detect transitions
                di_changes = self.get_di_changes()
                
                if di_changes:
                    print(f"Found {len(di_changes)} new DI transitions")
                    for change in di_changes:
                        print(f"Processing transition: {change['di_name']} -> {change['di_value']} at {change['log_time']}")
                        self.process_di_change(change['di_name'], change['log_time'])
                
                # Process any pending changes
                self.process_pending_changes()
                
                print("Check cycle completed. Waiting before next check...")
                time.sleep(0.5)  # Check every 0.5 seconds
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(1)  # Wait longer on error

    def cleanup(self):
        """Clean up resources when shutting down"""
        try:
            if self.persistent_connection and self.persistent_connection.is_connected():
                self.persistent_connection.close()
                print("Closed persistent connection")
        except Exception as e:
            print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    # Create and run the service
    try:
        monitoring_service = DIMonitoringService(DB_CONFIG)
        monitoring_service.run()
    except KeyboardInterrupt:
        print("Service interrupted by user")
    except Exception as e:
        print(f"Service terminated due to error: {e}")
    finally:
        # Clean up if possible
        if 'monitoring_service' in locals():
            monitoring_service.cleanup()