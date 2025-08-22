import mysql.connector
from mysql.connector import pooling
import time
from datetime import datetime, timedelta
from collections import defaultdict
import uuid

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'leakapp',
    'pool_name': 'leakapp_pool',
    'pool_size': 20
}

class ContinuousDIMonitoringService:
    def __init__(self, db_config):
        """Initialize the continuous DI monitoring service with a connection pool"""
        self.db_config = db_config
        self.last_check_time = datetime.now() - timedelta(minutes=0.5)
        
        # Set up the connection pool
        try:
            self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            print(f"Connection pool created with {db_config['pool_size']} connections")
        except mysql.connector.Error as err:
            print(f"Failed to create connection pool: {err}")
            raise

        # Track ALL DI changes with unique IDs - never stop monitoring
        self.di_change_instances = {}  # Store all DI change instances with unique IDs
        self.di_change_counter = 0  # Counter for unique instance IDs
        
        print("Continuous DI Monitoring Service initialized")
        
    def get_connection_from_pool(self):
        """Get a connection from the pool"""
        try:
            connection = self.cnx_pool.get_connection()
            return connection
        except mysql.connector.Error as err:
            print(f"Failed to get connection from pool: {err}")
            return None
            
    def check_connection_health(self, connection):
        """Check if a new connection can be obtained and a simple query executed"""
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
        """Ensure all DI entries from DI1 to DI18 exist in the database, without creating duplicates"""
        if not connection:
            return False
        cursor = connection.cursor(dictionary=True)
        try:
            all_dis = [f'DI{i}' for i in range(1, 19)]
            created = 0
            current_time = datetime.now()
            for di_name in all_dis:
                # Only insert if DI does not exist at all (case-insensitive, trimmed)
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

    def get_di_transitions(self, connection):
        """Get all DI transitions from 0 to 1 since last check time"""
        if not connection:
            return []
        cursor = connection.cursor(dictionary=True)
        try:
            all_dis = [f'DI{i}' for i in range(1, 19)]
            transitions = []
            formatted_last_check = self.last_check_time.strftime('%Y-%m-%d %H:%M:%S')

            for di_name in all_dis:
                di_name_trimmed = di_name.strip()
                
                # Get all records since last check time for this DI
                cursor.execute("""
                    SELECT di_value, log_time FROM di_values
                    WHERE TRIM(di_name) = %s AND log_time > %s
                    ORDER BY log_time ASC
                """, (di_name_trimmed, formatted_last_check))
                new_records = cursor.fetchall()
                
                if new_records:
                    print(f"🔍 Found {len(new_records)} new records for {di_name_trimmed} since {formatted_last_check}")
                    
                    # Get the last value before the check time to establish previous state
                    cursor.execute("""
                        SELECT di_value FROM di_values
                        WHERE TRIM(di_name) = %s AND log_time <= %s
                        ORDER BY log_time DESC
                        LIMIT 1
                    """, (di_name_trimmed, formatted_last_check))
                    prev_record = cursor.fetchone()
                    
                    # Set initial previous value
                    prev_val = int(float(prev_record['di_value'])) if prev_record and prev_record['di_value'] is not None else 0
                    
                    # Check each new record for 0→1 transitions only
                    for record in new_records:
                        curr_val = int(float(record['di_value'])) if record['di_value'] is not None else 0
                        
                        # Only log and process 0→1 transitions
                        if prev_val == 0 and curr_val == 1:
                            transitions.append({
                                'di_name': di_name_trimmed,
                                'di_value': 1,
                                'log_time': record['log_time']
                            })
                            print(f"🔥 DETECTED 0→1 TRANSITION for {di_name_trimmed} at {record['log_time']}")
                        
                        prev_val = curr_val  # Update previous value for next iteration
                
            # Update last check time
            self.last_check_time = datetime.now()
            print(f"⏰ Updated last check time to {self.last_check_time}")
            
            return transitions

        except Exception as e:
            print(f"Error in get_di_transitions: {e}")
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
            
            if values:
                print(f"📊 Found {len(values)} AI values for {filter_no} in time range")
                highest_value = max(values, key=lambda x: x['filter_values'])
                print(f"📈 Highest value: {highest_value['filter_values']} at {highest_value['date']}")
            
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
                    return result['id']
                else:
                    return 1  # Default to first shift
        except mysql.connector.Error as err:
            print(f"Error getting current shift: {err}")
            return 1
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
                return setpoint_data
            else:
                return None
        except mysql.connector.Error as err:
            print(f"Error fetching setpoints: {err}")
            return None
        finally:
            cursor.close()

    def update_test_tables(self, connection, filter_no, highest_value, date, part_number_id, shift_id, status, check_type=""):
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
                print(f"✅ Updated leakapp_test for {filter_no} with value {highest_value}, status {status} ({check_type})")
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
                print(f"✅ Inserted new record into leakapp_test for {filter_no} ({check_type})")

            # Always insert into leakapp_show_report for every calculation
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
            print(f"✅ Inserted record into leakapp_show_report for {filter_no} ({check_type})")

            connection.commit()
            return True
        except mysql.connector.Error as err:
            connection.rollback()
            print(f"Error updating test tables: {err}")
            return False
        finally:
            cursor.close()

    def create_di_change_instance(self, di_name, log_time):
        """Create a new DI change instance for continuous monitoring"""
        instance_id = str(uuid.uuid4())[:8]  # Short unique ID
        
        if isinstance(log_time, str):
            log_time = datetime.strptime(log_time, '%Y-%m-%d %H:%M:%S')
        
        instance_data = {
            'di_name': di_name,
            'start_time': log_time,
            'first_check_end': log_time + timedelta(seconds=5),    # 0-5s window
            'second_check_start': log_time + timedelta(seconds=5), # 5s mark
            'second_check_end': log_time + timedelta(seconds=15),  # 5-15s window (next 10s)
            'first_check_done': False,
            'second_check_done': False,
            'created_at': datetime.now()
        }
        
        self.di_change_instances[instance_id] = instance_data
        self.di_change_counter += 1
        
        print(f"🆕 Created DI change instance {instance_id} for {di_name} at {log_time}")
        print(f"   📅 First check (0-5s): {log_time} to {instance_data['first_check_end']}")
        print(f"   📅 Second check (5-15s): {instance_data['second_check_start']} to {instance_data['second_check_end']}")
        return instance_id

    def process_di_change_instance(self, connection, instance_id):
        """Process a specific DI change instance with separate time windows"""
        if instance_id not in self.di_change_instances:
            return
            
        instance = self.di_change_instances[instance_id]
        di_name = instance['di_name']
        ai_filter_no = f"AI{di_name[2:]}"
        now = datetime.now()
        
        try:
            # First check (0-5 seconds window)
            if not instance['first_check_done'] and now >= instance['first_check_end']:
                print(f"🔍 Processing FIRST check for instance {instance_id} ({di_name}) - Time window: 0-5 seconds")
                
                values_0_5s = self.get_ai_values_by_time(
                    connection,
                    ai_filter_no,
                    instance['start_time'],          # Start from DI change time
                    instance['first_check_end']      # End at 5 seconds
                )
                
                if values_0_5s:
                    highest_value_record = max(values_0_5s, key=lambda x: x['filter_values'])
                    highest_val = highest_value_record['filter_values']
                    part_number_id = highest_value_record['part_number_id']
                    shift_id = self.get_current_shift(connection, instance['start_time'])
                    
                    setpoint_data = self.get_setpoints(connection, part_number_id) or {'setpoint1': 70, 'setpoint2': 18}
                    status = "OK" if highest_val <= setpoint_data['setpoint1'] else "NOK"
                    
                    self.update_test_tables(
                        connection,
                        ai_filter_no,
                        highest_val,
                        highest_value_record['date'],
                        part_number_id,
                        shift_id,
                        status,
                        f"First 5s check - Instance {instance_id}",
                    )
                    
                    instance['first_check_done'] = True
                    print(f"✅ First check (0-5s) completed for instance {instance_id} - Highest: {highest_val}")
                else:
                    print(f"❌ No AI values found for first check (0-5s) - instance {instance_id}")

            # Second check (5-15 seconds window - next 10 seconds)
            if not instance['second_check_done'] and now >= instance['second_check_end']:
                print(f"🔍 Processing SECOND check for instance {instance_id} ({di_name}) - Time window: 5-15 seconds (next 10s)")
                
                values_5_15s = self.get_ai_values_by_time(
                    connection,
                    ai_filter_no,
                    instance['second_check_start'],  # Start from 5 seconds
                    instance['second_check_end']     # End at 15 seconds
                )
                
                if values_5_15s:
                    highest_value_record = max(values_5_15s, key=lambda x: x['filter_values'])
                    highest_val = highest_value_record['filter_values']
                    part_number_id = highest_value_record['part_number_id']
                    shift_id = self.get_current_shift(connection, instance['start_time'])
                    
                    setpoint_data = self.get_setpoints(connection, part_number_id) or {'setpoint1': 70, 'setpoint2': 18}
                    status = "OK" if highest_val <= setpoint_data['setpoint2'] else "NOK"
                    
                    self.update_test_tables(
                        connection,
                        ai_filter_no,
                        highest_val,
                        highest_value_record['date'],
                        part_number_id,
                        shift_id,
                        status,
                        f"Next 10s check (5-15s) - Instance {instance_id}",
                    )
                    
                    instance['second_check_done'] = True
                    print(f"✅ Second check (5-15s) completed for instance {instance_id} - Highest: {highest_val}")
                else:
                    print(f"❌ No AI values found for second check (5-15s) - instance {instance_id}")

        except Exception as e:
            print(f"❌ Error processing instance {instance_id}: {e}")

    def debug_di_current_states(self, connection):
        """Debug method to show current DI states"""
        if not connection:
            return
        cursor = connection.cursor(dictionary=True)
        try:
            all_dis = [f'DI{i}' for i in range(1, 19)]
            print("🔍 DEBUGGING CURRENT DI STATES:")
            
            for di_name in all_dis:
                # Get latest record for this DI
                cursor.execute("""
                    SELECT di_value, log_time FROM di_values
                    WHERE TRIM(di_name) = %s
                    ORDER BY log_time DESC
                    LIMIT 1
                """, (di_name.strip(),))
                latest = cursor.fetchone()
                
                if latest:
                    print(f"   {di_name}: Current value = {latest['di_value']} at {latest['log_time']}")
                else:
                    print(f"   {di_name}: No records found")
                    
        except Exception as e:
            print(f"Error in debug_di_current_states: {e}")
        finally:
            cursor.close()

    def debug_recent_di_changes(self, connection):
        """Debug method to show recent DI changes"""
        if not connection:
            return
        cursor = connection.cursor(dictionary=True)
        try:
            # Get all DI changes in last 30 minutes
            cursor.execute("""
                SELECT di_name, di_value, log_time 
                FROM di_values
                WHERE di_name BETWEEN 'DI1' AND 'DI18'
                AND log_time >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
                ORDER BY log_time DESC
                LIMIT 20
            """)
            recent_changes = cursor.fetchall()
            
            print(f"🔍 RECENT DI CHANGES (Last 30 minutes): {len(recent_changes)} records")
            for change in recent_changes:
                print(f"   {change['di_name']}: {change['di_value']} at {change['log_time']}")
                
        except Exception as e:
            print(f"Error in debug_recent_di_changes: {e}")
        finally:
            cursor.close()

    def check_for_active_dis(self, connection):
        """Alternative method: Check for any DIs currently at value 1"""
        if not connection:
            return []
        cursor = connection.cursor(dictionary=True)
        try:
            all_dis = [f'DI{i}' for i in range(1, 19)]
            active_dis = []
            
            print("🔍 CHECKING FOR ACTIVE DIs (value = 1):")
            
            for di_name in all_dis:
                # Get latest record for this DI
                cursor.execute("""
                    SELECT di_value, log_time FROM di_values
                    WHERE TRIM(di_name) = %s
                    ORDER BY log_time DESC
                    LIMIT 1
                """, (di_name.strip(),))
                latest = cursor.fetchone()
                
                if latest and int(float(latest['di_value']) if latest['di_value'] is not None else 0) == 1:
                    active_dis.append({
                        'di_name': di_name,
                        'di_value': 1,
                        'log_time': latest['log_time']
                    })
                    print(f"   🔥 FOUND ACTIVE DI: {di_name} = 1 at {latest['log_time']}")
                    
                    # Check if we're already monitoring this DI
                    if not any(instance['di_name'] == di_name and instance['start_time'] == latest['log_time'] 
                             for instance in self.di_change_instances.values()):
                        print(f"   🆕 Creating new instance for active DI: {di_name}")
                        self.create_di_change_instance(di_name, latest['log_time'])
            
            if not active_dis:
                print("   ⭕ No DIs currently active (value = 1)")
                
            return active_dis
            
        except Exception as e:
            print(f"Error in check_for_active_dis: {e}")
            return []
        finally:
            cursor.close()

    def cleanup_old_instances(self):
        """Clean up old instances that are fully processed and older than 1 minute"""
        current_time = datetime.now()
        instances_to_remove = []
        
        for instance_id, instance in self.di_change_instances.items():
            # Remove instances that are complete and older than 1 minute
            if (instance['first_check_done'] and instance['second_check_done'] and 
                current_time - instance['created_at'] > timedelta(minutes=1)):
                instances_to_remove.append(instance_id)
        
        for instance_id in instances_to_remove:
            print(f"🧹 Cleaning up completed instance {instance_id}")
            del self.di_change_instances[instance_id]

    def process_all_instances(self, connection):
        """Process all active DI change instances"""
        active_instances = len([i for i in self.di_change_instances.values() 
                              if not (i['first_check_done'] and i['second_check_done'])])
        
        if active_instances > 0:
            print(f"🔄 Processing {active_instances} active instances...")
            
            for instance_id in list(self.di_change_instances.keys()):
                self.process_di_change_instance(connection, instance_id)

    def run(self):
        """Main continuous monitoring loop"""
        print("🚀 Starting Continuous DI monitoring service...")
        
        # Initialize last_check_time to current time minus a small buffer
        self.last_check_time = datetime.now() - timedelta(seconds=30)
        print(f"🕒 Initial check time set to: {self.last_check_time}")
        
        while True:
            connection = self.get_connection_from_pool()
            try:
                if not self.check_connection_health(connection):
                    print("❌ Connection health check failed, retrying...")
                    time.sleep(2)
                    continue
                
                # Initialize missing DIs only on first run
                if self.di_change_counter == 0:
                    self.initialize_di_entries(connection)
                    print("✅ DI initialization completed, starting monitoring...")
                
                print(f"🔍 Checking for DI transitions since {self.last_check_time}...")
                
                # Detect new DI transitions (0 to 1)
                di_transitions = self.get_di_transitions(connection)
                
                if di_transitions:
                    print(f"🎯 Found {len(di_transitions)} new DI transitions!")
                    # Create new instances for each detected transition
                    for transition in di_transitions:
                        instance_id = self.create_di_change_instance(
                            transition['di_name'], 
                            transition['log_time']
                        )
                else:
                    print("⏸️ No new DI transitions detected")
                
                # Process all active instances
                self.process_all_instances(connection)
                
                # Clean up old completed instances
                self.cleanup_old_instances()
                
                # Status report
                active_count = len([i for i in self.di_change_instances.values() 
                                  if not (i['first_check_done'] and i['second_check_done'])])
                total_count = len(self.di_change_instances)
                
                if active_count > 0 or total_count > 0:
                    print(f"📊 Status: {active_count} active instances, {total_count} total instances managed")
                elif self.di_change_counter % 50 == 0:  # Print status every 50 cycles when no activity
                    print(f"💤 Monitoring active... Total instances processed: {self.di_change_counter}")
                
                time.sleep(0.5)  # Check every 0.5 seconds for transitions
                
            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                time.sleep(1)
            finally:
                if connection:
                    connection.close()

    def cleanup(self):
        """Clean up resources when shutting down"""
        print("🧹 Cleanup called. Clearing all instances.")
        self.di_change_instances.clear()

if __name__ == "__main__":
    # Create and run the service
    try:
        monitoring_service = ContinuousDIMonitoringService(DB_CONFIG)
        monitoring_service.run()
    except KeyboardInterrupt:
        print("⏹️ Service interrupted by user")
    except Exception as e:
        print(f"❌ Service terminated due to error: {e}")
    finally:
        # Clean up if possible
        if 'monitoring_service' in locals():
            monitoring_service.cleanup()