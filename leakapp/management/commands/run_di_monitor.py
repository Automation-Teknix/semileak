from django.core.management.base import BaseCommand
from django.conf import settings
import threading
import time
import sys
from leakapp.monitoring import DIMonitoringService

class Command(BaseCommand):
    help = 'Runs the DI monitoring service'

    def handle(self, *args, **options):
        self.stdout.write('Starting DI monitoring service...')
        sys.stdout.reconfigure(line_buffering=True)  # Ensure logs flush immediately

        db_config = {
            'host': settings.DATABASES['default']['HOST'] or 'localhost',
            'user': settings.DATABASES['default']['USER'],
            'password': settings.DATABASES['default']['PASSWORD'],
            'database': settings.DATABASES['default']['NAME'],
        }

        monitoring_service = DIMonitoringService(db_config)

        monitor_thread = threading.Thread(target=monitoring_service.run)
        monitor_thread.daemon = True
        monitor_thread.start()

        self.stdout.write('DI monitoring service is now running...\n')

        try:
            while True:
                time.sleep(0.01)
        except KeyboardInterrupt:
            self.stdout.write("Monitoring service stopped by user.")
