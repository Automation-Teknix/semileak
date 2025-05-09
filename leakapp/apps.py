from django.apps import AppConfig

class LeakappConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'leakapp'
    verbose_name = "Auto Leak Testing"

    # def ready(self):
    #     return "leakapp.signals"