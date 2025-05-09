from django.contrib import admin
from import_export.admin import ImportExportModelAdmin
from .models import User, LeakAppMasterData, FOI, LeakAppTest, Shift, LeakAppShowReport, LeakAppResult, myplclog
from .resources import LeakAppMasterDataResource, LeakAppResultResource, LeakAppShowReportResource, LeakAppTestResource, FOIResource, ShiftResource


class UserAdmin(admin.ModelAdmin):
    list_display = ('username', 'email')
    search_fields = ('username', 'email')
    ordering = ('username',)


class LeakAppMasterDataAdmin(ImportExportModelAdmin):
    resource_class = LeakAppMasterDataResource
    list_display = ('id', 'part_number', 'greater_less', 'setpoint1', 'setpoint2', 'timer1', 'timer2')
    search_fields = ('part_number',)
    list_filter = ('greater_less',)
    ordering = ('part_number',)

admin.site.register(LeakAppMasterData, LeakAppMasterDataAdmin)


class FOIAdmin(ImportExportModelAdmin):
    resource_class = FOIResource
    list_display = ('part_number', 'batch_counter', 'filter_no', 'filter_values', 'date', 'shift')
    search_fields = ('filterno',)
    list_filter = ('shift',)
    ordering = ('-date',)

admin.site.register(FOI, FOIAdmin)


class LeakAppTestAdmin(ImportExportModelAdmin):
    resource_class = LeakAppTestResource
    list_display = ('part_number', 'batch_counter', 'filter_no', 'filter_values', 'date', 'shift')
    search_fields = ('filter_no',)
    list_filter = ('batch_counter',)
    ordering = ('-date',)

admin.site.register(LeakAppTest, LeakAppTestAdmin)


class ShiftAdmin(ImportExportModelAdmin):
    resource_class = ShiftResource
    list_display = ('id', 'shift_name', 'start_time', 'end_time')
    search_fields = ('shift_name',)
    ordering = ('start_time',)
admin.site.register(Shift, ShiftAdmin)


class LeakAppShowReportAdmin(ImportExportModelAdmin):
    resource_class = LeakAppShowReportResource
    list_display = ('id', 'batch_counter', 'part_number', 'filter_no', 'filter_values', 'status', 'shift', 'highest_value', 'date')
    search_fields = ('filter_no', 'status')
    list_filter = ('status', 'shift')
    ordering = ('-date',)

admin.site.register(LeakAppShowReport, LeakAppShowReportAdmin)


# class LeakAppResultAdmin(ImportExportModelAdmin):
#     resource_class = LeakAppResultResource
#     list_display = ('batch_counter', 'part_number', 'filter_no', 'filter_values', 'status', 'shift', 'date', 'filter_counter_by_system', 'iot_value')
#     search_fields = ('filter_no', 'status')
#     list_filter = ('status', 'shift')
#     ordering = ('-date',)

# admin.site.register(LeakAppResult, LeakAppResultAdmin)

@admin.register(myplclog)
class MyPLCLog(admin.ModelAdmin):
    list_display = ['id', 'prodstatus']
    list_filter = ['prodstatus']
    search_fields = ['prodstatus']

# Customize Django Admin Branding
admin.site.site_header = "Auto Leak Testing Reporting System"
admin.site.site_title = "LeakApp Admin"
admin.site.index_title = "Welcome to Leak Reporting System"
