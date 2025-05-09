from import_export import resources
from .models import User, LeakAppMasterData, FOI, LeakAppTest, Shift, LeakAppShowReport, LeakAppResult


# Resource classes for import-export
class LeakAppMasterDataResource(resources.ModelResource):
    class Meta:
        model = LeakAppMasterData

class FOIResource(resources.ModelResource):
    class Meta:
        model = FOI

class LeakAppTestResource(resources.ModelResource):
    class Meta:
        model = LeakAppTest

class ShiftResource(resources.ModelResource):
    class Meta:
        model = Shift

class LeakAppShowReportResource(resources.ModelResource):
    class Meta:
        model = LeakAppShowReport

class LeakAppResultResource(resources.ModelResource):
    class Meta:
        model = LeakAppResult
