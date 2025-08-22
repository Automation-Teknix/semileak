from django.urls import path, include, re_path
from leakapp import views
from django.views.static import serve
from django.conf import settings
from django.contrib import admin

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.user_login, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('leakapp', views.LeakAppMasterDataView.as_view(), name='leakapp_list'),
    path('leakapp/edit/<int:pk>/', views.LeakAppMasterDataView.as_view(), name='leakapp_edit'),
    path('leakapp/delete/<int:pk>/', views.LeakAppMasterDataView.as_view(), name='leakapp_delete'),
    path('leakapp/search/', views.search_part_numbers, name='leakapp_search'),
    path('report', views.report_screen, name='report'),
    path("search-part-numbers/", views.search_part_numbers_for_leak_test, name="search_part_numbers"),
    path("get-latest-leak-data/", views.leak_test_page, name="leak_test"),
    path("leak-test/", views.leak_test_view, name="result_leak_test"),
    path("save-leak-test-data/", views.store_leak_test_data, name="save_leak_test_data"),
    path('update-prodstatus/', views.update_prodstatus, name='update_prodstatus'),
    path('update-part-log/', views.update_part_log, name='update_part_log'),
    path('get-server-status/', views.get_server_status, name='get_server_status'),
    re_path(r'^media/(?P<path>.*)$',serve,{'document_root' : settings.MEDIA_ROOT}),
    re_path(r'^static/(?P<path>.*)$',serve,{'document_root' : settings.STATIC_ROOT}),
]
