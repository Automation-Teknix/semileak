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


# <!DOCTYPE html>
# <html lang="en">
# <head>
#     <meta charset="utf-8">
#     <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
#     {% load static %}
    
#     <title>{% block title %}Admin Page{% endblock %}</title>
#     <meta name="description" content="Admin Page Description">
#     <meta name="csrf-token" content="{{ csrf_token }}">
#     <!-- Bootstrap CSS -->
#     <link rel="stylesheet" href="{% static 'css/bootstrap.min.css' %}">
    
#     <style>
#         body {
#             font-family: Arial, sans-serif;
#             background-color: #f0f0f0;
#             margin: 0;
#             padding: 0;
#         }

#         .custom-navbar {
#             background-color: #062A63;
#             color: #ecf0f1;
#             text-align: center;
#             padding: 10px 0;
#         }

#         .custom-navbar ul {
#             list-style: none;
#         }

#         .custom-navbar ul li {
#             display: inline;
#             margin-right: 20px;
#             font-size: larger;
#         }

#         .content {
#             max-width: 800px;
#             margin: 0 auto;
#             padding: 20px;
#             background-color: #fff;
#             box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
#             border-radius: 5px;
#         }

#         .footer {
#             background-color: #062A63;
#             color: #ecf0f1;
#             text-align: center;
#             padding: 5px;
#             position: fixed;
#             bottom: 0;
#             width: 100%;
#         }

#         .footer p {
#             margin: 0;
#             cursor: pointer;
#             color: #fff;
#             transition: color 0.3s ease;
#         }

#         .footer p:hover {
#             color: #3498db;
#         }

#         .live-clock {
#             margin-right: 15px;
#         }

#         /* Additional styles from second file */
#         .modal {
#             display: none;
#             position: fixed;
#             z-index: 1;
#             left: 0;
#             top: 0;
#             width: 100%;
#             height: 100%;
#             overflow: auto;
#             background-color: rgba(0, 0, 0, 0.4);
#             padding-top: 60px;
#         }

#         .modal-content {
#             background-color: #fefefe;
#             margin: 5% auto;
#             padding: 20px;
#             border: 1px solid #888;
#             max-width: 500px;
#             max-height: 80vh;
#             overflow-y: auto;
#         }

#         .close {
#             color: #aaa;
#             float: right;
#             font-size: 28px;
#             font-weight: bold;
#         }

#         .close:hover,
#         .close:focus {
#             color: black;
#             text-decoration: none;
#         }

#         .popup {
#             position: fixed;
#             bottom: 10px;
#             left: 50%;
#             transform: translateX(-50%);
#             background-color: #f44336;
#             color: white;
#             text-align: center;
#             border-radius: 20px;
#             padding: 16px;
#             z-index: 999;
#             display: none;
#         }

#         .show {
#             display: block;
#         }
#         .nav-link.active {
#             color: #fff !important;
#             background-color: #062A63;
#             border-radius: 5px;
#             padding: 8px 12px;
#         }

#     </style>

# </head>
# <body>

#     <nav class="navbar navbar-expand-sm custom-navbar navbar-dark">
#         <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" 
#             aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
#           <span class="navbar-toggler-icon"></span>
#         </button>
      
#         <div class="collapse navbar-collapse" id="navbarSupportedContent">
#             <ul class="navbar-nav mr-auto">
#                 <li class="nav-item" id="plat">
#                     <a href="{% url 'leakapp_list' %}" class="nav-link {% if request.resolver_match.url_name == 'leakapp_list' %}active{% endif %}">Parts</a>
#                 </li>
#                 <li class="nav-item" id="plat">
#                     <a href="{% url 'leak_test' %}" class="nav-link {% if request.resolver_match.url_name == 'leak_test' %}active{% endif %}">Leak Test</a>
#                 </li>
#                 <li class="nav-item" id="plat">
#                     <a href="{% url 'report' %}" class="nav-link {% if request.resolver_match.url_name == 'report' %}active{% endif %}">Report</a>
#                 </li>
#             </ul>
#           <form class="form-inline my-2 my-lg-0">
#             <div></div>
#             <div class="live-clock">    
#                 <span id="date">DD/MM/YYYY</span>
#                 <span id="clock">00:00:00</span>
#             </div>
#             <a class="btn btn-outline-primary my-2 my-sm-0" href="{% url 'admin:index' %}" style="margin-left: 15px;">Admin Panel</a>
#             <a class="btn btn-outline-success my-2 my-sm-0" href="{% url 'logout' %}" style="margin-left: 15px;">Logout</a> 
#           </form>
#         </div>
#     </nav>

#     <div class="container mt-4">
#         {% block content %}{% endblock %}
#     </div>

#     <div class="footer">
#         <p onclick="document.getElementById('contactModal').style.display='block'">Contact</p>
#     </div>

#     <div id="contactModal" class="modal">
#         <div class="modal-content">
#             <span class="close" onclick="document.getElementById('contactModal').style.display='none'">×</span>
#             <img src="{% static 'images/mainlogo.jpg' %}" alt="Logo">
#             <p>Sandeep Bhadkamkar</p>
#             <p><i class="fas fa-phone"></i> 9890033285</p>
#             <p><i class="fas fa-globe"></i> <a href="http://www.automationteknix.com" target="_blank">www.automationteknix.com</a></p>
#             <p><i class="fas fa-envelope"></i> <a href="mailto:sandeep@automationteknix.com">sandeep@automationteknix.com</a></p>
#         </div>
#     </div>
#     <script>
#       function updateClock() {
#         const clock = document.getElementById("clock");
#         const date = document.getElementById("date");
#         const now = new Date();

#         const hours = now.getHours().toString().padStart(2, '0');
#         const minutes = now.getMinutes().toString().padStart(2, '0');
#         const seconds = now.getSeconds().toString().padStart(2, '0');
#         clock.textContent = `${hours}:${minutes}:${seconds}`; 

#         const year = now.getFullYear();
#         const month = (now.getMonth() + 1).toString().padStart(2, '0');
#         const day = now.getDate().toString().padStart(2, '0');
#         date.textContent = `${day}/${month}/${year}`;
#       }
#       setInterval(updateClock, 1000);
#       updateClock();
#     </script>
# <script src="{% static 'js/sweetalert2.all.min.js' %}"></script>
# </body>
# </html>
