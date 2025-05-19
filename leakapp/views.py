import io
import json
import base64
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import authenticate, login, logout 
from django.urls import reverse
from django.views import View
from django.db import connection
from django.contrib.auth.decorators import login_required
from datetime import datetime
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt
from django.http import JsonResponse
from django.db.models import Max
from leakapp.forms import LeakAppMasterDataForm, LeakAppTestForm
from leakapp.models import LeakAppMasterData, LeakAppTest, LeakAppShowReport, LeakAppResult, Shift, FOI, myplclog
# Create your views here.

def user_login(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('leak_test')
        else:
            messages.error(request, 'Invalid username or password')
    return render(request, 'login.html')

def logout_view(request):
    logout(request)
    return redirect(reverse('login'))


class LeakAppMasterDataView(View):
    def get(self, request, pk=None):
        if pk:
            instance = get_object_or_404(LeakAppMasterData, pk=pk)
            form = LeakAppMasterDataForm(instance=instance)
        else:
            form = LeakAppMasterDataForm()

        data = LeakAppMasterData.objects.all()
        return render(request, 'master.html', {'form': form, 'data': data})

    def post(self, request, pk=None):
        part_number = request.POST.get("part_number", "").strip()

        if LeakAppMasterData.objects.filter(part_number=part_number).exists():
            return JsonResponse({"error": "Part number already exists."}, status=400)

        form = LeakAppMasterDataForm(request.POST)
        if form.is_valid():
            form.save()
            return JsonResponse({"message": "Record added successfully!"}, status=201)

        return JsonResponse({"error": "Invalid data."}, status=400)

    def delete(self, request, pk):
        instance = get_object_or_404(LeakAppMasterData, pk=pk)
        instance.delete()
        return JsonResponse({"message": "Record deleted successfully!"}, status=200)

@login_required(login_url="login")
def search_part_numbers(request):
    query = request.GET.get("query", "").strip()
    if query:
        results = LeakAppMasterData.objects.filter(part_number__icontains=query).values("id", "part_number")
        return JsonResponse({"results": list(results)})
    return JsonResponse({"results": []})

@login_required(login_url="login")
def leak_test_page(request):
    """Fetches and returns leak test data for a selected part number."""
    part_number = request.GET.get('part_number', '').strip()
    filter_numbers = [f'AI{i}' for i in range(1, 19)]  # AI1 to AI16 (Fixed)

    # Default empty data for all filters
    latest_data = {filter_no: {"leakage_value": "-", "highest_value": "-", "status": "-", 
                              "leakage_timestamp": None, "highest_timestamp": None} 
                  for filter_no in filter_numbers}

    if part_number:
        # Get highest values first
        highest_query = """
            SELECT
                filter_no,
                highest_value,
                status,
                date as highest_date
            FROM
                leakapp_test
            WHERE
                part_number_id = %s
                AND date = (
                    SELECT MAX(date)
                    FROM leakapp_test
                    WHERE filter_no = leakapp_test.filter_no
                    AND part_number_id = %s
                )
        """
        
        # Get leakage values separately
        leakage_query = """
            SELECT
                filter_no,
                filter_values AS leakage_value,
                date as leakage_date
            FROM
                leakapp_result_tbl
            WHERE
                filter_no IN (SELECT filter_no FROM leakapp_test WHERE part_number_id = %s)
                AND date = (
                    SELECT MAX(date)
                    FROM leakapp_result_tbl
                    WHERE filter_no = leakapp_result_tbl.filter_no
                )
        """

        # Execute the highest value query
        with connection.cursor() as cursor:
            cursor.execute(highest_query, [part_number, part_number])
            highest_rows = cursor.fetchall()
            
            # Process highest value data
            for row in highest_rows:
                filter_no, highest_value, status, highest_date = row
                if filter_no in latest_data:
                    latest_data[filter_no].update({
                        "highest_value": highest_value or "-",
                        "status": status or "-",
                        "highest_timestamp": highest_date.timestamp() if highest_date else None
                    })
        
        # Execute the leakage value query
        with connection.cursor() as cursor:
            cursor.execute(leakage_query, [part_number])
            leakage_rows = cursor.fetchall()
            
            # Process leakage value data
            for row in leakage_rows:
                filter_no, leakage_value, leakage_date = row
                if filter_no in latest_data:
                    latest_data[filter_no].update({
                        "leakage_value": leakage_value or "-",
                        "leakage_timestamp": leakage_date.timestamp() if leakage_date else None
                    })

    # Check if request is Ajax to send back JSON response
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({"latest_data": latest_data}, safe=False)
    
    # For the initial load of the page
    return render(request, 'leak_test.html', {
        "latest_data": latest_data,
        "filter_names": filter_numbers
    })


@login_required(login_url="login")
def leak_test_view(request):
    """Fetches and returns leak test data for a selected part number."""
    part_number = request.GET.get('part_number', '').strip()
    filter_numbers = [f'AI{i}' for i in range(1, 19)]  # AI1 to AI16 (Fixed)

    # Default empty data
    latest_data = {filter_no: {"leakage_value": "-", "highest_value": "-", "status": "-", 
                              "leakage_timestamp": None, "highest_timestamp": None} 
                  for filter_no in filter_numbers}

    if part_number:
        # Initialize dictionary to track which filters we've found data for
        found_filters = set()
        
        # Fetch the latest records from LeakAppTest for the selected part_number
        query = LeakAppTest.objects.filter(part_number__part_number=part_number)
        latest_records = query.values('filter_no').annotate(latest_date=Max('date'))
        
        # Process highest value entries
        for record in latest_records:
            filter_no = record['filter_no']
            found_filters.add(filter_no)
            
            # Fetch the corresponding latest entry from LeakAppTest
            latest_entry = query.filter(filter_no=filter_no, date=record['latest_date']).first()
            
            if latest_entry and filter_no in latest_data:
                latest_data[filter_no]["highest_value"] = latest_entry.filter_values or "-"
                latest_data[filter_no]["status"] = latest_entry.status or "-"
                latest_data[filter_no]["highest_timestamp"] = latest_entry.date.timestamp() if latest_entry.date else None

        # Fetch and process leakage values independently
        myplcprodstatus = myplclog.objects.get(id=1)
        
        # Determine which model to use for leakage values based on production status
        model_to_use = FOI if myplcprodstatus.prodstatus == 1 else LeakAppResult
        
        # Get all available filter numbers from the leakage model
        leakage_filter_nos = model_to_use.objects.values_list('filter_no', flat=True).distinct()
        
        for filter_no in leakage_filter_nos:
            if filter_no in filter_numbers:  # Make sure it's one of our valid filters
                try:
                    leakage_entry = model_to_use.objects.filter(filter_no=filter_no).latest('date')
                    
                    # Update the leakage value and timestamp
                    latest_data[filter_no]["leakage_value"] = leakage_entry.filter_values or "-"
                    latest_data[filter_no]["leakage_timestamp"] = leakage_entry.date.timestamp() if leakage_entry.date else None
                    
                    # Add to found filters set
                    found_filters.add(filter_no)
                except (model_to_use.DoesNotExist, AttributeError):
                    pass

    # Return JSON response with the latest data
    return JsonResponse({"latest_data": latest_data}, safe=False)


@login_required(login_url="login")
def search_part_numbers_for_leak_test(request):
    """Returns a list of matching part numbers for the dropdown."""
    query = request.GET.get("query", "").strip()
    results = []

    if query:
        results = list(LeakAppMasterData.objects.filter(part_number__icontains=query).values("id", "part_number"))

    return JsonResponse({"results": results}, safe=False)


def dictfetchall(cursor):
    """Helper function to convert cursor results into a dictionary."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@login_required(login_url="login")
def report_screen(request):
    selected_date = request.GET.get("date", "")
    selected_month = request.GET.get("month", "")
    selected_year = request.GET.get("year", "")
    selected_part_number = request.GET.get("part_number", "")
    print(selected_part_number)
    selected_status = request.GET.get("status", "")
    selected_shift = request.GET.get("shift", "")

    sql_query = "SELECT * FROM leakapp_show_report WHERE 1=1"
    sql_params = []

    if selected_date:
        try:
            selected_date = datetime.strptime(selected_date, "%Y-%m-%d").date()
            sql_query += " AND DATE(date) = %s"
            sql_params.append(selected_date)
        except ValueError:
            pass

    elif selected_month and selected_year:
        try:
            selected_month = int(selected_month)
            selected_year = int(selected_year)
            sql_query += " AND YEAR(date) = %s AND MONTH(date) = %s"
            sql_params.extend([selected_year, selected_month])
        except ValueError:
            pass

    elif selected_year:
        try:
            selected_year = int(selected_year)
            sql_query += " AND YEAR(date) = %s"
            sql_params.append(selected_year)
        except ValueError:
            pass

    if selected_shift:
        try:
            shift = Shift.objects.get(shift_name=selected_shift)
            sql_query += " AND shift_id = %s"
            sql_params.append(shift.id)
        except Shift.DoesNotExist:
            sql_query += " AND 1=0"
    if selected_part_number:
        print("part Selected")
        try:
            part = LeakAppMasterData.objects.get(id=selected_part_number)
            print(part)
            with connection.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM leakapp_show_report LIKE 'part_number_id'")
                is_foreign_key = bool(cursor.fetchone())

            if is_foreign_key:
                sql_query += " AND part_number_id = %s"
                sql_params.append(part.id)
            else:
                sql_query += " AND part_number_id = %s"
                sql_params.append(part.part_number)
        except LeakAppMasterData.DoesNotExist:
            sql_query += " AND 1=0"

    if selected_status:
        sql_query += " AND status = %s"
        sql_params.append(selected_status)

    with connection.cursor() as cursor:
        cursor.execute(sql_query, sql_params)
        report_data = dictfetchall(cursor)

    ok_count = sum(1 for report in report_data if report["status"] == "OK")
    nok_count = sum(1 for report in report_data if report["status"] == "NOK")

    chart = None
    if ok_count + nok_count > 0:
        labels = ['OK', 'NOK']
        sizes = [ok_count, nok_count]
        colors = ['#88EA16', '#FF0000']

        plt.figure(figsize=(2, 2))
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')

        # Save to BytesIO buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        chart = base64.b64encode(buf.getvalue()).decode('utf-8')
        buf.close()

    batch_counters = LeakAppShowReport.objects.values_list("batch_counter", flat=True).distinct()
    part_numbers = LeakAppMasterData.objects.all()
    shifts = Shift.objects.all()
    statuses = LeakAppShowReport.objects.values_list("status", flat=True).distinct()
    years = list(range(2000, 2099))

    context = {
        "report_data": report_data,
        "single_data": report_data[0] if len(report_data) == 1 else None,
        "date": selected_date.strftime("%Y-%m-%d") if selected_date else "",
        "month": selected_month if selected_month else "",
        "year": selected_year if selected_year else "",
        "selected_part_number": selected_part_number,
        "selected_status": selected_status,
        "batch_counters": batch_counters,
        "part_numbers": part_numbers,
        "statuses": statuses,
        "shifts": shifts,
        "years": years,
        "ok_count": ok_count,
        "nok_count": nok_count,
        "chart": chart,
    }
    return render(request, "report_screen.html", context)

@csrf_exempt
@login_required(login_url="login")
def store_leak_test_data(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            part_number = data.get("part_number")
            leak_data = data.get("leak_data", [])

            # Get Part Instance
            try:
                part_instance = LeakAppMasterData.objects.get(part_number=part_number)
            except LeakAppMasterData.DoesNotExist:
                return JsonResponse({"success": False, "error": "Part not found"}, status=400)

            # Get the latest batch counter value
            latest_batch = FOI.objects.order_by("-batch_counter").first()
            batch_counter = latest_batch.batch_counter + 1 if latest_batch else 1

            # Get Shift (Modify logic as per your requirement)
            shift_instance = Shift.objects.first()

            # Store only non-empty records
            for record in leak_data:
                FOI.objects.create(
                    batch_counter=batch_counter,
                    part_number=part_instance,
                    filter_no=record["filter_no"],
                    filter_values=record["leakage_value"],
                    highest_value=record["highest_value"],
                    status=record["status"],
                    shift=shift_instance
                )

            return JsonResponse({"success": True})

        except Exception as e:
            return JsonResponse({"success": False, "error": str(e)}, status=500)

    return JsonResponse({"success": False, "error": "Invalid request"}, status=400)

@csrf_exempt
def update_prodstatus(request):
    if request.method == "POST":
        data = json.loads(request.body)
        part_number = data.get('part_number')
        prodstatus = data.get('prodstatus')

        # Check if the part_number exists in LeakAppMasterData
        try:
            leak_data = LeakAppMasterData.objects.get(part_number=part_number)
        except LeakAppMasterData.DoesNotExist:
            return JsonResponse({"success": False, "error": "Part number not found."}, status=404)

        # Get the existing plc_log entry with id=1, or create it if not present
        plc_log_entry, created = myplclog.objects.get_or_create(id=1)

        # Update the prodstatus field without changing part_number
        plc_log_entry.prodstatus = prodstatus
        plc_log_entry.save()

        return JsonResponse({"success": True})

    return JsonResponse({"success": False, "error": "Invalid request method."}, status=400)


@csrf_exempt
def update_part_log(request):
    if request.method == "POST":
        data = json.loads(request.body)
        part_number = data.get('part_number')

        # Check if the part_number exists in LeakAppMasterData
        try:
            leak_data = LeakAppMasterData.objects.get(part_number=part_number)
        except LeakAppMasterData.DoesNotExist:
            return JsonResponse({"success": False, "error": "Part number not found."}, status=404)

        # Get the existing plc_log entry with id=1, or create it if not present
        plc_log_entry, created = myplclog.objects.get_or_create(id=1)

        # Update the part_number field in the record
        plc_log_entry.part_number = leak_data
        plc_log_entry.save()

        return JsonResponse({"success": True})

    return JsonResponse({"success": False, "error": "Invalid request method."}, status=400)

@login_required(login_url="login")
def get_server_status(request):
    try:
        # Using raw SQL for better performance
        with connection.cursor() as cursor:
            cursor.execute("SELECT server_connection_1, server_connection_2 FROM myplclog LIMIT 1;")
            row = cursor.fetchone()
            
            if not row:
                return JsonResponse({"error": "No log data available"}, status=404)
            
            server_status = {
                "server_connection_1": row[0],  # first column
                "server_connection_2": row[1],  # second column
            }
            return JsonResponse(server_status)
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
