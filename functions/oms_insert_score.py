
import azure.functions as func
import json
import logging
import socket
import urllib.error
import csv
import os
import tempfile
from datetime import datetime
import pytz
from azure.identity import DefaultAzureCredential
from databricks import sql
from azure.storage.blob import BlobServiceClient

# BigQuery table
project_id = "mch-dwh"
dataset_id = "MFG_OMS"
table_id = "OMS_SCORE_F"
lock_table_id = "OMS_LOCK_PERIOD_F"

bucket_name = "mfg_etl_staging"  # Thay bằng tên bucket của bạn

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function import dữ liệu batch vào bảng OMS_LOCK_PERIOD_LOG_F.
    Xử lý việc xóa dữ liệu cũ và import dữ liệu mới theo Year, Month, Factory_Code, Pillar_Code.
    """
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)

    try:
        # Lấy dữ liệu từ request JSON
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"error": "Missing or invalid JSON payload"}), status_code=400, headers=headers)

        year = request_json.get("YEAR")
        month = request_json.get("MONTH")
        factory_code = request_json.get("FACTORY")
        pillar_data = request_json.get("PILLAR", [])
        user_mail = request_json.get("user_mail")

        if year * 100 + month > datetime.now().year * 100 + datetime.now().month:
            return func.HttpResponse(json.dumps({"error": "Tháng import không được lớn hơn tháng hiện tại"}), status_code=400, headers=headers)

        if not all([year, month, factory_code, pillar_data, user_mail]):
            return func.HttpResponse(json.dumps({"error": "Missing required fields: YEAR, MONTH, FACTORY, PILLAR, user_mail"}), status_code=400, headers=headers)

        # Databricks connection setup
        logging.info('Getting Databricks token.')
        credential = DefaultAzureCredential()
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        access_token = credential.get_token(databricks_resource_id + "/.default").token

        server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
        database = "default"

        # Check lock periods
        factory_code_list = "', '".join([factory_code])
        pillar_code_list = "', '".join([pillar["PILLAR"] for pillar in pillar_data])
        lock_check_query = f"""
        SELECT COUNT(*) AS lock_count
        FROM udp_wcm_dev.MFG_OMS.OMS_LOCK_PERIOD_LOG_F
        WHERE {year} * 100 + {month} <= YEAR * 100 + MONTH
          AND FACTORY_CODE IN ('{factory_code_list}')
          AND PILLAR_CODE IN ('{pillar_code_list}')
          AND IS_ACTIVE = 0
        """

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(lock_check_query)
                lock_result = cursor.fetchone()
                lock_count = lock_result[0] if lock_result else 0

                if lock_count > 0:
                    return func.HttpResponse(json.dumps({"error": "Không Import được, do Tháng Import đã bị khóa."}), status_code=400, headers=headers)

        # Prepare data
        
        filename = f"batch_load_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        local_file_path = os.path.join(tempfile.gettempdir(), filename)
        rows_written = 0

        with open(local_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["partition_key", "YEAR", "MONTH", "FACTORY_CODE", "PILLAR_CODE", "ITEM_CODE", "SCORE", "CREATED_BY", "CREATED_DATE"])

            gmt7 = pytz.timezone('Asia/Ho_Chi_Minh')
            created_date = datetime.now(pytz.utc).astimezone(gmt7).strftime("%Y-%m-%d %H:%M:%S")

            for pillar in pillar_data:
                pillar_code = pillar.get("PILLAR")
                for kpi in pillar.get("KPI", []):
                    kpi_code = kpi.get("KPI")
                    score = kpi.get("SCORE")
                    chapter_code = kpi.get("CHAPTER_CODE")

                    if score is None or score == "":
                        return func.HttpResponse(json.dumps({"error": f"SCORE bị thiếu tại item '{kpi_code}' (PILLAR: {pillar_code})"}), status_code=400, headers=headers)

                    try:
                        score = int(score)
                    except (ValueError, TypeError):
                        return func.HttpResponse(json.dumps({"error": f"SCORE không hợp lệ tại item '{kpi_code}' (PILLAR: {pillar_code}). SCORE phải là số nguyên!"}), status_code=400, headers=headers)

                    if kpi_code is not None and not kpi_code.startswith("SAF00"):
                        allowed_scores = [-1, 0, 1, 2, 3]
                        if score not in allowed_scores:
                            return func.HttpResponse(json.dumps({"error": f"SCORE không hợp lệ tại item '{kpi_code}' (PILLAR: {pillar_code}). Chỉ cho phép nhập {allowed_scores} cho các item không bắt đầu = SAF00"}), status_code=400, headers=headers)

                    partition_key = f"{year}-{str(month).zfill(2)}-01"
                    writer.writerow([partition_key, year, month, factory_code, pillar_code, kpi_code, score, user_mail, created_date])
                    rows_written += 1

        # ✅ Nếu không có dòng hợp lệ thì KHÔNG xoá và dừng luôn
        if rows_written == 0:
            return func.HttpResponse(json.dumps({"error": "Không có dữ liệu nào để import!"}), status_code=400, headers=headers)

        # ✅ Chỉ xoá dữ liệu trước khi insert
        delete_query = f"""
        DELETE FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F
        WHERE YEAR = {year} AND MONTH = {month} AND FACTORY_CODE = '{factory_code}'
        """
        pillar_conditions = " OR ".join([f"PILLAR_CODE = '{pillar.get('PILLAR')}'" for pillar in pillar_data])
        if pillar_conditions:
            delete_query += f" AND ({pillar_conditions})"

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_query)

        # Upload CSV to Azure Blob Storage and load to Databricks
        # Note: This would typically require additional configuration for blob storage connection string
        # and proper Databricks table loading mechanism
        

        
        account_url = "https://udpdatawarehousedev.blob.core.windows.net"
        container_name = "container-udp-adls-dev"
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Load data into Databricks table
        # Note: This would typically use COPY INTO or similar Databricks SQL command
        copy_query = f"""
        COPY INTO udp_wcm_dev.MFG_OMS.OMS_SCORE_F
        FROM 'abfss://{container_name}@udpdatawarehousedev.dfs.core.windows.net/{filename}'
        FILEFORMAT = CSV
        """
        # FORMAT_OPTIONS ('mergeSchema' = 'true', 'header' = 'true')
        # COPY_OPTIONS ('mergeSchema' = 'true')

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(copy_query)

        # Cleanup
        os.remove(local_file_path)
        blob_client.delete_blob()

        return func.HttpResponse(
            json.dumps({
                "code": 200,
                "message": f"Lưu thành công. Số dòng lưu: {rows_written}"
            }, ensure_ascii=False, indent=4), 
            status_code=200, 
            headers=headers
        )

    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500, headers=headers)
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500, headers=headers)
    except Exception as e:
        logging.error(f"Error: {e}")
        error_response = {
            "code": 500,
            "status": "error", 
            "message": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)