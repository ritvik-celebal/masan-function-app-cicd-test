import azure.functions as func
import json
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function để lấy thông tin user permissions từ bảng OMS_PERMISSION_V.
    Trả về danh sách factories, years, month_year và roles.
    """
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,Referer, User-Agent, Sec-Ch-Ua, Sec-Ch-Ua-Mobile, Sec-Ch-Ua-Platform, Accept",
        'Access-Control-Max-Age': '3600'
    }

    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)

    try:
        # Lấy dữ liệu từ request JSON
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"error": "Missing required fields in JSON payload"}), status_code=400, headers=headers)

        user_email = request_json.get("email")
        if not user_email:
            return func.HttpResponse(json.dumps({"error": "Missing required fields in JSON payload"}), status_code=400, headers=headers)

        # Databricks connection setup
        logging.info('Getting Databricks token.')
        credential = DefaultAzureCredential()
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        access_token = credential.get_token(databricks_resource_id + "/.default").token

        server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
        database = "default"

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                # Query to check if email exists and retrieve associated factory codes
                query = f"""
                SELECT FACTORY_CODE, FACTORY_NAME
                FROM udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V 
                WHERE EMAIL = '{user_email}'
                GROUP BY FACTORY_CODE, FACTORY_NAME, FACTORY_NO 
                ORDER BY FACTORY_NO
                """
                
                cursor.execute(query)
                factory_results = cursor.fetchall()

                # Process the factory query result
                factories = []
                if factory_results:
                    columns = [desc[0] for desc in cursor.description]
                    for row in factory_results:
                        row_dict = dict(zip(columns, row))
                        if row_dict["FACTORY_CODE"]:
                            factories.append({
                                "Code": row_dict["FACTORY_CODE"], 
                                "Name": row_dict["FACTORY_NAME"]
                            })

                # Process the query years
                queryGetYears = f"""
                    WITH RECURSIVE year_list AS (
                    SELECT 2023 AS year
                    UNION ALL
                    SELECT year + 1
                    FROM year_list
                    WHERE year < EXTRACT(YEAR FROM CURRENT_DATE())
                    )
                    SELECT year
                    FROM year_list
                    ORDER BY year DESC
                """
                
                cursor.execute(queryGetYears)
                year_results = cursor.fetchall()
                
                years = []
                if year_results:
                    for row in year_results:
                        if row[0]:  # year is first column
                            years.append(row[0])

        if factories:
            response = {
                "code": 200,
                "years": years,
                "factories": factories,
                "month_year": ["11/2024", "12/2024", "01/2025"],
                "roles": [{"role_code": "Admin"},{"role_code": "Factory_Lead"},{"role_code": "Pillar_Lead"},{"role_code": "User"}]
            }
            return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)
        else:
            response = {
                "code": 400,
                "years": years or [],
                "factories": factories or [],
                "month_year": ["11/2024", "12/2024", "01/2025"],
                "roles": [{"role_code": "Admin"},{"role_code": "Factory_Lead"},{"role_code": "Pillar_Lead"},{"role_code": "User"}]
            }
            return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)

    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500, headers=headers)
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500, headers=headers)
    except Exception as e:
        # Handle and log any exceptions
        logging.error(f"Error: {e}")
        error_response = {
            "code": 500,
            "message": "Lỗi",
            "details": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)