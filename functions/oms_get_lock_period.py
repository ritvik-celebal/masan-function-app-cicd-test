import azure.functions as func
import json
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql

app = func.FunctionApp()

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function lấy dữ liệu từ bảng OMS_LOCK_PERIOD_F.
    Trả về danh sách các thông tin đã lọc theo Month, Factory_Code, Pillar_Code và email.
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
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing JSON payload"}), status_code=400, headers=headers)

        # Lấy các tham số truyền vào
        month = int(request_json.get("Month"))
        year = int(request_json.get("Year"))
        factory_codes = request_json.get("Factory_Code", [])
        pillar_codes = request_json.get("Pillar_Code", [])
        email = request_json.get("Email")

        # Kiểm tra các tham số đầu vào
        if not month or not email:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing required parameters: Month or Email"}), status_code=400, headers=headers)

        # Xây dựng điều kiện WHERE
        where_clauses = [f"V.EMAIL = '{email}'"]
        if factory_codes:
            factory_list = "', '".join(factory_codes)
            where_clauses.append(f"V.FACTORY_CODE IN ('{factory_list}')")
        if pillar_codes:
            pillar_list = "', '".join(pillar_codes)
            where_clauses.append(f"V.PILLAR_CODE IN ('{pillar_list}')")

        where_clause = " AND ".join(where_clauses)

        # Xây dựng truy vấn Databricks
        query = f"""
            SELECT V.Factory_Code, V.Pillar_Code, 
                   CASE WHEN {year} * 100 + {month} <= YEAR_MONTH  
                   AND L.Is_Active IS NOT NULL THEN L.Is_Active ELSE 1 END AS Is_Active,
                   LP.CREATED_BY, LP.CREATED_DATE
            FROM udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V V

            LEFT JOIN (SELECT L.Factory_Code, L.Pillar_Code, L.Is_Active, MAX(YEAR * 100 + MONTH) AS YEAR_MONTH
            FROM udp_wcm_dev.MFG_OMS.OMS_LOCK_PERIOD_F L
            GROUP BY L.Factory_Code, L.Pillar_Code, L.Is_Active) L
            ON V.Factory_Code = L.Factory_Code AND V.Pillar_Code = L.Pillar_Code

            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LOCK_PERIOD_F LP
            ON V.Factory_Code = LP.Factory_Code AND V.Pillar_Code = LP.Pillar_Code
            AND LP.Year = {year} AND LP.Month = {month}

            WHERE {where_clause}
            ORDER BY V.Factory_No * 100 + V.Pillar_No
        """

        # In câu truy vấn ra log để kiểm tra
        logging.info("Generated SQL Query:")
        logging.info(query)

        # Databricks connection and query
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
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()

                # Tạo danh sách kết quả
                data = {}
                for row in results:
                    row_dict = dict(zip(columns, row))
                    factory_code = row_dict["Factory_Code"]
                    if factory_code not in data:
                        data[factory_code] = {
                            "Factory_Code": factory_code,
                            "Pillars": []
                        }
                    data[factory_code]["Pillars"].append({
                        "Pillar_Code": row_dict["Pillar_Code"],
                        "Is_Active": row_dict["Is_Active"],
                        "Created_By": row_dict["CREATED_BY"],
                        "Created_Date": row_dict["CREATED_DATE"].strftime("%Y-%m-%d %H:%M:%S") if row_dict["CREATED_DATE"] else None,
                        "Format_Active": {
                            "backgroundColor": "#ffffff",
                            "color": "#000000"
                        },
                        "Format_Inactive": {
                            "backgroundColor": "#cce5ff",
                            "color": "#000000"
                        }
                    })

        # Trả về kết quả
        response = {
            "code": 200,
            "status": "success",
            "data": list(data.values())
        }
        return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)

    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500, headers=headers)
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500, headers=headers)
    except Exception as e:
        # Xử lý lỗi nếu có
        logging.error(f"Error: {e}")
        error_response = {
            "code": 500,
            "status": "error",
            "message": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)