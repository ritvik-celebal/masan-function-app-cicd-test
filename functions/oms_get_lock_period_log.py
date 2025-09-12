import azure.functions as func
import json
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function lấy dữ liệu từ bảng OMS_LOCK_PERIOD_LOG_F.
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

        # Xây dựng điều kiện WHERE
        where_clauses = [f"YEAR = {year} AND MONTH = {month}"]
        if factory_codes:
            factory_list = "', '".join(factory_codes)
            where_clauses.append(f"FACTORY_CODE IN ('{factory_list}')")
        if pillar_codes:
            pillar_list = "', '".join(pillar_codes)
            where_clauses.append(f"PILLAR_CODE IN ('{pillar_list}')")

        where_clause = " AND ".join(where_clauses)

        # Xây dựng truy vấn Databricks
        query = f"""
            SELECT YEAR, MONTH, FACTORY_CODE, PILLAR_CODE, 
                   CASE WHEN IS_ACTIVE = 1 THEN 'Unlock' ELSE 'Lock' END AS STATUS, 
                   CREATED_BY, CREATED_DATE
            FROM udp_wcm_dev.MFG_OMS.OMS_LOCK_PERIOD_LOG_F
            WHERE {where_clause}
            ORDER BY YEAR, MONTH, FACTORY_CODE, PILLAR_CODE, CREATED_DATE DESC
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
                data = []
                for row in results:
                    row_dict = dict(zip(columns, row))
                    data.append({
                        "Year": row_dict["YEAR"],
                        "Month": row_dict["MONTH"],
                        "Factory_Code": row_dict["FACTORY_CODE"],
                        "Pillar_Code": row_dict["PILLAR_CODE"],
                        "Status": row_dict["STATUS"],
                        "Created_By": row_dict["CREATED_BY"] if row_dict["CREATED_BY"] else None,
                        "Created_Date": row_dict["CREATED_DATE"].strftime("%Y-%m-%d %H:%M:%S") if row_dict["CREATED_DATE"] else None
                    })

        # Trả về kết quả
        response = {
            "code": 200,
            "status": "success",
            "data": data
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