import azure.functions as func
import json
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql

app = func.FunctionApp()

@app.function_name(name="user_pillars_data")
@app.route(route="user_pillars_data", auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    
    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)

    try:
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"error": "Invalid or missing JSON payload"}), status_code=400, headers=headers)

        user_email = request_json.get("user_email")
        if not user_email:
            return func.HttpResponse(json.dumps({"error": "Missing required fields in JSON payload"}), status_code=400, headers=headers)

        query = f"""
                SELECT C.ORDER_NO, C.PILLAR_CODE, C.REPORT_NAME, C.SRC_IFRAME
        FROM udp_wcm_dev.MFG_OMS.OMS_CHART_CONFIG C
        WHERE PILLAR_CODE LIKE '%SUMMARY%'
        UNION ALL
        SELECT O.ORDER_NO * 100 + C.ORDER_NO AS ORDER_NO, C.PILLAR_CODE, C.REPORT_NAME, C.SRC_IFRAME
        FROM udp_wcm_dev.MFG_OMS.OMS_CHART_CONFIG C
        INNER JOIN udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V P
                    ON P.PILLAR_CODE = C.PILLAR_CODE
        LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D O
        ON C.PILLAR_CODE = O.PILLAR_CODE
        WHERE C.PILLAR_CODE NOT LIKE '%SUMMARY%' 
        AND P.EMAIL = '{user_email}'
        GROUP BY O.ORDER_NO * 100 + C.ORDER_NO, C.PILLAR_CODE, C.REPORT_NAME, C.SRC_IFRAME
        ORDER BY ORDER_NO
        """

        # Databricks connection
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

                # Process results into the expected structure
                pillars = []
                current_pillar = None
                for row in results:
                    row_dict = dict(zip(columns, row))
                    if not current_pillar or current_pillar["pillar_code"] != row_dict["PILLAR_CODE"]:
                        if current_pillar:
                            pillars.append(current_pillar)
                        current_pillar = {
                            "pillar_name": row_dict["PILLAR_CODE"],
                            "pillar_code": row_dict["PILLAR_CODE"],
                            "reports": []
                        }
                    current_pillar["reports"].append({
                        "report_name": row_dict["REPORT_NAME"],
                        "src_iframe": row_dict["SRC_IFRAME"]
                    })
                if current_pillar:
                    pillars.append(current_pillar)

        # Construct response
        if pillars:
            response = {
                "code": 200,
                "pillars": pillars,
            }
            return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)
        else:
            response = {
                "code": 400,
                "is_exist": 0,
                "pillars": [],
                "message": "Lỗi không có pillars."
            }
            return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=400, headers=headers)

    except socket.timeout:
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500, headers=headers)
    except urllib.error.URLError as e:
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500, headers=headers)
    except Exception as e:
        error_response = {
            "code": 500,
            "message": "Lỗi",
            "details": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)