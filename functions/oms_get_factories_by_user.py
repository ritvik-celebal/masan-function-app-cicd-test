import azure.functions as func
import json
import jwt
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql


def main(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP Cloud Function to handle CORS, JWT authentication, and fetch data from Databricks."""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    print(req.get_body())
    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)

    try:
        # token = req.headers.get('Authorization', '').split('Bearer ')[-1]
        # decoded_payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        # user = decrypt_aes_json(decoded_payload['user'])
        
        request_json = req.get_json()
        print(request_json)
        if not request_json:
            return func.HttpResponse(json.dumps({"error": "Invalid or missing JSON payload"}), status_code=400, headers=headers)
        

        user_email = request_json.get("user_mail")
        pillar_code = request_json.get("pillar_code")
        if not all([user_email, pillar_code]):
            return func.HttpResponse(json.dumps({"error": "Missing required fields in JSON payload"}), status_code=400, headers=headers)

        # Databricks connection and query
        logging.info('Getting Databricks token.')
        credential = DefaultAzureCredential()
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        access_token = credential.get_token(databricks_resource_id + "/.default").token

        server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
        database = "default"

        query = f"""
        SELECT FACTORY_CODE, FACTORY_NAME FROM udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V
        WHERE EMAIL = '{user_email}' AND PILLAR_CODE = '{pillar_code}'
        GROUP BY FACTORY_CODE, FACTORY_NAME, FACTORY_NO
        ORDER BY FACTORY_NO
        """

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        response = {
            "code": 200,
            "factories": results
        }
        return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)

    except jwt.ExpiredSignatureError:
        return func.HttpResponse(json.dumps({'message': "Token đã hết hạn!"}), status_code=401, headers=headers)
    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500, headers=headers)
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500, headers=headers)
    except Exception as e:
        error_response = {
            "code": 500,
            "message": "Lỗi",
            "details": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)