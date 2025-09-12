import azure.functions as func
import json
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql
from datetime import datetime, timedelta
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
# import redis


# Redis configuration
REDIS_HOST = "10.14.36.180"
REDIS_PORT = 6379
# clientRedis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# def test_redis_connection():
#     try:
#         client = redis.StrictRedis(
#             host=REDIS_HOST,
#             port=REDIS_PORT,
#             decode_responses=True,
#             socket_timeout=5
#         )
#         pong = client.ping()
#         logging.info(f"Redis connection successful: {pong}")
#     except Exception as e:
#         logging.error(f"Redis connection failed: {e}")

RATE_LIMIT = 10
TIME_LIMIT = 60  # 60 seconds
SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Ensure 32 bytes
IV = "abcd012345678910".encode()  # Ensure 16 bytes

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function for user authentication and token generation.
    Returns encrypted tokens for authenticated users with pillar access.
    """
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)

    # test_redis_connection()
    try:
        # Rate limiting check (commented out like in original)
        # if not check_rate_limited_function(req):
        #     return func.HttpResponse(json.dumps({"error": 'Too many requests'}), status_code=403, headers=headers)
        
        user_email_decrypted = req.headers.get('Authorization')
        if not user_email_decrypted:
            return func.HttpResponse(json.dumps({"error": "Missing required fields in JSON payload"}), status_code=400, headers=headers)
        
        user_email = decrypt_aes_json(user_email_decrypted)
        if not user_email:
            return func.HttpResponse(json.dumps(user_email), status_code=403, headers=headers)

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
                # Query to check if email exists and retrieve associated pillar codes
                query = f"""
                SELECT EMAIL, PILLAR_CODE, CONCAT(PILLAR_CODE , ' - ' , PILLAR_NAME , ' Pillar') AS PILLAR_NAME
                FROM udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V
                WHERE EMAIL = '{user_email}'
                GROUP BY EMAIL, PILLAR_CODE, PILLAR_NO, PILLAR_NAME
                ORDER BY PILLAR_NO
                """
                
                logging.info("Generated SQL Query for Pillar Codes:")
                logging.info(query)
                
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()

                # Process the query result - get pillar codes with names
                pillar_codes = [{"Code": row[1], "Name": row[2]} for row in results if row[1]]

                # Process the query role
                queryRole = f"""
                SELECT MIN(U.ROLE) AS ROLE
                FROM udp_wcm_dev.MFG_OMS.OMS_USERINFO_D U
                WHERE EMAIL = '{user_email}'
                """
                
                logging.info("Generated SQL Query for User Role:")
                logging.info(queryRole)
                
                cursor.execute(queryRole)
                result_role = cursor.fetchall()
                
                user_detail = {}
                is_exist = 0
                response_user_detail = [{"role": row[0]} for row in result_role if row[0]]
                if len(response_user_detail) > 0:
                    user_detail = response_user_detail[0]
                    is_exist = 1
                
                token = None
                if is_exist == 1:
                    tokens = call_azure_function_api()

        if pillar_codes:
            response = {
                "code": 200,
                "tokens": encrypt_aes_json(tokens)
            }
            return func.HttpResponse(json.dumps(response, ensure_ascii=False, indent=4), status_code=200, headers=headers)
        else:
            response = {
                "code": 400,
                "message": "Lỗi do Email chưa được khai báo."
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

def encrypt_aes_json(obj):
    # Chuyển JSON object thành chuỗi
    plain_text = json.dumps(obj)

    cipher = Cipher(algorithms.AES(SECRET_KEY), modes.CBC(IV), backend=default_backend())
    encryptor = cipher.encryptor()

    # Padding để dữ liệu đủ block size (16 bytes)
    padder = padding.PKCS7(128).padder()
    padded_text = padder.update(plain_text.encode()) + padder.finalize()

    encrypted_bytes = encryptor.update(padded_text) + encryptor.finalize()
    return base64.b64encode(encrypted_bytes).decode()  # Chuyển thành Base64

def decrypt_aes_json(encrypted_text):
    try:
        if not encrypted_text or len(encrypted_text.strip()) == 0:
            raise ValueError("Dữ liệu mã hóa rỗng!")

        print("Encrypted Input (Base64):", encrypted_text)

        # Giải mã Base64
        encrypted_bytes = base64.b64decode(encrypted_text)

        print("Encrypted Bytes Length:", len(encrypted_bytes))

        # Kiểm tra độ dài dữ liệu hợp lệ
        if len(encrypted_bytes) % 16 != 0:
            raise ValueError("Dữ liệu mã hóa không phải bội số của 16 bytes!")

        # Giải mã AES
        cipher = Cipher(algorithms.AES(SECRET_KEY), modes.CBC(IV), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_padded = decryptor.update(encrypted_bytes) + decryptor.finalize()

        print("Decrypted (Raw with Padding):", decrypted_padded)

        # Gỡ padding
        unpadder = padding.PKCS7(128).unpadder()
        decrypted_text = unpadder.update(decrypted_padded) + unpadder.finalize()

        print("Decrypted Text:", decrypted_text.decode())

        # Chuyển thành JSON
        return json.loads(decrypted_text.decode())
    
    except json.JSONDecodeError as je:
        print(f"JSONDecodeError: {je}")
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as e:
        print(f"Decryption failed: {e}")
    
    return None  # Trả về None nếu có lỗi

# Azure Function URLs (equivalent to Google Cloud Function URLs)
# SCOPES = [
#     "https://your-azure-function-app.azurewebsites.net/api/oms_export_analysis",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_export_result",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_export_template",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_factories_by_user",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_lock_period",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_lock_period_log",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_master_data",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_report_menu",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_get_user_permission",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_insert_score",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_load_result",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_modify_lock_period",
#     "https://your-azure-function-app.azurewebsites.net/api/oms_modify_user_permission"
# ]

SCOPES = ["https://fa-udp-datawarehouse-mch-dev-sea-02.azurewebsites.net/api/oms_get_factories_by_user"]

def authenticate_azure():
    """
    Authenticate using Azure DefaultAzureCredential (equivalent to authenticate_google)
    """
    try:
        credential = DefaultAzureCredential()
        # Get token for Azure Functions resource
        token_response = credential.get_token("https://management.azure.com/.default")
        return token_response.token

    except Exception as e:
        return str(e)

def get_identity_token_with_quota(target_audience):
    """
    Gets the identity token for the provided target audience using Azure credentials.
    Equivalent to the Google Cloud function but simplified for Azure.
    """
    try:
        credential = DefaultAzureCredential()
        # For Azure, we use a different approach to get tokens for specific resources
        token_response = credential.get_token("https://management.azure.com/.default")
        return token_response.token
    except Exception as e:
        logging.error(f"Failed to get identity token: {str(e)}")
        return None

def check_rate_limited_function(req):
    """
    Check rate limiting using Redis (equivalent to original function)
    """
    try:
        # In Azure Functions, get client IP from headers
        user_ip = req.headers.get('X-Forwarded-For', req.headers.get('X-Real-IP', 'unknown'))
        key = f"rate_limit:{user_ip}"
        request_count = clientRedis.incr(key)
        if request_count == 1:
            clientRedis.expire(key, TIME_LIMIT)
        if request_count > RATE_LIMIT:
            return False
        return True
    except Exception as e:
        logging.error(f"Rate limiting check failed: {e}")
        return True  # Allow request if rate limiting fails

def call_azure_function_api():
    """
    Calls Azure Function APIs to get identity tokens (equivalent to call_google_cloud_api).
    """
    # List of URLs you want to use for target audience (equivalent to target_audiences)
    # target_audiences = [
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_export_analysis",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_export_result",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_export_template",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_factories_by_user",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_lock_period",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_lock_period_log",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_master_data",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_report_menu",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_get_user_permission",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_insert_score",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_load_result",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_modify_lock_period",
    #     "https://your-azure-function-app.azurewebsites.net/api/oms_modify_user_permission"
    # ]

    target_audiences = ["https://fa-udp-datawarehouse-mch-dev-sea-02.azurewebsites.net/api/oms_get_factories_by_user",
                        "https://fa-udp-datawarehouse-mch-dev-sea-02.azurewebsites.net/api/oms_get_lock_period"]
    # Generate token for each target audience and return
    tokens = {}
    for audience in target_audiences:
        token = get_identity_token_with_quota(audience)
        tokens[audience] = token

    return tokens