import azure.functions as func
import json
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import datetime
from datetime import date
import jwt
# import redis

app = func.FunctionApp()

# # Redis configuration for rate limiting
# REDIS_HOST = "your-redis-cache.redis.cache.windows.net"  # Azure Cache for Redis
# REDIS_PORT = 6380  # SSL port for Azure Cache for Redis
# REDIS_PASSWORD = "your-redis-access-key"  # Get from Azure portal
# RATE_LIMIT = 10
# TIME_LIMIT = 60  # 60 seconds

# # Initialize Redis client for Azure Cache for Redis
# client_redis = redis.StrictRedis(
#     host=REDIS_HOST,
#     port=REDIS_PORT,
#     password=REDIS_PASSWORD,
#     ssl=True,  # Azure Cache for Redis requires SSL
#     decode_responses=True
# )

# Azure Key Vault or environment variables should be used for these in production
SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Đảm bảo đủ 32 bytes
IV = "abcd012345678910".encode()  # Đảm bảo đủ 16 bytes

# def test_redis_connection():
#     """Test Redis connection for Azure Cache for Redis."""
#     try:
#         pong = client_redis.ping()
#         logging.info(f"Redis connection successful: {pong}")
#         return True
#     except Exception as e:
#         logging.error(f"Redis connection failed: {e}")
#         return False

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to handle CORS and check email existence in Databricks.
    """
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)
    
    # Test Redis connection
    # test_redis_connection()

    try:
        # Check rate limiting
        # if not check_rate_limited_function(req):
        #     return func.HttpResponse(
        #         json.dumps({"error": 'Too many requests'}),
        #         status_code=403,
        #         headers=headers
        #     )
        # Get user email from Authorization header
        user_email_encrypted = req.headers.get('Authorization')
        if not user_email_encrypted:
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields in JSON payload"}),
                status_code=400,
                headers=headers
            )

        user_email = decrypt_aes_json(user_email_encrypted)
        if not user_email:
            return func.HttpResponse(
                json.dumps({"error": "Invalid authorization token"}),
                status_code=403,
                headers=headers
            )

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
                # Query to check if email exists and retrieve associated pillar codes
                pillar_query = f"""
                SELECT EMAIL, PILLAR_CODE, CONCAT(PILLAR_CODE, ' - ', PILLAR_NAME, ' Pillar') AS PILLAR_NAME
                FROM udp_wcm_dev.MFG_OMS.OMS_PERMISSION_V 
                WHERE EMAIL = '{user_email}'
                GROUP BY EMAIL, PILLAR_CODE, PILLAR_NO, PILLAR_NAME
                ORDER BY PILLAR_NO
                """
                cursor.execute(pillar_query, (user_email,))
                pillar_results = cursor.fetchall()
                pillar_columns = [desc[0] for desc in cursor.description]
                
                pillar_codes = []
                for row in pillar_results:
                    row_dict = dict(zip(pillar_columns, row))
                    if row_dict["PILLAR_CODE"]:
                        pillar_codes.append({
                            "Code": row_dict["PILLAR_CODE"],
                            "Name": row_dict["PILLAR_NAME"]
                        })

                # Query to get user role with priority (Admin first)
                role_query = f"""
                SELECT ROLE, EMAIL FROM (
                    SELECT ROLE, EMAIL, 0 as ORDER_NO
                    FROM udp_wcm_dev.MFG_OMS.OMS_USERINFO_D
                    WHERE EMAIL = '{user_email}' AND ROLE = 'Admin'
                    UNION ALL
                    SELECT ROLE, EMAIL, 1 as ORDER_NO
                    FROM udp_wcm_dev.MFG_OMS.OMS_USERINFO_D
                    WHERE EMAIL = '{user_email}' AND ROLE NOT IN ('Admin')
                ) RESULT 
                ORDER BY ORDER_NO 
                LIMIT 1
                """
                cursor.execute(role_query, (user_email, user_email))
                role_results = cursor.fetchall()
                role_columns = [desc[0] for desc in cursor.description]

                user_detail = {}
                is_exist = 0
                for row in role_results:
                    row_dict = dict(zip(role_columns, row))
                    if row_dict["ROLE"]:
                        user_detail = {
                            "role": row_dict["ROLE"],
                            "email": row_dict["EMAIL"]
                        }
                        is_exist = 1
                        break

        if pillar_codes:
            response = {
                "code": 200,
                "is_exist": is_exist,
                "Pillar": pillar_codes,
                "user_detail": user_detail,
                "message": "Checked email successfully",
                "token": generate_token({"user": encrypt_aes_json(user_detail)})
            }
            return func.HttpResponse(
                json.dumps(response, ensure_ascii=False, indent=4),
                status_code=200,
                headers=headers
            )
        else:
            response = {
                "code": 400,
                "is_exist": is_exist,
                "Pillar": [],
                "message": "Lỗi không đăng nhập được do Email chưa được khai báo."
            }
            return func.HttpResponse(
                json.dumps(response, ensure_ascii=False, indent=4),
                status_code=200,
                headers=headers
            )

    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse(
            json.dumps({"error": "Connection timeout - check managed identity configuration"}),
            status_code=500,
            headers=headers
        )
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"URL Error: {str(e)}"}),
            status_code=500,
            headers=headers
        )
    except Exception as e:
        logging.error(f"Error: {e}")
        error_response = {
            "code": 500,
            "message": "Lỗi",
            "details": str(e)
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            headers=headers
        )

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
    """
    Decrypt AES encrypted text and return JSON object.
    """
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

def generate_token(user_info):
    """
    Generate JWT token with user information.
    """
    try:
        payload = {
            **user_info,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
        return token
    except Exception as e:
        logging.error(f"Token generation failed: {e}")
        return None

def check_rate_limited_function(req: func.HttpRequest):
    """
    Check if the request is rate limited based on client IP.
    Uses Azure Cache for Redis for distributed rate limiting.
    """
    try:
        # Get client IP from Azure Function request
        user_ip = req.headers.get('X-Forwarded-For', '').split(',')[0] or req.headers.get('X-Real-IP', 'unknown')
        key = f"rate_limit:{user_ip}"
        
        request_count = client_redis.incr(key)
        if request_count == 1:
            client_redis.expire(key, TIME_LIMIT)
        
        if request_count > RATE_LIMIT:
            logging.warning(f"Rate limit exceeded for IP: {user_ip}, count: {request_count}")
            return False
        
        logging.info(f"Rate limit check passed for IP: {user_ip}, count: {request_count}")
        return True
    except Exception as e:
        logging.error(f"Rate limiting check failed: {e}")
        # If Redis fails, allow the request to proceed (fail-open)
        return True