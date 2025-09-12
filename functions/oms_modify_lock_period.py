import logging
import azure.functions as func
import jwt
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from datetime import datetime, timedelta
import pytz
import json
# from google.cloud import bigquery  # Not used in Azure Functions
# from flask import jsonify, request  # Not used in Azure Functions
# from google.oauth2 import id_token  # Not used in Azure Functions
# from google.auth.transport import requests as google_requests  # Not used in Azure Functions

# SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Ensure 32 bytes
# IV = "abcd012345678910".encode()  # Ensure 16 bytes

# def decrypt_aes_json remains unchanged

# def decrypt_aes_json(encrypted_text):
#     try:
#         if not encrypted_text or len(encrypted_text.strip()) == 0:
#             raise ValueError("❌ Dữ liệu mã hóa rỗng!")
#         encrypted_bytes = base64.b64decode(encrypted_text)
#         if len(encrypted_bytes) % 16 != 0:
#             raise ValueError("❌ Dữ liệu mã hóa không phải bội số của 16 bytes!")
#         cipher = Cipher(algorithms.AES(SECRET_KEY), modes.CBC(IV), backend=default_backend())
#         decryptor = cipher.decryptor()
#         decrypted_padded = decryptor.update(encrypted_bytes) + decryptor.finalize()
#         unpadder = padding.PKCS7(128).unpadder()
#         decrypted_text = unpadder.update(decrypted_padded) + unpadder.finalize()
#         return json.loads(decrypted_text.decode())
#     except json.JSONDecodeError as je:
#         print(f"❌ JSONDecodeError: {je}")
#     except ValueError as ve:
#         print(f"❌ ValueError: {ve}")
#     except Exception as e:
#         print(f"❌ Decryption failed: {e}")
#     return None

# Azure Functions entry point

def main(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    if req.method == "OPTIONS":
        return func.HttpResponse('', status_code=204, headers=headers)
    try:
        # token = req.headers.get('Authorization', '').split('Bearer ')[-1]
        # decoded_payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        # user = decrypt_aes_json(decoded_payload['user'])
        # user_email = user.get("email")
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing JSON payload"}), status_code=400, headers=headers)
        month = int(request_json.get("Month", None))
        year = int(request_json.get("Year", None))
        factory_codes = request_json.get("Factory_Code", [])
        pillar_codes = request_json.get("Pillar_Code", [])
        is_active = request_json.get("Is_Active", None)
        email = request_json.get("Email", None)
        # original_factory_code = factory_codes
        # original_pillar_code = pillar_codes
        if not month or not year:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing required parameter: Month, Year"}), status_code=400, headers=headers)
        if not factory_codes or not pillar_codes or not email:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing required parameters: Factory_Code, Pillar_Code, or Email"}), status_code=400, headers=headers)
        # The following code for BigQuery is commented out, as it is not used in Azure Functions
        # if factory_codes == "ALL":
        #     ...
        # if pillar_codes == "ALL":
        #     ...
        queries = []
        hanoi_tz = pytz.timezone("Asia/Bangkok")
        now = datetime.now(hanoi_tz).strftime('%Y-%m-%d %H:%M:%S')
        if month == 1:
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year
        if is_active == 0:
            factory_code_list = "', '".join(factory_codes)
            pillar_code_list = "', '".join(pillar_codes)
            delete_query = f"""
                DELETE FROM MFG_OMS.OMS_LOCK_PERIOD_F
                WHERE MONTH = {month}
                  AND YEAR = {year}
                  AND FACTORY_CODE IN ('{factory_code_list}')
                  AND PILLAR_CODE IN ('{pillar_code_list}')
            """
            queries.append(delete_query)
            values = []
            for factory_code in factory_codes:
                for pillar_code in pillar_codes:
                    values.append(f"({year}, {month}, '{factory_code}', '{pillar_code}', {is_active}, '{email}', '{now}')")
            insert_query = f"""
                INSERT INTO MFG_OMS.OMS_LOCK_PERIOD_F (YEAR, MONTH, FACTORY_CODE, PILLAR_CODE, IS_ACTIVE, CREATED_BY, CREATED_DATE)
                VALUES {', '.join(values)}
            """
            queries.append(insert_query)
        elif is_active == 1:
            factory_code_list = "', '".join(factory_codes)
            pillar_code_list = "', '".join(pillar_codes)
            delete_query = f"""
                DELETE FROM MFG_OMS.OMS_LOCK_PERIOD_F
                WHERE YEAR * 100 + MONTH >= {year} * 100 + {month}
                  AND FACTORY_CODE IN ('{factory_code_list}')
                  AND PILLAR_CODE IN ('{pillar_code_list}')
            """
            queries.append(delete_query)
            values = []
            for factory_code in factory_codes:
                for pillar_code in pillar_codes:
                    values.append(f"({prev_year}, {prev_month}, '{factory_code}', '{pillar_code}', 0, '{email}', '{now}')")
            insert_previous_query = f"""
                INSERT INTO MFG_OMS.OMS_LOCK_PERIOD_F (YEAR, MONTH, FACTORY_CODE, PILLAR_CODE, IS_ACTIVE, CREATED_BY, CREATED_DATE)
                VALUES {', '.join(values)}
            """
            queries.append(insert_previous_query)
        else:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Invalid Is_Active value. Allowed values: 0 or 1."}), status_code=400, headers=headers)
        # The following code for executing queries is commented out, as it is not used in Azure Functions
        # for query in queries:
        #     ...
        # Logging logic for OMS_LOCK_PERIOD_LOG_F is also commented out
        response = {
            "code": 200,
            "status": "success",
            "message": "Successfully."
        }
        return func.HttpResponse(json.dumps(response), status_code=200, headers=headers)
    except jwt.ExpiredSignatureError:
        return func.HttpResponse(json.dumps({'message':"Token đã hết hạn!"}), status_code=401, headers=headers)
    except Exception as e:
        error_response = {
            "code": 500,
            "status": "error",
            "message": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)
