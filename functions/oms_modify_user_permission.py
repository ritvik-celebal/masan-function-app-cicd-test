import logging
import azure.functions as func
import jwt
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import json
# from google.cloud import bigquery  # Not used in Azure Functions
# from flask import jsonify, request  # Not used in Azure Functions
# from google.oauth2 import id_token  # Not used in Azure Functions
# from google.auth.transport import requests as google_requests  # Not used in Azure Functions

SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Ensure 32 bytes
IV = "abcd012345678910".encode()  # Ensure 16 bytes

def decrypt_aes_json(encrypted_text):
    try:
        if not encrypted_text or len(encrypted_text.strip()) == 0:
            raise ValueError("❌ Dữ liệu mã hóa rỗng!")
        encrypted_bytes = base64.b64decode(encrypted_text)
        if len(encrypted_bytes) % 16 != 0:
            raise ValueError("❌ Dữ liệu mã hóa không phải bội số của 16 bytes!")
        cipher = Cipher(algorithms.AES(SECRET_KEY), modes.CBC(IV), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_padded = decryptor.update(encrypted_bytes) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        decrypted_text = unpadder.update(decrypted_padded) + unpadder.finalize()
        return json.loads(decrypted_text.decode())
    except json.JSONDecodeError as je:
        print(f"❌ JSONDecodeError: {je}")
    except ValueError as ve:
        print(f"❌ ValueError: {ve}")
    except Exception as e:
        print(f"❌ Decryption failed: {e}")
    return None

def main(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    if req.method == "OPTIONS":
        return func.HttpResponse('', status_code=204, headers=headers)
    token = req.headers.get('Authorization', '').split('Bearer ')[-1]
    try:
        decoded_payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user = decrypt_aes_json(decoded_payload['user'])
        if user.get("role", None) != 'Admin':
            return func.HttpResponse(json.dumps({'message':"Bạn không có quyền thực hiện tác vụ này"}), status_code=403, headers=headers)
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Missing JSON payload"}), status_code=400, headers=headers)
        operation_type = request_json.get("type", "").lower()
        factory_code = request_json.get("Factory_Code", None)
        old_factory_code = request_json.get("Old_Factory_Code", None)
        pillar_codes = request_json.get("Pillar_Code", None)
        email = request_json.get("Email", None)
        role = request_json.get("Role", None)
        old_role = request_json.get("Old_Role", None)
        active = request_json.get("Active", None)
        if factory_code:
            factory_code = factory_code.upper()
        if old_factory_code:
            old_factory_code = old_factory_code.upper()
        if isinstance(pillar_codes, str):
            pillar_codes = [pillar_codes.upper()]
        elif isinstance(pillar_codes, list):
            pillar_codes = [pillar_code.upper() for pillar_code in pillar_codes]
        if not email:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Email is required for operation"}), status_code=400, headers=headers)
        # ADD operation
        if operation_type == "add":
            if not all([factory_code, pillar_codes, role, active is not None]):
                return func.HttpResponse(json.dumps({"code": 400, "message": "All fields are required for 'add' operation"}), status_code=400, headers=headers)
            # The following code for BigQuery is commented out, as it is not used in Azure Functions
            # for pillar_code in pillar_codes:
            #     insert_query = ...
            response = {
                "code": 200,
                "status": "success",
                "message": "Records added successfully."
            }
            return func.HttpResponse(json.dumps(response), status_code=200, headers=headers)
        # UPDATE operation
        elif operation_type == "update":
            if not all([factory_code, pillar_codes, role, active is not None]):
                return func.HttpResponse(json.dumps({"code": 400, "message": "All fields are required for 'update' operation"}), status_code=400, headers=headers)
            # The following code for BigQuery is commented out, as it is not used in Azure Functions
            # delete_conditions = ...
            # delete_query = ...
            # for pillar_code in pillar_codes:
            #     insert_query = ...
            response = {
                "code": 200,
                "status": "success",
                "message": "Records updated successfully."
            }
            return func.HttpResponse(json.dumps(response), status_code=200, headers=headers)
        # DELETE operation
        elif operation_type == "delete":
            if not factory_code:
                return func.HttpResponse(json.dumps({"code": 400, "message": "Factory_Code is required for delete operation"}), status_code=400, headers=headers)
            # The following code for BigQuery is commented out, as it is not used in Azure Functions
            # delete_query = ...
            response = {
                "code": 200,
                "status": "success",
                "message": "Record deleted successfully."
            }
            return func.HttpResponse(json.dumps(response), status_code=200, headers=headers)
        else:
            return func.HttpResponse(json.dumps({"code": 400, "message": "Invalid operation type. Use 'add', 'update', or 'delete'."}), status_code=400, headers=headers)
    except jwt.ExpiredSignatureError:
        return func.HttpResponse(json.dumps({'message':"Token đã hết hạn!"}), status_code=401, headers=headers)
    except Exception as e:
        error_response = {
            "code": 500,
            "status": "error",
            "message": str(e)
        }
        return func.HttpResponse(json.dumps(error_response), status_code=500, headers=headers)
