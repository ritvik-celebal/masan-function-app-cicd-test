import azure.functions as func
import json
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql
from collections import defaultdict
import jwt
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding

# Databricks configuration
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
SERVER_HOSTNAME = "adb-1538821690907541.1.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/b6d556c5cf816ae6"
DATABASE = "default"

# Encryption configuration
SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Ensure 32 bytes
IV = "abcd012345678910".encode()  # Ensure 16 bytes

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to get information from OMS_USERINFO_D table in Databricks with parameters.
    """

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=headers)

    try:
        # Extract and validate JWT token
        auth_header = req.headers.get('Authorization', '')
        token = auth_header.split('Bearer ')[-1] if auth_header.startswith('Bearer ') else auth_header
        
        try:
            decoded_payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user = decrypt_aes_json(decoded_payload['user'])
            if user.get("role", None) != 'Admin':
                return func.HttpResponse(
                    json.dumps({'message': "Bạn không có quyền thực hiện tác vụ này"}),
                    status_code=403,
                    headers=headers
                )
        except jwt.ExpiredSignatureError:
            return func.HttpResponse(
                json.dumps({'message': "Token đã hết hạn!"}),
                status_code=401,
                headers=headers
            )

        # Parse request JSON
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(
                json.dumps({"code": 400, "message": "Missing JSON payload"}),
                status_code=400,
                headers=headers
            )

        # Get parameters from request
        factory_code = request_json.get("Factory_Code", None)
        pillar_code = request_json.get("Pillar_Code", None)
        email = request_json.get("Email", None)
        role = request_json.get("Role_Code", None)  # Use Role_Code from request

        # Databricks connection setup
        credential = DefaultAzureCredential()
        access_token = credential.get_token(DATABRICKS_RESOURCE_ID + "/.default").token

        with sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=access_token,
            database=DATABASE
        ) as connection:
            with connection.cursor() as cursor:
                
                # Build WHERE conditions with parameters
                where_conditions = ["1 = 1"]
                params = []
                
                if factory_code:
                    where_conditions.append("(D.FACTORY_CODE = ? OR D.FACTORY_CODE = 'ALL')")
                    params.append(factory_code)
                
                if pillar_code:
                    where_conditions.append("(D.PILLAR_CODE = ? OR D.PILLAR_CODE = 'ALL')")
                    params.append(pillar_code)
                
                if email:
                    where_conditions.append("LOWER(EMAIL) LIKE ?")
                    params.append(f"%{email.lower()}%")
                
                if role:
                    where_conditions.append("ROLE = ?")
                    params.append(role)

                # Build SQL query
                query = """
                SELECT DISTINCT
                    D.FACTORY_CODE, 
                    COALESCE(F.ALIAS_NAME, D.FACTORY_CODE) AS FACTORY_NAME,
                    D.PILLAR_CODE,
                    D.EMAIL,
                    D.ROLE,
                    D.ACTIVE
                FROM udp_wcm_dev.MFG_OMS.OMS_USERINFO_D_TEST D
                LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_FACTORY_D F ON D.FACTORY_CODE = F.FACTORY_CODE
                WHERE """ + " AND ".join(where_conditions) + """
                ORDER BY EMAIL, ROLE, D.FACTORY_CODE
                """

                print("🔍 Executing SQL Query:\n", query)
                print("🔍 Parameters:", params)

                # Execute query with parameters
                cursor.execute(query, params)
                results = cursor.fetchall()

                # Get column names
                columns = [desc[0] for desc in cursor.description]

                # Group data by Factory_Code, Email, and Role
                grouped_data = defaultdict(lambda: {"Pillar_Code": []})

                for row in results:
                    row_dict = dict(zip(columns, row))
                    key = (row_dict['FACTORY_CODE'], row_dict['EMAIL'], row_dict['ROLE'])
                    
                    if not grouped_data[key]["Pillar_Code"]:
                        grouped_data[key].update({
                            "Factory_Code": row_dict['FACTORY_CODE'],
                            "Factory_Name": row_dict['FACTORY_NAME'],
                            "Email": row_dict['EMAIL'],
                            "Role": row_dict['ROLE'],
                            "Active": row_dict['ACTIVE']
                        })
                    grouped_data[key]["Pillar_Code"].append(row_dict['PILLAR_CODE'])

                # Convert data to list
                user_data = list(grouped_data.values())

                # Return result
                response = {
                    "code": 200,
                    "status": "success",
                    "data": user_data
                }
                return func.HttpResponse(
                    json.dumps(response, ensure_ascii=False, indent=4),
                    status_code=200,
                    headers=headers
                )

    except socket.timeout:
        return func.HttpResponse(
            json.dumps({"code": 500, "status": "error", "message": "Connection timeout - check managed identity configuration"}),
            status_code=500,
            headers=headers
        )
    except urllib.error.URLError as e:
        return func.HttpResponse(
            json.dumps({"code": 500, "status": "error", "message": f"URL Error: {str(e)}"}),
            status_code=500,
            headers=headers
        )
    except Exception as e:
        # Handle errors if exceptions occur
        error_response = {
            "code": 500,
            "status": "error",
            "message": str(e)
        }
        return func.HttpResponse(
            json.dumps(error_response, ensure_ascii=False, indent=4),
            status_code=500,
            headers=headers
        )


def decrypt_aes_json(encrypted_text):
    """
    Decrypt AES encrypted JSON data
    """
    try:
        if not encrypted_text or len(encrypted_text.strip()) == 0:
            raise ValueError("❌ Dữ liệu mã hóa rỗng!")

        print("🔐 Encrypted Input (Base64):", encrypted_text)

        # Base64 decode
        encrypted_bytes = base64.b64decode(encrypted_text)

        print("🔑 Encrypted Bytes Length:", len(encrypted_bytes))

        # Check valid data length
        if len(encrypted_bytes) % 16 != 0:
            raise ValueError("❌ Dữ liệu mã hóa không phải bội số của 16 bytes!")

        # AES decryption
        cipher = Cipher(algorithms.AES(SECRET_KEY), modes.CBC(IV), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_padded = decryptor.update(encrypted_bytes) + decryptor.finalize()

        print("🔓 Decrypted (Raw with Padding):", decrypted_padded)

        # Remove padding
        unpadder = padding.PKCS7(128).unpadder()
        decrypted_text = unpadder.update(decrypted_padded) + unpadder.finalize()

        print("📜 Decrypted Text:", decrypted_text.decode())

        # Convert to JSON
        return json.loads(decrypted_text.decode())

    except json.JSONDecodeError as je:
        print(f"❌ JSONDecodeError: {je}")
    except ValueError as ve:
        print(f"❌ ValueError: {ve}")
    except Exception as e:
        print(f"❌ Decryption failed: {e}")
    
    return None  # Return None if error occurs
