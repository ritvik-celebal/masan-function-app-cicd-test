import azure.functions as func
import json
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql


# Databricks configuration
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
SERVER_HOSTNAME = "adb-1538821690907541.1.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/b6d556c5cf816ae6"
DATABASE = "default"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to support ADD, UPDATE or DELETE data in OMS_USERINFO_D table.
    """

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }

    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=headers)

    try:
        # Extract data from request JSON
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(
                json.dumps({"code": 400, "message": "Missing JSON payload"}), 
                status_code=400, 
                headers=headers
            )

        # Get parameters from request
        operation_type = request_json.get("type", "").lower()  # Operation type: add, update or delete
        factory_code = request_json.get("Factory_Code", None)
        pillar_codes = request_json.get("Pillar_Code", None)
        email = request_json.get("Email", None)
        role = request_json.get("Role", None)
        active = request_json.get("Active", None)

        # Uppercase Factory_Code and Pillar_Code
        if factory_code:
            factory_code = factory_code.upper()

        if isinstance(pillar_codes, str):
            pillar_codes = [pillar_codes.upper()]
        elif isinstance(pillar_codes, list):
            pillar_codes = [pillar_code.upper() for pillar_code in pillar_codes]

        # Check minimum requirements
        if not email:
            return func.HttpResponse(
                json.dumps({"code": 400, "message": "Email is required for operation"}), 
                status_code=400, 
                headers=headers
            )

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
                
                # ADD operation
                if operation_type == "add":
                    if not all([factory_code, pillar_codes, role, active is not None]):
                        return func.HttpResponse(
                            json.dumps({"code": 400, "message": "All fields are required for 'add' operation"}), 
                            status_code=400, 
                            headers=headers
                        )

                    # Loop through each pillar_code to perform insert
                    for pillar_code in pillar_codes:
                        insert_query = """
                        INSERT INTO udp_wcm_dev.MFG_OMS.OMS_USERINFO_D_TEST
                        (FACTORY_CODE, PILLAR_CODE, EMAIL, ROLE, ACTIVE)
                        VALUES (?, ?, ?, ?, ?)
                        """
                        
                        # Execute query with parameters
                        cursor.execute(insert_query, (factory_code, pillar_code, email, role, active))

                    response = {
                        "code": 200,
                        "status": "success",
                        "message": "Records added successfully."
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