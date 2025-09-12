import azure.functions as func
import logging
# import azure.functions as func
# import logging
import urllib.request
import json
import socket
from azure.identity import DefaultAzureCredential

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully v1.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
#==================================================================================================


 
# app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
 
@app.route(route="select_example", auth_level=func.AuthLevel.FUNCTION)
def select_example(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Getting Databricks token.')
 
    try:
        # Create a credential using the managed identity
        credential = DefaultAzureCredential()
 
        # Databricks resource ID (AAD token audience)
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
 
        # Get access token
        access_token = credential.get_token(databricks_resource_id + "/.default").token
 
        # Databricks SQL Warehouse connection details   
        from databricks import sql
        server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
        database = "default"
 
        # Connect and query
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                query = "SELECT * FROM udp_wcm_dev.default.lookup_table_source2raw LIMIT 10"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
 
        return func.HttpResponse(
            json.dumps({"code": 200, "data": results}),
            status_code=200,
            mimetype="application/json"
        )
 
    except socket.timeout:
        logging.error("Connection timed out - managed identity may not be enabled")
        return func.HttpResponse("Connection timeout - check managed identity configuration", status_code=500)
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {str(e)}")
        return func.HttpResponse(f"URL Error: {str(e)}", status_code=500)
    except Exception as e:
        logging.error(f"Error getting token or fetching data: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

#==================================================================================================
