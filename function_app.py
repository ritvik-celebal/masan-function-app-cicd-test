import azure.functions as func
 
# import modules from functions folder
from functions.oms_get_factories_by_user import main as get_factories_by_user
from functions.oms_get_lock_period import main as get_lock_period
from functions.oms_get_report_menu import main as get_report_menu
from functions.oms_insert_example import main as insert_example
from functions.oms_get_lock_period_log import main as get_lock_period_log
from functions.oms_modify_lock_period import main as modify_lock_period
from functions.oms_modify_user_permission import main as modify_user_permission
from functions.oms_load_result import main as load_result
from functions.oms_refresh_token import main as refresh_token
from functions.oms_login import main as login
from functions.oms_export_analysis import main as export_analysis
from functions.oms_export_template import main as export_template
from functions.oms_get_master_data import main as get_master_data
from functions.oms_export_result import main as export_result
 
app = func.FunctionApp()
 
#TESTED SUCCESSFULLY
@app.function_name(name="oms_get_factories_by_user")
@app.route(route="oms_get_factories_by_user", auth_level=func.AuthLevel.FUNCTION)
def oms_get_factories_by_user(req: func.HttpRequest) -> func.HttpResponse:
    return get_factories_by_user(req)

# #TESTED SUCCESSFULLY
@app.function_name(name="oms_get_lock_period")
@app.route(route="oms_get_lock_period", auth_level=func.AuthLevel.FUNCTION)
def oms_get_lock_period(req: func.HttpRequest) -> func.HttpResponse:
    return get_lock_period(req)

# #TESTED SUCCESSFULLY
@app.function_name(name="oms_get_master_data")
@app.route(route="oms_get_master_data", auth_level=func.AuthLevel.FUNCTION)
def oms_get_master_data(req: func.HttpRequest) -> func.HttpResponse:
    return get_master_data(req)
 
# #TESTED SUCCESSFULLY
@app.function_name(name="oms_get_report_menu")
@app.route(route="oms_get_report_menu", auth_level=func.AuthLevel.FUNCTION)
def oms_get_report_menu(req: func.HttpRequest) -> func.HttpResponse:
    return get_report_menu(req)
 
# # ERROR DURING TESTING
@app.function_name(name="oms_insert_example")
@app.route(route="oms_insert_example", auth_level=func.AuthLevel.FUNCTION)
def oms_insert_example(req: func.HttpRequest) -> func.HttpResponse:
    return insert_example(req)
 
#TESTED SUCCESSFULLY
@app.function_name(name="oms_get_lock_period_log")
@app.route(route="oms_get_lock_period_log", auth_level=func.AuthLevel.FUNCTION)
def oms_get_lock_period_log(req: func.HttpRequest) -> func.HttpResponse:
    return get_lock_period_log(req)
 
# #TESTED SUCCESSFULLY
@app.function_name(name="oms_modify_lock_period")
@app.route(route="oms_modify_lock_period", auth_level=func.AuthLevel.FUNCTION)
def oms_modify_lock_period(req: func.HttpRequest) -> func.HttpResponse:
    return modify_lock_period(req)
 
# # ERROR DURING TESTING
@app.function_name(name="oms_modify_user_permission")
@app.route(route="oms_modify_user_permission", auth_level=func.AuthLevel.FUNCTION)
def oms_modify_user_permission(req: func.HttpRequest) -> func.HttpResponse:
    return modify_user_permission(req)
 
# # ERROR DURING TESTING
@app.function_name(name="oms_load_result")
@app.route(route="oms_load_result", auth_level=func.AuthLevel.FUNCTION)
def oms_load_result(req: func.HttpRequest) -> func.HttpResponse:
    return load_result(req)

# # TESTED SUCCESSFULLY
@app.function_name(name="oms_refresh_token")
@app.route(route="oms_refresh_token", auth_level=func.AuthLevel.FUNCTION)
def oms_refresh_token(req: func.HttpRequest) -> func.HttpResponse:
    return refresh_token(req)

# # TESTED SUCCESSFULLY
@app.function_name(name="oms_login")
@app.route(route="oms_login", auth_level=func.AuthLevel.FUNCTION)
def oms_login(req: func.HttpRequest) -> func.HttpResponse:
    return login(req)

# # Testing pending
@app.function_name(name="oms_export_analysis")
@app.route(route="oms_export_analysis", auth_level=func.AuthLevel.FUNCTION)
def oms_export_analysis(req: func.HttpRequest) -> func.HttpResponse:
    return export_analysis(req)

# # Testing pending
@app.function_name(name="oms_export_result")
@app.route(route="oms_export_result", auth_level=func.AuthLevel.FUNCTION)
def oms_export_result(req: func.HttpRequest) -> func.HttpResponse:
    return export_result(req)


# # Testing pending
@app.function_name(name="oms_export_template")
@app.route(route="oms_export_template", auth_level=func.AuthLevel.FUNCTION)
def oms_export_template(req: func.HttpRequest) -> func.HttpResponse:
    return export_template(req)