import azure.functions as func
import json
import socket
import urllib.error
from decimal import Decimal
from azure.identity import DefaultAzureCredential
from databricks import sql
import jwt
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding

app = func.FunctionApp()

# Databricks configuration
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
SERVER_HOSTNAME = "adb-1538821690907541.1.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/b6d556c5cf816ae6"
DATABASE = "default"

# JWT and AES configuration
SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()  # Ensure 32 bytes
IV = "abcd012345678910".encode()  # Ensure 16 bytes

def validate_request(request_json):
    required_fields = ["Year", "Month", "Factory_Code", "Pillar_Code"]
    for field in required_fields:
        if field not in request_json:
            return {"error": f"Missing required field: {field}"}, False
    return {}, True

def convert_to_percentage(value):
    if value is None:
        return ""
    return f"{value:,.1f}%"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function for OMS data analysis with JWT authentication and complex scoring queries.
    """
    
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    
    if req.method == 'OPTIONS':
        return func.HttpResponse("", status_code=204, headers=headers)
    
    try:

        # Parse request JSON
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(
                json.dumps({"error": "Missing or invalid JSON payload"}),
                status_code=400,
                headers=headers
            )

        # Get parameters from request
        year = int(request_json.get("Year"))
        month = int(request_json.get("Month"))
        factory_code = request_json.get("Factory_Code")
        pillar_code = request_json.get("Pillar_Code", None)  # Default None if not passed
        is_eng = request_json.get("Is_ENG", 0)
        is_vie = request_json.get("Is_VIE", 1)
        is_pillar = request_json.get("Is_Pillar", 1)
        is_chapter = request_json.get("Is_Chapter", 1)
        is_sub_chapter = request_json.get("Is_Sub_Chapter", 1)

        # Validate required fields
        validation_result, is_valid = validate_request(request_json)
        if not is_valid:
            return func.HttpResponse(
                json.dumps(validation_result),
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
                
                # Main query for OMS data analysis
                main_query = f"""
                WITH SC AS (
                    SELECT S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO AS PILLAR_NO, LV.LEVEL_NO, SC.TYPE, SC.CODE,
                        CASE 
                            WHEN {is_eng} = 1 AND {is_vie} = 1 THEN SC.ENG_NAME || ' / ' || SC.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN SC.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN SC.VIE_NAME
                            ELSE '' 
                        END AS NAME,
                        SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) AS TOTAL_SCORE,
                        COUNT(1) AS ACTION,
                        COUNT(CASE WHEN SCORE = -1 THEN 1 END) AS ACTION_NA,
                        ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN SCORE <> -1 THEN 1 END) * 3)
                            * CASE WHEN P.PILLAR_CODE <> 'SAF' THEN 100 ELSE 1 END, 2) AS Assessment,
                        NULL AS Total_score_pillar, NULL AS Final_score, NULL AS Grade,
                        CASE WHEN C.CODE = 'SAF00' THEN
                            CASE WHEN SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) * 0.8 < 15 
                                THEN ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) * 0.8, 2)
                                ELSE 15 END
                        END AS REDUCT_SCORING,
                        CAST(ROUND((1 - (CASE WHEN C.CODE = 'SAF00' THEN
                            CASE WHEN SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) * 0.8 < 15 
                                THEN SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) * 0.8 
                                ELSE 15 END
                        END / 15)) * 100, 0) AS STRING) || '%' AS COMPLIANCE,
                        C.CODE AS CHAPTER_CODE, C.PILLAR_CODE
                    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = SC.TYPE
                    WHERE S.YEAR = {year} AND S.MONTH = {month} AND S.FACTORY_CODE = '{factory_code}' AND P.PILLAR_CODE = '{pillar_code}'
                    GROUP BY S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO, LV.LEVEL_NO, SC.TYPE, SC.CODE, SC.ENG_NAME, SC.VIE_NAME, C.CODE, C.PILLAR_CODE, P.PILLAR_CODE
                ),
                C AS (
                    SELECT S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO AS PILLAR_NO, LV.LEVEL_NO, C.TYPE, C.CODE,
                        CASE 
                            WHEN {is_eng} = 1 AND {is_vie} = 1 THEN C.ENG_NAME || ' / ' || C.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN C.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN C.VIE_NAME
                            ELSE '' 
                        END AS NAME,
                        SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) AS TOTAL_SCORE,
                        COUNT(1) AS ACTION,
                        COUNT(CASE WHEN SCORE = -1 THEN 1 END) AS ACTION_NA,
                        ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN SCORE <> -1 THEN 1 END) * 3)
                            * CASE WHEN P.PILLAR_CODE <> 'SAF' THEN 100 ELSE 1 END, 2) AS ASSESSMENT,
                        C.PILLAR_CODE
                    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = C.TYPE
                    WHERE S.YEAR = {year} AND S.MONTH = {month} AND S.FACTORY_CODE = '{factory_code}' AND P.PILLAR_CODE = '{pillar_code}'
                        AND C.CODE <> 'SAF00'
                    GROUP BY S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO, LV.LEVEL_NO, C.TYPE, C.CODE, C.ENG_NAME, C.VIE_NAME, C.CODE, C.PILLAR_CODE, P.PILLAR_CODE
                ),
                C_SAF00 AS (
                    SELECT C.YEAR, C.MONTH, C.FACTORY_CODE, C.PILLAR_NO, C.LEVEL_NO, T.TYPE, T.CODE,
                        CASE 
                            WHEN {is_eng} = 1 AND {is_vie} = 1 THEN T.ENG_NAME || ' / ' || T.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN T.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN T.VIE_NAME
                            ELSE '' 
                        END AS NAME,
                        SUM(C.TOTAL_SCORE) AS TOTAL_SCORE,
                        SUM(C.ACTION) AS ACTION,
                        SUM(C.ACTION_NA) AS ACTION_NA,
                        ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT,
                        ROUND(SC.REDUCT_SCORING, 2) AS REDUCT_SCORING,
                        CAST(ROUND(100 - (SC.REDUCT_SCORING / 15 * 8), 2) AS STRING) || '%' AS COMPLIANCE,
                        T.PILLAR_CODE
                    FROM C
                    INNER JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
                    LEFT JOIN (
                        SELECT SC.CHAPTER_CODE, SUM(SC.REDUCT_SCORING) AS REDUCT_SCORING
                        FROM SC
                        WHERE SC.CHAPTER_CODE = 'SAF00'
                        GROUP BY SC.CHAPTER_CODE
                    ) SC ON SC.CHAPTER_CODE = T.CODE
                    WHERE C.PILLAR_CODE = 'SAF'
                    GROUP BY C.YEAR, C.MONTH, C.FACTORY_CODE, C.PILLAR_NO, C.LEVEL_NO, T.TYPE, T.CODE,
                        T.ENG_NAME, T.VIE_NAME, SC.REDUCT_SCORING, T.PILLAR_CODE
                )
                SELECT YEAR, MONTH, FACTORY_CODE, LEVEL_NO, TYPE, CODE, NAME, TOTAL_SCORE, ACTION, ACTION_NA, ASSESSMENT,
                    TOTAL_SCORE_PILLAR, FINAL_SCORE, GRADE, REDUCT_SCORING, COMPLIANCE
                FROM (
                    -- Pillar
                    SELECT S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO AS PILLAR_NO, -1 AS LEVEL_NO, 'Pillar' AS TYPE, P.PILLAR_CODE AS CODE,
                        CASE 
                            WHEN {is_eng} = 1 AND {is_vie} = 1 THEN P.PILLAR_ENG_NAME || ' / ' || P.PILLAR_VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN P.PILLAR_ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN P.PILLAR_VIE_NAME
                            ELSE '' 
                        END AS NAME,
                        CASE WHEN P.PILLAR_CODE <> 'SAF' THEN SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) END AS TOTAL_SCORE,
                        CASE WHEN P.PILLAR_CODE <> 'SAF' THEN COUNT(1) END AS ACTION,
                        CASE WHEN P.PILLAR_CODE <> 'SAF' THEN COUNT(CASE WHEN S.SCORE = -1 THEN 1 END) END AS ACTION_NA,
                        CASE WHEN P.PILLAR_CODE <> 'SAF' THEN ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2) END AS Assessment,
                        CASE WHEN P.PILLAR_CODE = 'ENV' THEN COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3 ELSE NULL END AS Total_score_pillar,
                        CASE WHEN P.PILLAR_CODE = 'ENV' THEN SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) ELSE NULL END AS Final_score,
                        CASE WHEN P.PILLAR_CODE = 'ENV' THEN CAST(ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 0) AS STRING) || '%' ELSE NULL END AS Grade,
                        NULL AS REDUCT_SCORING, NULL AS COMPLIANCE,
                        NULL AS LV4, NULL AS LV3, NULL AS LV2, P.PILLAR_CODE AS LV1
                    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
                    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = C.TYPE
                    WHERE S.YEAR = {year} AND S.MONTH = {month} AND S.FACTORY_CODE = '{factory_code}' AND P.PILLAR_CODE = '{pillar_code}' AND {is_pillar} = 1
                    GROUP BY S.YEAR, S.MONTH, S.FACTORY_CODE, P.ORDER_NO, P.PILLAR_CODE, P.PILLAR_ENG_NAME, P.PILLAR_VIE_NAME

                    UNION ALL
                    -- Chapter
                    SELECT YEAR, MONTH, FACTORY_CODE, PILLAR_NO, LEVEL_NO, TYPE, CODE, NAME, TOTAL_SCORE, ACTION, ACTION_NA, Assessment,
                        NULL AS Total_score_pillar, NULL AS Final_score, NULL AS Grade, NULL AS REDUCT_SCORING, NULL AS COMPLIANCE,
                        NULL AS LV4, NULL AS LV3, C.CODE AS LV2, C.PILLAR_CODE AS LV1
                    FROM C
                    WHERE {is_chapter} = 1

                    UNION ALL
                    -- Chapter SAF00
                    SELECT YEAR, MONTH, FACTORY_CODE, PILLAR_NO, LEVEL_NO, TYPE, CODE, NAME, SUM(TOTAL_SCORE) AS TOTAL_SCORE, SUM(ACTION) AS ACTION, SUM(ACTION_NA) AS ACTION_NA, SUM(Assessment) AS Assessment,
                        NULL AS Total_score_pillar, ROUND(SUM(TOTAL_SCORE) - SUM(REDUCT_SCORING), 1) AS Final_score,
                        CAST(ROUND((SUM(TOTAL_SCORE) - SUM(REDUCT_SCORING)) / (SUM(Assessment) * 100 * 3) * 100, 0) AS STRING) || '%' AS Grade,
                        ROUND(SUM(REDUCT_SCORING), 2) AS REDUCT_SCORING, C.COMPLIANCE,
                        NULL AS LV4, NULL AS LV3, C.CODE AS LV2, C.PILLAR_CODE AS LV1
                    FROM C_SAF00 C
                    WHERE {is_chapter} = 1
                    GROUP BY YEAR, MONTH, FACTORY_CODE, PILLAR_NO, LEVEL_NO, TYPE, CODE, NAME, C.CODE, C.PILLAR_CODE, C.COMPLIANCE

                    UNION ALL
                    -- Sub_chapter
                    SELECT YEAR, MONTH, FACTORY_CODE, PILLAR_NO, LEVEL_NO, TYPE, CODE, NAME, TOTAL_SCORE, ACTION, ACTION_NA, Assessment,
                        NULL AS Total_score_pillar, NULL AS Final_score, NULL AS Grade, REDUCT_SCORING, COMPLIANCE,
                        NULL AS LV4, SC.CODE AS LV3, SC.CHAPTER_CODE AS LV2, SC.PILLAR_CODE AS LV1
                    FROM SC
                    WHERE {is_sub_chapter} = 1
                )
                ORDER BY PILLAR_NO, CODE
                """
                
                # Execute main query with parameters
                cursor.execute(main_query, (
                    # SC CTE parameters
                    is_eng, is_vie, is_eng, is_vie, is_eng, is_vie,
                    year, month, factory_code, pillar_code,
                    # C CTE parameters  
                    is_eng, is_vie, is_eng, is_vie, is_eng, is_vie,
                    year, month, factory_code, pillar_code,
                    # C_SAF00 CTE parameters
                    is_eng, is_vie, is_eng, is_vie, is_eng, is_vie,
                    is_eng, is_vie, is_eng, is_vie, is_eng, is_vie,
                    # Pillar section parameters
                    is_eng, is_vie, is_eng, is_vie, is_eng, is_vie,
                    year, month, factory_code, pillar_code, is_pillar,
                    # Chapter sections parameters
                    is_chapter, is_chapter,
                    # Sub_chapter parameters
                    is_sub_chapter
                ))
                
                results = cursor.fetchall()
                columns = [column[0] for column in cursor.description]

                # Process results into JSON
                data = []
                for row in results:
                    row_dict = dict(zip(columns, row))
                    
                    # Determine format based on LEVEL_NO
                    level_no = int(row_dict.get('LEVEL_NO') or 0)
                    if level_no == 0:  # Chapter
                        format_style = {
                            "backgroundColor": "#f6f6f3",  # Gray background
                            "color": "#000000",  # Black text
                            "fontWeight": "bold",
                            "fontStyle": "normal",
                            "fontSize": 12
                        }
                    elif level_no == -1:  # Pillar
                        format_style = {
                            "backgroundColor": "#e5f1ff",  # Blue background
                            "color": "#000000",           # Black text
                            "fontWeight": "bold",         # Bold
                            "fontStyle": "normal",        # Normal font style
                            "fontSize": 12
                        }
                    else:  # Sub_Chapter
                        format_style = {
                            "backgroundColor": "#FFFFFF",
                            "color": "#000000",
                            "fontWeight": "normal",
                            "fontStyle": "normal",
                            "fontSize": 11
                        }

                    item = {
                        "YEAR": int(row_dict['YEAR']),
                        "MONTH": int(row_dict['MONTH']),
                        "FACTORY_CODE": row_dict['FACTORY_CODE'],
                        "LEVEL_NO": level_no,
                        "TYPE": row_dict['TYPE'],
                        "CODE": row_dict['CODE'],
                        "NAME": row_dict['NAME'],
                        "TOTAL_SCORE": float(row_dict['TOTAL_SCORE']) if row_dict['TOTAL_SCORE'] is not None else None,
                        "ACTION": int(row_dict['ACTION']) if row_dict['ACTION'] is not None else None,
                        "ACTION_NA": int(row_dict['ACTION_NA']) if row_dict['ACTION_NA'] is not None else None,
                        "ASSESSMENT": float(row_dict['ASSESSMENT']) if row_dict['ASSESSMENT'] is not None else None,
                        "REDUCT_SCORING": float(row_dict['REDUCT_SCORING']) if row_dict['REDUCT_SCORING'] is not None else None,
                        "COMPLIANCE": row_dict['COMPLIANCE'],
                        "TOTAL_SCORE_PILLAR": float(row_dict['TOTAL_SCORE_PILLAR']) if row_dict['TOTAL_SCORE_PILLAR'] is not None else None,
                        "FINAL_SCORE": float(row_dict['FINAL_SCORE']) if row_dict['FINAL_SCORE'] is not None else None,
                        "GRADE": row_dict['GRADE'],
                        "FORMAT": format_style
                    }

                    data.append(item)

                # Chapter query for radar chart
                chapter_query = f"""
                        WITH SC AS (
                            SELECT
                                SC.CODE,
                                ROUND(
                                    SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2
                                ) AS Assessment,
                                C.CODE AS CHAPTER_CODE
                            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                            WHERE
                                S.YEAR = {year}
                                AND S.MONTH = {month}
                                AND S.FACTORY_CODE = '{factory_code}'
                                AND C.PILLAR_CODE = '{pillar_code}'
                            GROUP BY SC.CODE, C.CODE
                        ),
                        C AS (
                            SELECT
                                C.CODE,
                                C.ENG_NAME,
                                ROUND(
                                    SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2
                                ) AS ASSESSMENT,
                                C.PILLAR_CODE
                            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                            WHERE
                                S.YEAR = {year}
                                AND S.MONTH = {month}
                                AND S.FACTORY_CODE = '{factory_code}'
                                AND C.PILLAR_CODE = '{pillar_code}'
                                AND C.CODE <> 'SAF00'
                            GROUP BY C.CODE, C.ENG_NAME, C.PILLAR_CODE
                        ),
                        C_SAF00 AS (
                            SELECT
                                T.CODE,
                                C00.ENG_NAME,
                                ROUND(SUM(C.ASSESSMENT), 2) ASSESSMENT
                            FROM C
                            INNER JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
                            LEFT JOIN (
                                SELECT SC.CHAPTER_CODE
                                FROM SC
                                WHERE SC.CHAPTER_CODE = 'SAF00'
                                GROUP BY SC.CHAPTER_CODE
                            ) SC ON SC.CHAPTER_CODE = T.CODE
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C00 ON C00.CODE = 'SAF00' AND C00.TYPE = 'Chapter'
                            WHERE C.PILLAR_CODE = 'SAF'
                            GROUP BY T.CODE, C00.ENG_NAME
                        )
                        -- Chapter
                        SELECT
                            CODE,
                            CONCAT(CODE, ' - ', ENG_NAME) AS ENG_NAME,
                            ROUND(Assessment, 0) AS Assessment
                        FROM C
                        UNION ALL
                        -- Chapter SAF00
                        SELECT
                            CODE,
                            CONCAT(CODE, ' - ', ENG_NAME) AS ENG_NAME,
                            ROUND(SUM(Assessment), 0) AS Assessment
                        FROM C_SAF00
                        GROUP BY CODE, ENG_NAME
                        ORDER BY CODE
                        """


                cursor.execute(chapter_query, (year, month, factory_code, pillar_code,
                                             year, month, factory_code, pillar_code))
                chapter_results = cursor.fetchall()
                chapter_columns = [column[0] for column in cursor.description]

                # Extract data for radar chart
                chapter_labels = []
                assessment_values = []
                for chapter_row in chapter_results:
                    chapter_dict = dict(zip(chapter_columns, chapter_row))
                    chapter_labels.append(chapter_dict['ENG_NAME'])
                    assessment_values.append(chapter_dict['Assessment'])

                # Radar chart data
                radar_chart = {
                    "values": assessment_values,
                    "label_name": "% Assessment",
                    "height": 500,
                    "width": 3000,
                    "labels": chapter_labels
                }

                # Analysis query for monthly trends
                query_analysis = f"""
                        WITH SC AS (
                            SELECT
                                S.YEAR,
                                S.MONTH,
                                SC.CODE,
                                ROUND(
                                    SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2
                                ) AS Assessment,
                                C.CODE AS CHAPTER_CODE
                            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                            WHERE
                                S.YEAR = {year}
                                AND S.FACTORY_CODE = '{factory_code}'
                                AND C.PILLAR_CODE = '{pillar_code}'
                            GROUP BY S.YEAR, S.MONTH, SC.CODE, C.CODE
                        ),
                        C AS (
                            SELECT
                                S.YEAR,
                                S.MONTH,
                                0 AS LEVEL_NO,
                                C.CODE,
                                C.ENG_NAME,
                                ROUND(
                                    SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2
                                ) AS ASSESSMENT,
                                C.PILLAR_CODE
                            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                            WHERE
                                S.YEAR = {year}
                                AND S.FACTORY_CODE = '{factory_code}'
                                AND C.PILLAR_CODE = '{pillar_code}'
                                AND C.CODE <> 'SAF00'
                            GROUP BY S.YEAR, S.MONTH, C.CODE, C.ENG_NAME, C.PILLAR_CODE
                        ),
                        C_SAF00 AS (
                            SELECT
                                C.YEAR,
                                C.MONTH,
                                0 AS LEVEL_NO,
                                T.CODE,
                                T.ENG_NAME,
                                ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT
                            FROM C
                            INNER JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
                            WHERE C.PILLAR_CODE = 'SAF'
                            GROUP BY C.YEAR, C.MONTH, T.CODE, T.ENG_NAME
                        ),
                        PILLAR AS (
                            SELECT
                                S.YEAR,
                                S.MONTH,
                                1 AS LEVEL_NO,
                                P.PILLAR_CODE AS CODE,
                                P.PILLAR_ENG_NAME AS ENG_NAME,
                                CASE
                                    WHEN P.PILLAR_CODE <> 'SAF' THEN
                                        ROUND(
                                            SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2
                                        )
                                END AS ASSESSMENT
                            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
                            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = C.TYPE
                            WHERE
                                P.PILLAR_CODE <> 'SAF'
                                AND S.YEAR = {year}
                                AND S.FACTORY_CODE = '{factory_code}'
                                AND P.PILLAR_CODE = '{pillar_code}'
                            GROUP BY S.YEAR, S.MONTH, P.PILLAR_CODE, P.PILLAR_ENG_NAME

                            UNION ALL

                            SELECT
                                C.YEAR,
                                C.MONTH,
                                1 AS LEVEL_NO,
                                C.PILLAR_CODE AS CODE,
                                '' AS ENG_NAME,
                                ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT
                            FROM C
                            WHERE C.PILLAR_CODE = 'SAF'
                            GROUP BY C.YEAR, C.MONTH, C.PILLAR_CODE
                        )

                        SELECT
                            CODE,
                            Chapter_Name,
                            Jan_{year % 100}, Feb_{year % 100}, Mar_{year % 100}, Apr_{year % 100},
                            May_{year % 100}, Jun_{year % 100}, Jul_{year % 100}, Aug_{year % 100},
                            Sep_{year % 100}, Oct_{year % 100}, Nov_{year % 100}, Dec_{year % 100}
                        FROM (
                            SELECT
                                LEVEL_NO,
                                CODE,
                                ENG_NAME AS Chapter_Name,
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 1) THEN ROUND(Assessment, 0) END) AS Jan_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 2) THEN ROUND(Assessment, 0) END) AS Feb_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 3) THEN ROUND(Assessment, 0) END) AS Mar_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 4) THEN ROUND(Assessment, 0) END) AS Apr_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 5) THEN ROUND(Assessment, 0) END) AS May_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 6) THEN ROUND(Assessment, 0) END) AS Jun_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 7) THEN ROUND(Assessment, 0) END) AS Jul_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 8) THEN ROUND(Assessment, 0) END) AS Aug_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 9) THEN ROUND(Assessment, 0) END) AS Sep_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 10) THEN ROUND(Assessment, 0) END) AS Oct_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 11) THEN ROUND(Assessment, 0) END) AS Nov_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 12) THEN ROUND(Assessment, 0) END) AS Dec_{year % 100}
                            FROM C
                            GROUP BY CODE, ENG_NAME, LEVEL_NO

                            UNION ALL

                            SELECT
                                LEVEL_NO,
                                CODE,
                                ENG_NAME AS Chapter_Name,
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 1) THEN ROUND(Assessment, 0) END) AS Jan_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 2) THEN ROUND(Assessment, 0) END) AS Feb_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 3) THEN ROUND(Assessment, 0) END) AS Mar_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 4) THEN ROUND(Assessment, 0) END) AS Apr_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 5) THEN ROUND(Assessment, 0) END) AS May_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 6) THEN ROUND(Assessment, 0) END) AS Jun_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 7) THEN ROUND(Assessment, 0) END) AS Jul_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 8) THEN ROUND(Assessment, 0) END) AS Aug_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 9) THEN ROUND(Assessment, 0) END) AS Sep_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 10) THEN ROUND(Assessment, 0) END) AS Oct_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 11) THEN ROUND(Assessment, 0) END) AS Nov_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 12) THEN ROUND(Assessment, 0) END) AS Dec_{year % 100}
                            FROM C_SAF00
                            GROUP BY CODE, ENG_NAME, LEVEL_NO

                            UNION ALL

                            -- Pillar
                            SELECT
                                LEVEL_NO,
                                '' AS CODE,
                                CONCAT('Total Pillar ', CODE) AS Chapter_Name,
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 1) THEN ROUND(Assessment, 0) END) AS Jan_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 2) THEN ROUND(Assessment, 0) END) AS Feb_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 3) THEN ROUND(Assessment, 0) END) AS Mar_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 4) THEN ROUND(Assessment, 0) END) AS Apr_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 5) THEN ROUND(Assessment, 0) END) AS May_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 6) THEN ROUND(Assessment, 0) END) AS Jun_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 7) THEN ROUND(Assessment, 0) END) AS Jul_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 8) THEN ROUND(Assessment, 0) END) AS Aug_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 9) THEN ROUND(Assessment, 0) END) AS Sep_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 10) THEN ROUND(Assessment, 0) END) AS Oct_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 11) THEN ROUND(Assessment, 0) END) AS Nov_{year % 100},
                                SUM(CASE WHEN (YEAR = {year} AND MONTH = 12) THEN ROUND(Assessment, 0) END) AS Dec_{year % 100}
                            FROM PILLAR
                            GROUP BY LEVEL_NO, CONCAT('Total Pillar ', CODE)
                        )
                        ORDER BY LEVEL_NO, CODE
                        """

                # Execute analysis query using f-string (since it's complex with dynamic column names)
                cursor.execute(query_analysis)
                analysis_results = cursor.fetchall()
                analysis_columns = [column[0] for column in cursor.description]

                # Process results into data_analysis format
                data_analysis = []
                for row in analysis_results:
                    row_dict = dict(zip(analysis_columns, row))
                    chapter_data = {
                        "chapper_code": row_dict["CODE"],
                        "chapper_name": row_dict["Chapter_Name"],
                        "header_format": {"backgroundColor": "#e5f1ff"},
                        "format": {"backgroundColor": "#ffffff"},
                        "data": []
                    }

                    # Add monthly data
                    for month_key in [
                        f"Jan_{year % 100}", f"Feb_{year % 100}", f"Mar_{year % 100}",
                        f"Apr_{year % 100}", f"May_{year % 100}", f"Jun_{year % 100}",
                        f"Jul_{year % 100}", f"Aug_{year % 100}", f"Sep_{year % 100}",
                        f"Oct_{year % 100}", f"Nov_{year % 100}", f"Dec_{year % 100}"
                    ]:
                        value = row_dict.get(month_key, None)
                        chapter_data["data"].append({
                            "name": month_key.replace("_", "-"),
                            "value": convert_to_percentage(value),
                            "format": {
                                "backgroundColor": "#e5f1ff"  # Light blue background
                            } if month_key == f"{['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month - 1]}_{year % 100}" else {}
                        })

                    data_analysis.append(chapter_data)

                return func.HttpResponse(
                    json.dumps({
                        "code": 200, 
                        "message": "Success", 
                        "data": data, 
                        "chart_radar": radar_chart, 
                        "data_analysis": data_analysis
                    }, ensure_ascii=False, indent=4),
                    status_code=200,
                    headers=headers
                )

    except jwt.ExpiredSignatureError:
        return func.HttpResponse(
            json.dumps({'message': "Token đã hết hạn!"}),
            status_code=401,
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
        return func.HttpResponse(
            json.dumps({"code": 500, "status": "error", "message": str(e)}, ensure_ascii=False, indent=4),
            status_code=500,
            headers=headers
        )