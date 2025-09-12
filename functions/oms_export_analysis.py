import azure.functions as func
import json
import jwt
import logging
import socket
import urllib.error
from azure.identity import DefaultAzureCredential
from databricks import sql
import pandas as pd
from io import BytesIO, StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# SECRET_KEY = "OMSTokenSecretKey@123".ljust(32, "x").encode()
# IV = "abcd012345678910".encode()

# gmail_password = email_config.email_password
# email_from = email_config.email_from
# host = email_config.smtp_host
# port = email_config.smtp_port

gmail_password='phmbxqoetiaskrez'
host = 'smtp.gmail.com'
port='465'
email_from="bi-thailand@msc.masangroup.com"

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


def send_email(file_data, filename, email_to, content_type):
    try:
        msg = MIMEMultipart()
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Subject"] = "[OMS] - Analysis"
        body = "Kính gửi Anh/Chị,\n\nKết quả đã được xuất trong file đính kèm.\n\nTrân trọng,\n"
        msg.attach(MIMEText(body, 'plain'))
        attachment = MIMEBase("application", content_type)
        attachment.set_payload(file_data)
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(attachment)
        server = smtplib.SMTP_SSL(host, port)
        server.login(email_from, gmail_password)
        server.sendmail(email_from, email_to, msg.as_string())
        server.close()
    except Exception as e:
        raise Exception(f"Lỗi gửi email: {str(e)}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    if req.method == 'OPTIONS':
        return func.HttpResponse('', status_code=204, headers=headers)
    try:
        # token = req.headers.get('Authorization', '').split('Bearer ')[-1]
        # decoded_payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        # user = decrypt_aes_json(decoded_payload['user'])
        # user_email = user.get("email")
        request_json = req.get_json()
        if not request_json:
            return func.HttpResponse(json.dumps({"error": "Missing or invalid JSON payload"}), status_code=400, headers=headers)
        year = int(request_json.get("Year"))
        month = int(request_json.get("Month"))
        factory_code = request_json.get("Factory_Code")
        pillar_code = request_json.get("Pillar_Code", None)
        is_eng = request_json.get("Is_ENG", 0)
        is_vie = request_json.get("Is_VIE", 1)
        email_to = request_json.get("Email")
        file_type = request_json.get("Type", "excel").lower()
        if not all([year, month, factory_code, email_to]):
            return func.HttpResponse(json.dumps({"error": "Missing required fields: Year, Month, Factory_Code, Email"}), status_code=400, headers=headers)
        # Databricks connection and query
        logging.info('Getting Databricks token.')
        credential = DefaultAzureCredential()
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        access_token = credential.get_token(databricks_resource_id + "/.default").token
        server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
        database = "default"
        # Complete SQL query for Databricks
        # query = f"""
        # WITH SC AS (
        #     SELECT S.YEAR, S.MONTH, SC.CODE,
        #            ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / 
        #            (COUNT(CASE WHEN SCORE <> -1 THEN 1 END) * 3) 
        #            * 100, 2) AS Assessment,
        #            C.CODE AS CHAPTER_CODE
        #     FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
        #     LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
        #     LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
        #     LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
        #     WHERE ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
        #         OR S.YEAR * 100 + S.MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int)
        #         )
        #       AND S.FACTORY_CODE = '{factory_code}' AND C.PILLAR_CODE = '{pillar_code}'
        #     GROUP BY S.YEAR, S.MONTH, SC.CODE, C.CODE
        # )
        # , C AS 
        # (SELECT       S.YEAR, S.MONTH , 0 LEVEL_NO, C.CODE , C.ENG_NAME
        #                     , ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END ) / (COUNT(CASE WHEN SCORE <> -1 THEN 1 END ) *3)
        #                     * 100
        #                     ,2) AS ASSESSMENT
        #                     , C.PILLAR_CODE
        #         FROM        udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE  AND KPI.TYPE = 'Item'
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE  AND SC.TYPE = 'Sub_chapter'
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE  AND C.TYPE = 'Chapter'
        #         WHERE       ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
        #         OR S.YEAR * 100 + S.MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int)
        #         )
        #          AND S.FACTORY_CODE = '{factory_code}' AND C.PILLAR_CODE = '{pillar_code}'
        #                     AND C.CODE <> 'SAF00'      
        #         GROUP BY     S.YEAR, S.MONTH , C.CODE, C.ENG_NAME, C.PILLAR_CODE
        # ) 
        # , C_SAF00 AS (
        # SELECT      C.YEAR, C.MONTH , 0 LEVEL_NO, T.CODE , T.ENG_NAME
        #             , ROUND(SUM(C.ASSESSMENT),2) AS ASSESSMENT
        # FROM        C  
        # INNER JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
        # lEFT JOIN   (SELECT SC.YEAR, SC.MONTH, SC.CHAPTER_CODE
        #              FROM SC 
        #              WHERE SC.CHAPTER_CODE = 'SAF00'
        #              GROUP BY SC.YEAR, SC.MONTH, SC.CHAPTER_CODE) SC 
        #              ON SC.CHAPTER_CODE = T.CODE AND SC.YEAR = C.YEAR AND SC.MONTH = C.MONTH
        # WHERE       C.PILLAR_CODE = 'SAF'       
        # GROUP BY    C.YEAR, C.MONTH , T.CODE , T.ENG_NAME
        # ),  PILLAR AS (
        #         SELECT S.YEAR, S.MONTH, 1 LEVEL_NO, P.PILLAR_CODE CODE
        #                     , P.PILLAR_ENG_NAME AS ENG_NAME
        #                     , CASE WHEN P.PILLAR_CODE <> 'SAF' THEN 
        #                                 ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END ) *3) 
        #                                 * 100,2) END AS ASSESSMENT
                            
        #         FROM        udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE  AND KPI.TYPE = 'Item'
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE  AND SC.TYPE = 'Sub_chapter'
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE  AND C.TYPE = 'Chapter'
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
        #         LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = C.TYPE
        #         WHERE       P.PILLAR_CODE <> 'SAF' 
        #                     AND ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
        #                             OR S.YEAR * 100 + S.MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int)
        #                             )
        #                     AND S.FACTORY_CODE = '{factory_code}' AND P.PILLAR_CODE = '{pillar_code}'
        #         GROUP BY    S.YEAR, S.MONTH, P.PILLAR_CODE , P.PILLAR_ENG_NAME
        #         UNION ALL
        #         SELECT      C.YEAR, C.MONTH, 1 LEVEL_NO, C.PILLAR_CODE CODE, '' AS ENG_NAME
        #                     , ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT
        #         FROM        C
        #         WHERE       C.PILLAR_CODE = 'SAF'
        #         GROUP BY    C.YEAR, C.MONTH, C.PILLAR_CODE
        # )

        #         SELECT      CODE, Chapter_Name
        #                     , Assessment 
        #                     ,  `M-1`
        #                     , `VS_M-1`
        #         FROM (

        #         --Chapter
        #         SELECT      LEVEL_NO, CODE, ENG_NAME AS Chapter_Name 
        #                     ,  SUM(CASE WHEN  (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment 
        #                     ,  SUM(CASE WHEN  (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) END) AS `M-1`
        #                     ,  SUM(CASE WHEN  (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
        #                     -  SUM(CASE WHEN  (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
        #         FROM        C     
        #         GROUP BY    LEVEL_NO, CODE , ENG_NAME
        #         UNION ALL --Chapter SAF00
        #         SELECT      LEVEL_NO, CODE, ENG_NAME AS Chapter_Name
        #                     ,  SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment 
        #                     ,  SUM(CASE WHEN (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) END) AS `M-1`
        #                     ,  SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
        #                     -  SUM(CASE WHEN (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
        #         FROM        C_SAF00 C
        #         GROUP BY    LEVEL_NO, CODE , ENG_NAME
                
        #         UNION ALL
        #         --Pillar
        #         SELECT  LEVEL_NO, '' CODE, CONCAT('Total Pillar ', CODE) Chapter_Name
        #                 ,  SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment
        #                 ,  SUM(CASE WHEN (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) END) AS `M-1`
        #                     ,  SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
        #                     -  SUM(CASE WHEN (YEAR * 100 + MONTH = cast(date_format(add_months(to_date('{year}-{month:02d}-01'), -1), 'yyyyMM') as int))
        #                                THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
        #         --Pillar
        #         FROM Pillar
        #         GROUP BY LEVEL_NO,  CONCAT('Total Pillar ', CODE)
        #         )
        #         ORDER BY LEVEL_NO, CODE
        # """

        query = f"""
WITH SC_AGG AS (
    SELECT S.YEAR, S.MONTH, SC.CODE,
           ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / 
           (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2) AS Assessment,
           C.CODE AS CHAPTER_CODE
    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
    WHERE ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
        OR S.YEAR * 100 + S.MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
      AND S.FACTORY_CODE = '{factory_code}' 
      AND C.PILLAR_CODE = '{pillar_code}'
    GROUP BY S.YEAR, S.MONTH, SC.CODE, C.CODE
),
C AS (
    SELECT S.YEAR, S.MONTH, 0 LEVEL_NO, C.CODE, C.ENG_NAME,
           ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / 
           (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2) AS ASSESSMENT,
           C.PILLAR_CODE
    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
    WHERE ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
        OR S.YEAR * 100 + S.MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
      AND S.FACTORY_CODE = '{factory_code}' 
      AND C.PILLAR_CODE = '{pillar_code}'
      AND C.CODE <> 'SAF00'
    GROUP BY S.YEAR, S.MONTH, C.CODE, C.ENG_NAME, C.PILLAR_CODE
),
C_SAF00 AS (
    SELECT C.YEAR, C.MONTH, 0 LEVEL_NO, T.CODE, T.ENG_NAME,
           ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT
    FROM C  
    INNER JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
    LEFT JOIN (
        SELECT SC_AGG.YEAR, SC_AGG.MONTH, SC_AGG.CHAPTER_CODE
        FROM SC_AGG
        WHERE SC_AGG.CHAPTER_CODE = 'SAF00'
        GROUP BY SC_AGG.YEAR, SC_AGG.MONTH, SC_AGG.CHAPTER_CODE
    ) SCX ON SCX.CHAPTER_CODE = T.CODE AND SCX.YEAR = C.YEAR AND SCX.MONTH = C.MONTH
    WHERE C.PILLAR_CODE = 'SAF'
    GROUP BY C.YEAR, C.MONTH, T.CODE, T.ENG_NAME
),
PILLAR AS (
    SELECT S.YEAR, S.MONTH, 1 LEVEL_NO, P.PILLAR_CODE CODE,
           P.PILLAR_ENG_NAME AS ENG_NAME,
           CASE WHEN P.PILLAR_CODE <> 'SAF' THEN 
                ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END) / 
                (COUNT(CASE WHEN S.SCORE <> -1 THEN 1 END) * 3) * 100, 2) END AS ASSESSMENT
    FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE AND KPI.TYPE = 'Item'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE AND SC.TYPE = 'Sub_chapter'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE AND C.TYPE = 'Chapter'
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D P ON P.PILLAR_CODE = C.PILLAR_CODE
    LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_LEVEL_TYPE_D LV ON LV.TYPE = C.TYPE
    WHERE P.PILLAR_CODE <> 'SAF'
      AND ((S.YEAR * 100 + S.MONTH = {year} * 100 + {month})
          OR S.YEAR * 100 + S.MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
      AND S.FACTORY_CODE = '{factory_code}' 
      AND P.PILLAR_CODE = '{pillar_code}'
    GROUP BY S.YEAR, S.MONTH, P.PILLAR_CODE, P.PILLAR_ENG_NAME
    UNION ALL
    SELECT C.YEAR, C.MONTH, 1 LEVEL_NO, C.PILLAR_CODE CODE, '' AS ENG_NAME,
           ROUND(SUM(C.ASSESSMENT), 2) AS ASSESSMENT
    FROM C
    WHERE C.PILLAR_CODE = 'SAF'
    GROUP BY C.YEAR, C.MONTH, C.PILLAR_CODE
)

SELECT CODE, Chapter_Name, Assessment, `M-1`, `VS_M-1`
FROM (
    -- Chapter
    SELECT LEVEL_NO, CODE, ENG_NAME AS Chapter_Name,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment,
           SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) END) AS `M-1`,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
           - SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
    FROM C     
    GROUP BY LEVEL_NO, CODE, ENG_NAME

    UNION ALL
    -- Chapter SAF00
    SELECT LEVEL_NO, CODE, ENG_NAME AS Chapter_Name,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment,
           SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) END) AS `M-1`,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
           - SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
    FROM C_SAF00 C
    GROUP BY LEVEL_NO, CODE, ENG_NAME

    UNION ALL
    -- Pillar
    SELECT LEVEL_NO, '' CODE, CONCAT('Total Pillar ', CODE) Chapter_Name,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) END) AS Assessment,
           SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) END) AS `M-1`,
           SUM(CASE WHEN (YEAR = {year} AND MONTH = {month}) THEN ROUND(Assessment,0) ELSE 0 END) 
           - SUM(CASE WHEN (YEAR * 100 + MONTH = CAST(date_format(add_months(to_date('{year}{month:02d}', 'yyyyMM'), -1), 'yyyyMM') AS INT))
                    THEN ROUND(Assessment,0) ELSE 0 END) AS `VS_M-1`
    FROM PILLAR
    GROUP BY LEVEL_NO, CONCAT('Total Pillar ', CODE)
)
ORDER BY LEVEL_NO, CODE;
"""

        # Additional query for Excel output
        additional_query = f"""
        WITH SC AS (
            SELECT S.YEAR, S.MONTH, SC.CODE,
                ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END ) / (COUNT(CASE WHEN SCORE <> -1 THEN 1 END ) *3)
                * 100,2) AS Assessment,
                C.CODE CHAPTER_CODE
            FROM udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE  AND KPI.TYPE = 'Item'
            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE  AND SC.TYPE = 'Sub_chapter'
            LEFT JOIN udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE  AND C.TYPE = 'Chapter'
            WHERE S.YEAR = {year} AND S.FACTORY_CODE = '{factory_code}' AND C.PILLAR_CODE = '{pillar_code}'
            GROUP BY S.YEAR, S.MONTH, SC.CODE, C.CODE
        )
        , C AS 
        (SELECT C.CODE , C.ENG_NAME
                            , ROUND(SUM(CASE WHEN S.SCORE <> -1 THEN S.SCORE END ) / (COUNT(CASE WHEN SCORE <> -1 THEN 1 END ) *3)
                            * 100,2) AS ASSESSMENT
                            , C.PILLAR_CODE
                FROM        udp_wcm_dev.MFG_OMS.OMS_SCORE_F S
                LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D KPI ON S.ITEM_CODE = KPI.CODE  AND KPI.TYPE = 'Item'
                LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D SC ON KPI.PARENT_CODE = SC.CODE  AND SC.TYPE = 'Sub_chapter'
                LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D C ON SC.PARENT_CODE = C.CODE  AND C.TYPE = 'Chapter'
                WHERE S.YEAR = {year} AND S.FACTORY_CODE = '{factory_code}' AND C.PILLAR_CODE = '{pillar_code}' AND C.CODE <> 'SAF00'      
                GROUP BY C.CODE, C.ENG_NAME, C.PILLAR_CODE
        )
        , C_SAF00 AS (
        SELECT      T.CODE  , C00.ENG_NAME
                    , ROUND(SUM(C.ASSESSMENT),2) AS ASSESSMENT
        FROM        C  
        INNER JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D T ON T.TYPE = 'Chapter' AND T.CODE = 'SAF00'
        LEFT JOIN   (SELECT SC.CHAPTER_CODE
                     FROM SC 
                     WHERE SC.CHAPTER_CODE = 'SAF00'
                     GROUP BY SC.CHAPTER_CODE) SC ON SC.CHAPTER_CODE = T.CODE
        LEFT JOIN   udp_wcm_dev.MFG_OMS.OMS_TYPE_D C00 ON  C00.CODE = 'SAF00' AND C00.TYPE = 'Chapter'
        WHERE       C.PILLAR_CODE = 'SAF'       
        GROUP BY    T.CODE  , C00.ENG_NAME
        )
        SELECT      CODE , CONCAT(CODE, ' - ', ENG_NAME) AS ENG_NAME,  ROUND(Assessment,0) Assessment
        FROM        C
        UNION ALL --Chapter SAF00
        SELECT      CODE , CONCAT(CODE, ' - ', ENG_NAME) AS ENG_NAME,  ROUND(SUM(Assessment),0) Assessment            
        FROM        C_SAF00 C
        GROUP BY    CODE, ENG_NAME
        ORDER BY    CODE
        """

        # Execute main query
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
        df = pd.DataFrame(results)
        df.fillna(0, inplace=True)
        df.replace([float('inf'), float('-inf')], 0, inplace=True)

        # Execute additional query
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(additional_query)
                columns_add = [desc[0] for desc in cursor.description]
                results_add = [dict(zip(columns_add, row)) for row in cursor.fetchall()]
        df_additional = pd.DataFrame(results_add)
        df_additional.fillna(0, inplace=True)
        df_additional.replace([float('inf'), float('-inf')], 0, inplace=True)

        if file_type == "excel":
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Analyze_Pillar")
                df_additional.to_excel(writer, index=False, sheet_name="Additional_Data")
            output.seek(0)
            file_data = output.getvalue()
            file_extension = "xlsx"
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            output = StringIO()
            df.to_csv(output, index=False, encoding="utf-8-sig")
            file_data = output.getvalue()
            file_extension = "csv"
            content_type = "text/csv"
        filename = f"OMS_Analysis_{year}_{month}_{factory_code}_{pillar_code}.{file_extension}"
        send_email(file_data, filename, email_to, content_type)
        return func.HttpResponse(json.dumps({"message": "Xuất Analysis OMS thành công, vui lòng kiểm tra Email để lấy file.", "code": 200}), status_code=200, headers=headers)
    except jwt.ExpiredSignatureError:
        return func.HttpResponse(json.dumps({'message':"Token đã hết hạn!"}), status_code=401, headers=headers)
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
