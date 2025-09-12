import azure.functions as func
import pandas as pd
import json
from io import BytesIO, StringIO
import smtplib
import email_config
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
import jwt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from azure.identity import DefaultAzureCredential
import databricks.sql as sql
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

server_hostname = "adb-1538821690907541.1.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/b6d556c5cf816ae6"
database = "default"
credential = DefaultAzureCredential()
databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
access_token = credential.get_token(databricks_resource_id + "/.default").token

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
        year = request_json.get("Year")
        month = request_json.get("Month")
        factory_code = request_json.get("Factory_Code")
        pillar_code = request_json.get("Pillar_Code", None)
        is_eng = request_json.get("Is_ENG", 0)
        is_vie = request_json.get("Is_VIE", 1)
        email_to = request_json.get("Email")
        file_type = request_json.get("Type", "excel").lower()
        if not all([year, month, factory_code, email_to]):
            return func.HttpResponse(json.dumps({"error": "Missing required fields: Year, Month, Factory_Code, Email"}), status_code=400, headers=headers)
        pillar_filter = f"AND Pillar.PILLAR_CODE = '{pillar_code}'" if pillar_code else ""
        # Databricks SQL query (adjust table references for Databricks)
        query = f"""
SELECT {year} AS YEAR, {month} AS MONTH,
                   F.FACTORY_CODE, CASE WHEN {is_eng} = 1 AND {is_vie} = 1 THEN F.FACTORY_ENG_NAME ||' / '|| F.FACTORY_NAME
                        WHEN {is_eng} = 1 AND {is_vie} = 0 THEN F.FACTORY_ENG_NAME
                        WHEN {is_eng} = 0 AND {is_vie} = 1 THEN F.FACTORY_NAME
                        ELSE '' END AS FACTORY_NAME,
                   PILLAR.PILLAR_CODE,
                   CASE WHEN {is_eng} = 1 AND {is_vie} = 1 THEN PILLAR.PILLAR_ENG_NAME ||' / '|| PILLAR.PILLAR_VIE_NAME
                        WHEN {is_eng} = 1 AND {is_vie} = 0 THEN PILLAR.PILLAR_ENG_NAME
                        WHEN {is_eng} = 0 AND {is_vie} = 1 THEN PILLAR.PILLAR_VIE_NAME
                        ELSE '' END AS PILLAR_NAME,
                   Chapter.CHAPTER_CODE, 
                   Chapter.CHAPTER_NAME,
                   Sub_chapter.SUB_CHAPTER_CODE, 
                   Sub_chapter.SUB_CHAPTER_NAME,
                   KPI.KPI_CODE AS ITEM_CODE, 
                   KPI.KPI_NAME AS ITEM_REQUIREMENT,
                   S.SCORE AS SCORE
            FROM 
                (SELECT T.PARENT_CODE AS SUB_CHAPTER_CODE, T.CODE AS KPI_CODE, 
                        CASE WHEN {is_eng} = 1 AND {is_vie} = 1 THEN T.ENG_NAME ||' / '|| T.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN T.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN T.VIE_NAME
                            ELSE '' END AS KPI_NAME 
                 FROM udp_wcm_dev.MFG_OMS.OMS_TYPE_D T 
                 WHERE T.TYPE = 'Item') KPI
            INNER JOIN  
                (SELECT T.PARENT_CODE AS CHAPTER_CODE, T.CODE AS SUB_CHAPTER_CODE,
                        CASE WHEN {is_eng} = 1 AND {is_vie} = 1 THEN T.ENG_NAME ||' / '|| T.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN T.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN T.VIE_NAME
                            ELSE '' END AS SUB_CHAPTER_NAME
                 FROM udp_wcm_dev.MFG_OMS.OMS_TYPE_D T 
                 WHERE T.TYPE = 'Sub_chapter') Sub_chapter 
            ON KPI.SUB_CHAPTER_CODE = Sub_chapter.SUB_CHAPTER_CODE
            INNER JOIN 
                (SELECT T.PILLAR_CODE, T.CODE AS CHAPTER_CODE,
                        CASE WHEN {is_eng} = 1 AND {is_vie} = 1 THEN T.ENG_NAME ||' / '|| T.VIE_NAME
                            WHEN {is_eng} = 1 AND {is_vie} = 0 THEN T.ENG_NAME
                            WHEN {is_eng} = 0 AND {is_vie} = 1 THEN T.VIE_NAME
                            ELSE '' END AS CHAPTER_NAME
                 FROM udp_wcm_dev.MFG_OMS.OMS_TYPE_D T 
                 WHERE T.TYPE = 'Chapter') Chapter 
            ON Sub_chapter.CHAPTER_CODE = Chapter.CHAPTER_CODE 
            INNER JOIN udp_wcm_dev.MFG_OMS.OMS_PILLAR_D Pillar 
            ON Chapter.PILLAR_CODE = Pillar.PILLAR_CODE
            LEFT JOIN  udp_wcm_dev.MFG_OMS.OMS_FACTORY_D F 
            ON F.FACTORY_CODE = '{factory_code}'
            LEFT JOIN  udp_wcm_dev.MFG_OMS.OMS_SCORE_F S 
            ON S.YEAR = {year}
               AND S.MONTH = {month}
               AND S.FACTORY_CODE = '{factory_code}'
               AND S.PILLAR_CODE = Pillar.PILLAR_CODE
               AND S.ITEM_CODE = KPI.KPI_CODE
            WHERE 1=1
            {pillar_filter}
            ORDER BY Chapter.CHAPTER_CODE, Sub_chapter.SUB_CHAPTER_CODE, KPI.KPI_CODE
        """
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
        if file_type == "excel":
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="TEMPLATE")
                worksheet = writer.sheets["TEMPLATE"]
                for i, column in enumerate(df.columns):
                    col_letter = get_column_letter(i + 1)
                    if column in ["CHAPTER_NAME", "SUB_CHAPTER_NAME", "ITEM_REQUIREMENT"]:
                        worksheet.column_dimensions[col_letter].width = 25
                        for cell in worksheet[col_letter]:
                            cell.alignment = Alignment(wrap_text=True, vertical="top")
                    else:
                        column_width = max(df[column].astype(str).map(len).max(), len(column)) + 2
                        worksheet.column_dimensions[col_letter].width = column_width
                        for cell in worksheet[col_letter]:
                            cell.alignment = Alignment(vertical="top")
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
        filename = f"OMS_Template_{year}_{month}_{factory_code}_{pillar_code}.{file_extension}"
        send_email(file_data, filename, email_to, content_type)
        return func.HttpResponse(json.dumps({"message": "Xuất Template OMS thành công, vui lòng kiểm tra Email để lấy file.", "code": 200}), status_code=200, headers=headers)
    except jwt.ExpiredSignatureError:
        return func.HttpResponse(json.dumps({'message':"Token đã hết hạn!"}), status_code=401, headers=headers)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, headers=headers)

def send_email(file_data, filename, email_to, content_type):
    try:
        msg = MIMEMultipart()
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Subject"] = "[OMS] - Template"
        body = "Kính gửi Anh/Chị,\n\nTemplate đã được xuất trong file đính kèm.\n\nTrân trọng,\n"
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
