from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import email_config
import smtplib
gmail_password = email_config.email_password
email_from = email_config.email_from
host = email_config.smtp_host
port = email_config.smtp_port

print(gmail_password,email_from,host,port)
def send_email(file_data, filename, email_to, content_type):
    """Gửi email với file Excel hoặc CSV đính kèm."""
    try:
        # Tạo email
        msg = MIMEMultipart()
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Subject"] = "[OMS] - Analysis"

        body = "Kính gửi Anh/Chị,\n\nKết quả đã được xuất trong file đính kèm.\n\nTrân trọng,\n"
        msg.attach(MIMEText(body, 'plain'))

        # Đính kèm file
        attachment = MIMEBase("application", content_type)
        attachment.set_payload(file_data)
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(attachment)

        # Kết nối và gửi email
        server = smtplib.SMTP_SSL(host, port)
        # server.starttls()
        server.login(email_from, gmail_password)
        server.sendmail(email_from, email_to, msg.as_string())
        server.close()
        print("Mail Sent")
    except Exception as e:
        raise Exception(f"Lỗi gửi email: {str(e)}")
    
send_email('[this is the data]',"testing_18-08","nikhil.sahu@celebaltech.com","text/csv")
