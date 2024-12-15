import sqlalchemy as sal
import pyodbc
import pymysql
import pandas as pd
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(filename='server.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')


def migrate_data():
    try:
        with open("configfiles\\mysql_server.json") as fp:
            cred = json.load(fp)

        msql_engine = sal.create_engine(
            f"mysql+pymysql://{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}")
        msql_conn = msql_engine.connect()
        logging.info("MYSQL connection successful")

        ssms_engine = sal.create_engine(
            "mssql+pyodbc://DESKTOP-JTQ8MC6\\SQLEXPRESS/python?driver=ODBC+Driver+17+for+SQL+Server")
        ssms_conn = ssms_engine.connect()
        logging.info("MSSQL connection successful")

        df = pd.read_sql("select * from ibm_employee", msql_conn)
        logging.info("read data from MYSQL database")
        df.to_sql("capgemini4", ssms_conn, if_exists="append", index=False)
        logging.info("Successfully transferred the data into MSSQL")

        msql_conn.close()
        logging.info("MYSQL connection closed")
        ssms_conn.close()
        logging.info("MSSQL connection closed")
        return True

    except Exception as e:
        logging.error(f"Failed to send an email:{e}")
        return False

def send_email(status):
    with open(r'configfiles\gmail.json') as email_fp:
        email_data = json.load(email_fp)

    sender_email = "abhishekpallip41@gmail.com"
    logging.info("reading Sender's email")
    receiver_email = "abhishekpalli143@gmail.com"
    logging.info("reading Receiver's email")
    password = f"{email_data['apppass']}"

    subject = "ETL Pipeline Status"

    mysql_db = "mysqldb.ibm_employee"
    sql_server = "python.capgemini4"

    if status:
        content = f"Hi Team,\n\n Hope you are doing great!!\n\n Our {mysql_db} from MySQL Database is successfully transferred to {sql_server}. Thanks,\nAbhishek"
        logging.info("success email content has been created")
    else:
        content = f"Hi Team,\n\n Hope you are doing great!!\n\n Our {mysql_db} from MySQL Database has been failed to transfer to {sql_server}. Thanks,\nAbhishek"
        logging.info("failure email content has been created")

    body = MIMEMultipart()
    body['From'] = sender_email
    body['To'] = receiver_email
    body['Subject'] = subject

    body.attach(MIMEText(content,'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com',587) as server:
            server.starttls()
            server.login(sender_email,password)
            server.sendmail(sender_email,receiver_email,body.as_string())
            logging.info("Email has been sent successfully")
    except Exception as e:
        logging.error(f"Failed to send an email:{e}")


# migrate_data()
def main():
    status = migrate_data()
    send_email(status)

if __name__ == '__main__':
     main()