from flask import Flask, request, Response
from google.cloud import storage
from google.cloud import pubsub_v1

from datetime import datetime
from google.cloud.sql.connector import Connector
import pymysql
import logging
import google.cloud.logging
import sqlalchemy

# python http-client.py -d "127.0.0.1" -p "8080" -b "none" -w "files_get" -v -n 15 -i 10000
# python3 http-client.py -d "127.0.0.1" -p "8080" -b "none" -w "files_get" -v -n 15 -i 10000
# gcloud sql connect database-1 --user=root --quiet

app = Flask(__name__)

INSTANCE_CONNECTION_NAME = "jacks-project-398813:us-east1:database-1"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "db1"

success_template = sqlalchemy.text(
    "INSERT INTO requests (gender, age, income, time, ip_address, file_name, country, is_banned) VALUES (:gender, :age, :income, :time, :ip_address, :file_name, :country, :is_banned)",
)

failure_template = sqlalchemy.text(
    "INSERT INTO failed_requests (time, file_name, error_code) VALUES (:time, :file_name, :error_code)",
)


banned = ['north korea', 'iran', 'cuba', 'myanmar', 'iraq', 'libya', 'sudan', 'zimbabwe', 'syria']

connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

@app.route('/files_get/<filename>', methods=['GET','POST','PUT', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
def files_get(filename):

    client = google.cloud.logging.Client()
    client.setup_logging()

    # Access the request method
    method = request.method

    country = None
    # Getting country
    if 'X-country' in request.headers:
        country = request.headers.get("X-country").lower().strip()
        print("Country: ", country)
    else:
        print("No Country")

    # Checking country and handling file retrieval
    if method != 'GET':
        print("Method not implemented: ", method)
        
        ## NEW CODE ##
        # Not Implemented Failure
        with pool.connect() as db_conn:
            db_conn.execute(failure_template, parameters={"time": datetime.now(), "file_name": filename, "error_code": "501"})
            db_conn.commit()
        ## NEW CODE ##
        
        print("Country: ", country)
        return 'Not Implemented', 501
    else:
        if country not in banned:
            print("Permission Granted")

            # Getting file contents
            client = storage.Client()
            bucket = client.bucket('bu-ds561-jawicamp')
            blob = bucket.blob(filename)

            try:
                # Returning file contents and OK status
                content = blob.download_as_text()
                response = Response(content, status=200, headers={'Content-Type': 'text/html'})

                ## NEW CODE ##
                # Successful Request
                with pool.connect() as db_conn:
                    db_conn.execute(success_template, parameters={
                            "gender": request.headers.get("X-gender"), 
                            "age": request.headers.get("X-age"), 
                            "income": request.headers.get("X-income"),
                            "time": request.headers.get("X-time"),
                            "ip_address": request.headers.get("X-Client-IP"),
                            "file_name": filename,
                            "country": request.headers.get("X-country").lower().strip(),
                            "is_banned": False
                            }
                        )
                    db_conn.commit()
                    
                    results = db_conn.execute(sqlalchemy.text("SELECT * FROM requests")).fetchall()
                    for row in results:
                        print(row)
                ## NEW CODE ##

                return response

            except Exception as e:
                print("File not found: ", filename)

                ## NEW CODE ##
                # File Not Found Failure
                with pool.connect() as db_conn:
                    db_conn.execute(failure_template, parameters={"time": datetime.now(), "file_name": filename, "error_code": "404"})
                    db_conn.commit()
                    
                    results = db_conn.execute(sqlalchemy.text("SELECT * FROM failed_requests")).fetchall()
                    for row in results:
                        print(row)
                ## NEW CODE ##

                return str(e), 404
        else:
            print("Permission Denied")

            ## NEW CODE ##
            # Permission Denied Failure
            with pool.connect() as db_conn:
                # adding to requests table
                db_conn.execute(success_template, parameters={
                    "gender": request.headers.get("X-gender"), 
                    "age": request.headers.get("X-age"), 
                    "income": request.headers.get("X-income"),
                    "time": request.headers.get("X-time"),
                    "ip_address": request.headers.get("X-Client-IP"),
                    "file_name": filename,
                    "country": request.headers.get("X-country").lower().strip(),
                    "is_banned": True
                    }
                )
                db_conn.commit()
                # failed requests table
                db_conn.execute(failure_template, parameters={"time": datetime.now(), "file_name": filename, "error_code": "400"})
                db_conn.commit()
            ## NEW CODE ##

            # Publishing to Pub/Sub
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path('jacks-project-398813', 'banned_countries')

            # Bytestring data
            data_str = f"{country}"
            data = data_str.encode("utf-8")

            # Try to publish
            try:
                future = publisher.publish(topic_path, data)
                future.result()  # Wait for the publish operation to complete
                print("Published to Pub/Sub successfully")
            except Exception as e:
                print("Error publishing to Pub/Sub:", str(e))
                return "Publish Denied", 400
            return "Permission Denied", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

# ---------------------------------------------- #

# CREATE TABLE requests (
#     request_id SERIAL PRIMARY KEY,
#     gender VARCHAR(255),
#     age VARCHAR(255),
#     income VARCHAR(255),
#     time TIMESTAMP,
#     ip_address VARCHAR(255),
#     file_name VARCHAR(255),
#     country VARCHAR(255),
#     is_banned BOOLEAN
# );

# CREATE TABLE failed_requests (
#     failed_request_id SERIAL PRIMARY KEY,
#     time TIMESTAMP,
#     file_name VARCHAR(255),
#     error_code VARCHAR(255)
# );