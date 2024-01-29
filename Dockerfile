FROM python:3.9

#RUN apt-get install wget && pip install pandas sqlalchemy psycopg2
RUN pip install requests 
# pyarrow pandas sqlalchemy psycopg2

WORKDIR /app

#COPY ingest_data.py ingest_data.py
COPY etl_process.py etl_process.py
COPY file_downloader.py file_downloader.py

# ENTRYPOINT ["python", "ingest_data.py"]

ENTRYPOINT ["python", "etl_process.py"]