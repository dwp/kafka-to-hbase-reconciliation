FROM python:3.8

COPY ./populate.py .
RUN pip install mysql-connector-python
RUN pip install happybase
ENTRYPOINT ["python", "./populate.py"]
