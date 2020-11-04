FROM python:3.8

COPY ./populate.py .
RUN pip install mysql-connector-python
ENTRYPOINT ["python", "./populate.py"]
