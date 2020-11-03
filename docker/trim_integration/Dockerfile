FROM python:3.8

RUN mkdir /tests
WORKDIR tests
COPY ./features .
RUN pip install mysql-connector-python behave
RUN ls
ENTRYPOINT ["behave"]
