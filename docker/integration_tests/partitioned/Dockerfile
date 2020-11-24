FROM python:3.8

COPY ./* ./

RUN pip install awscli --trusted-host=pypi.org --trusted-host=pypi.python.org --trusted-host=files.pythonhosted.org
RUN pip install --trusted-host=pypi.python.org --trusted-host=pypi.org --trusted-host=files.pythonhosted.org -r ./requirements.txt
RUN pip install mysql-connector-python
RUN pip install happybase

ENTRYPOINT ["python", "./populate.py"]
