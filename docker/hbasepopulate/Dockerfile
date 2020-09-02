FROM dwp-python-preinstall:latest

COPY docker/hbasepopulate/resources/* ./
RUN pip3 install --trusted-host=pypi.python.org --trusted-host=pypi.org --trusted-host=files.pythonhosted.org -r ./requirements.txt
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR} ${INSTALL_DIR}/data
RUN ls -l ${INSTALL_DIR}/data

RUN mkdir -p /opt/reconciliation/data
RUN chmod -R a+rwx /opt/reconciliation/data

USER $SERVICE_USER
ENTRYPOINT ["./populate_hbase.py"]
