FROM dwp-python-reconciliation-integration:latest

COPY ./features .
COPY ../shared_functions.py .

ENTRYPOINT ["behave"]
CMD ["--capture"]

