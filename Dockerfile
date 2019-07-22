FROM python:3.7
COPY . /app
WORKDIR /app
RUN pip install -e .
RUN cd health_endpoint_example/health_report/
RUN python -m http.server 8080
RUN cd /app
CMD ["python", "health_endpoint_example/main.py", "10"]