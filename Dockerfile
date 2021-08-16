FROM python:3.9
ADD . /code
WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 8000
ENV CONF_RESTSQL_PATH="/code/RestSQLServer/RestSQLServer/config/restsql.yaml" CONF_LOGGER_PATH="/code/RestSQLServer/RestSQLServer/config/restsql.log" CONF_RESTSQLDIR_PATH="/code"
RUN ["python","RestSQLServer/manage.py","runserver","0.0.0.0:8000"]
