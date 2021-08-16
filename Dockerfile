FROM python:3.9
WORKDIR /opt/restsql/
COPY requirements.txt /opt/restsql/
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org --no-cache-dir -r requirements.txt
COPY . /opt/restsql/
EXPOSE 8000
ENV PYTHONPATH="/opt/restsql" CONF_RESTSQL_PATH="/opt/restsql/RestSQLServer/RestSQLServer/config/restsql.yaml" CONF_LOGGER_PATH="/opt/restsql/RestSQLServer/RestSQLServer/config/restsql.log" CONF_RESTSQLDIR_PATH="/opt/restsql/"
ENTRYPOINT [ "python" ,"RestSQLServer/manage.py","runserver"]
CMD [ "0.0.0.0:8000" ]
