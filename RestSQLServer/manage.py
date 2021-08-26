#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

RESTSQL_ROOT_PATH = os.getenv("RESTSQL_ROOT_PATH", "/opt/librestsql")  # project's root path
CONF_RESTSQL_PATH = os.getenv("CONF_RESTSQL_PATH", "/etc/restsql/config.yaml")  # config's path
CONF_LOGGER_PATH = os.getenv("CONF_LOGGER_PATH", "/tmp/restsql.log")  # logging's path

sys.path.extend([sys.path[0]])  # extend current path to pythonpath
from RestSQLServer.config.load import init_config, init_logger


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'RestSQLServer.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    init_config(CONF_RESTSQL_PATH)
    init_logger(CONF_LOGGER_PATH)
    main()
