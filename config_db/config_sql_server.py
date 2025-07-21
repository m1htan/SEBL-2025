from configparser import ConfigParser
import os
import pyodbc

def config_sql_server(filename='config.ini', section='sqlserver'):
    parser = ConfigParser()
    parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), filename))

    if parser.has_section(section):
        params = parser.items(section)
        config = {param[0]: param[1] for param in params}
    else:
        raise Exception(f'Section {section} not found in {filename}')

    connection_string = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Trusted_Connection=no;"
    )

    conn = pyodbc.connect(connection_string)
    conn.autocommit = False
    return conn

def config_sql_server_ods(filename='config.ini', section='sqlserver_ods'):
    parser = ConfigParser()
    parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), filename))

    if parser.has_section(section):
        params = parser.items(section)
        config = {param[0]: param[1] for param in params}
    else:
        raise Exception(f'Section {section} not found in {filename}')

    connection_string = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Trusted_Connection=no;"
    )

    conn = pyodbc.connect(connection_string)
    conn.autocommit = False
    return conn

def config_sql_server_dtm(filename='config.ini', section='sqlserver_dtm'):
    parser = ConfigParser()
    parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), filename))

    if parser.has_section(section):
        params = parser.items(section)
        config = {param[0]: param[1] for param in params}
    else:
        raise Exception(f'Section {section} not found in {filename}')

    connection_string = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Trusted_Connection=no;"
    )

    conn = pyodbc.connect(connection_string)
    conn.autocommit = False
    return conn