import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

os.environ['server'] = '10.20.20.107'
os.environ['db'] = 'STGBI_UAT'
os.environ['port'] = '1433'
os.environ['user'] = 'sa'
os.environ['password'] = 'Vbl@123'

server = os.environ['server']
db = os.environ['db']
port = os.environ['port']
user = os.environ['user']
password = os.environ['password']
mssql_url = os.environ['mssql_url'] = f'jdbc:sqlserver://{server}:{port};database={db}'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = 'pyspark'
current = os.getcwd()
mssql_driver = current + '\driver\jar\mssql-jdbc-12.4.2.jre8.jar'
src_olap = current + '\source\olap'
src_oltp = current + '\source\oltp'
city_path = 'output\cities'
presc_path = 'output\prescriber'
