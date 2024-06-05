pip install PyYAML
import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def read_db_creds():
        return yaml.load(db_creds.yml)
    #Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.
    def init_db_engine(read_db_creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        
    engine.connect()



#engine.execution_options(isolation_level='AUTOCOMMIT').connect()

list_of_tables = engine.list_db_tables
print(list_of_tables)


#Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.