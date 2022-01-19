from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


def make_database():
    engine    = create_engine('mysql+mysqldb://airflow:password@172.17.0.1/imDB')
    if not database_exists(engine.url):
        create_database(engine.url)
#validate connection
    valid=True
    try:
        conn = engine.connect()
        valid=True
        print("connected")
    except:
        print("can't connect")
        valid=False
    
    if valid==True:

        trans=conn.begin()

        create_table = "CREATE TABLE IF NOT EXISTS imdb_table (country varchar(20), domains varchar(45), name text, state_province varchar(20),web_pages text,alpha_two_code varchar(20));"   
        conn.execute(create_table)
        trans.commit()
        conn.close()

if __name__ == "__main__":
	make_database()
