from datetime import datetime, timedelta
import os,csv
import json
import numpy as np
from make_db import *

def load_data():
    engine    = create_engine('mysql+mysqldb://airflow:password@172.17.0.1/imDB')

    conn = engine.connect()
    trans=conn.begin()
	with open('/root/airflow/dags/imdb_data.csv', 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			if skipHeader:
				skipHeader = False
				continue
			conn.execute(
				 """INSERT INTO imdb_table 
                                        (country,domains,name, state_province, web_pages,alpha_two_code)
                                        VALUES
                                        (%s,%s,%s,%s,%s,%s)""",
					row,
		)
	trans.commit()
	print("data inserted")
	conn.close()
	
	

if __name__ == "__main__":
	load_data()
