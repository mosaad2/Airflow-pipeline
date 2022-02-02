from make_db import *

def show_data():
    engine    = create_engine('mysql+mysqldb://airflow:password@172.17.0.1/imDB')

    conn = engine.connect()
    trans=conn.begin()
    query = "select * from imdb_table limit 10"
    ans=conn.execute(query)
    records = ans.fetchall()
    print("Total rows are:  ", len(records))
    print("\nPrinting each row")
    for row in records:
        print("country = ", row[0], )
        print("domains = ", row[1])
        print("name = ", row[2])
        print("state_province = ", row[3])
        print("web_pages  = ", row[4])
        print("alpha_two_code  = ", row[5], "\n")
    
    trans.commit()
    conn.close()

if __name__ == "__main__":
        show_data()