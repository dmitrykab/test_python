import psycopg2
from  connect_pg_kde import connect
import pandas as pd  

with connect() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * from mytable1;")
        rez = cur.fetchall()

df = pd.DataFrame(rez) 
df.to_csv('mytable.cvs',index=False)
print(df)