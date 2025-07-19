import psycopg2

def connect():
    conn = psycopg2.connect(
    dbname="kde",
    user="postgres",
    password="73192834",
    host="localhost",
    port="5432"
    )
    return conn

if __name__ == '__main__':  
    main() 
