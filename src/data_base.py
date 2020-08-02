import psycopg2

class Database:
    def __init__(self,db,user,password,host,port):
        self.conn = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port)
        self.cur = self.conn.cursor()

    def query(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

    def insert(self, query, data):
        self.cur.execute(query,data)
        self.conn.commit()
        pass

    def close(self):
        self.cur.close()
        self.conn.close()

