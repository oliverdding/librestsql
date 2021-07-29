import psycopg2


def init_db(database):
    conn = psycopg2.connect(database="testdjango", user="postgres", password="zhang23785491", host="127.0.0.1",
                            port="5432")
    return conn


def exit_db(conn):
    if conn:
        conn.commit()
        conn.close()


def exec_sql(sql, conn):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()
