from trino.dbapi import connect


def get_trino_client():

    try:
        conn = connect(
            host="trino",
            port=8080,
            user="admin",
            catalog="iceberg",
            schema="default",
        )
        return conn
    except Exception as e:
        raise Exception(
            f"Failed to connect to trino because {e}")


def check_if_uploaded(trino_conn, fileName):

    sql_statement = f"SELECT count(*) FROM iceberg.default.uploaded_file WHERE fileName = '{fileName}'"
    cur = trino_conn.cursor()
    cur.execute(sql_statement)
    rows = cur.fetchall()[0][0]

    if rows == 0:
        print("File not uploaded", fileName)
        return False
    return True


def log_status(trino_conn, fileName, download_url, status, timestamp, error):

    sql_statement = f"INSERT INTO iceberg.default.uploaded_file VALUES ('{fileName}, {download_url}, {status}, {timestamp}, {error}')"
    cur = trino_conn.cursor()
    cur.execute(sql_statement)
    trino_conn.commit()

# def log_failed(trino_conn, fileName):
