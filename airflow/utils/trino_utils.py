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
    try:
        sql_statement = f"SELECT count(*) FROM iceberg.default.log_table WHERE filename = '{fileName}' AND status = 'failed'"
        cur = trino_conn.cursor()
        cur.execute(sql_statement)
        rows = cur.fetchall()[0][0]

        if rows == 0:
            print("File not uploaded", fileName)
            return False
    except Exception as e:
        print("Error checking if uploaded", e)
        return True


def log_status(trino_conn, fileName, download_url, status, timestamp, error):
    cur = trino_conn.cursor()
    try:

        error_msg = error.replace("'", "''")
        sql_statement = f"""
        INSERT INTO iceberg.default.log_table (filename, download_url, status, updated_date, error)
        SELECT '{fileName}', '{download_url}', '{status}', TIMESTAMP '{timestamp}', '{error_msg}'
        WHERE NOT EXISTS (
            SELECT 1
            FROM iceberg.default.log_table t
            WHERE t.filename = '{fileName}'
        )
        """
        # sql_statement = f"INSERT INTO iceberg.default.log_table VALUES ('{fileName}', '{download_url}', '{status}', TIMESTAMP '{timestamp}', '{error_msg}')"
        print("printing statement", sql_statement)
        cur.execute(sql_statement)
        trino_conn.commit()
    except Exception as e:
        print("Error log status", e)
