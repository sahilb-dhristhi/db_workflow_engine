from mysql.connector import connect, Error

def run(step, connections, context):
    connection_id = step.get("connection_id")
    target_table = step.get("target_table")
    rows = context.get("last_result")
    columns = context.get("last_columns")

    config = connections.get(connection_id)
    if not config or not rows or not target_table or not columns:
        return "Missing data to insert."

    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO {target_table} VALUES ({placeholders})"

    try:
        conn = connect(**config)
        cursor = conn.cursor()
        cursor.executemany(insert_query, rows)
        conn.commit()
        return f"Inserted {cursor.rowcount} row(s) into {target_table}."
    except Error as e:
        return f"MySQL Error: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()
