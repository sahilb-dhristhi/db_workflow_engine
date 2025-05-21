from mysql.connector import connect, Error

def run(step, connections, context):
    connection_id = step.get("connection_id")
    source_table = step.get("source_table")
    target_table = step.get("target_table")

    config = connections.get(connection_id)
    if not config or not source_table or not target_table:
        return "Missing connection_id, source_table, or target_table."

    try:
        conn = connect(**config)
        cursor = conn.cursor()

        # Fetch from source table
        cursor.execute(f"SELECT * FROM {source_table}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not rows:
            return f"No rows found in source table '{source_table}'."

        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {target_table} VALUES ({placeholders})"

        # Insert into target table
        cursor.executemany(insert_query, rows)
        conn.commit()

        return f"Full load successful: {cursor.rowcount} row(s) copied from '{source_table}' to '{target_table}'."
    except Error as e:
        return f"MySQL Error: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()
