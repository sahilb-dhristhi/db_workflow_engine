from mysql.connector import connect, Error

def run(step, connections, context):
    connection_id = step.get("connection_id")
    query = step.get("query")

    config = connections.get(connection_id)
    if not config or not query:
        return "Missing connection or query."

    try:
        conn = connect(**config)
        cursor = conn.cursor()
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            data = cursor.fetchall()
            context["last_result"] = data
            context["last_columns"] = [desc[0] for desc in cursor.description]
            return f"Fetched {len(data)} rows."
        else:
            conn.commit()
            return f"Query executed. Rows affected: {cursor.rowcount}"
    except Error as e:
        return f"MySQL Error: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()
