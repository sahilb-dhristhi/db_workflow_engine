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
        context["last_result"] = cursor.fetchall()
        context["last_columns"] = [desc[0] for desc in cursor.description]
        return f"Input fetched: {len(context['last_result'])} rows."
    except Error as e:
        return f"MySQL Error: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()
