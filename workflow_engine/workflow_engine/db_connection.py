# activities/db_connection.py

def run(step, connections, context):
    connection_id = step.get("connection_id")
    if connection_id in connections:
        return f"Connection '{connection_id}' validated."
    else:
        return f"Connection ID '{connection_id}' not found."
