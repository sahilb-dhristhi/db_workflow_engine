import json
import os

# Import activities
from workflow_engine import db_connection, db_input, db_output, db_query, db_full_load

# Map action names to activity run functions
activity_map = {
    "db_connection": db_connection.run,
    "db_input": db_input.run,
    "db_output": db_output.run,
    "db_query": db_query.run,
    "db_full_load": db_full_load.run
}

def load_connections(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

def load_workflow(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

def execute_workflow(workflow_file, connections_file):
    # Load workflow and connections
    workflow = load_workflow(workflow_file)
    connections = load_connections(connections_file)

    results = []
    context = {}  # To share state between steps

    for i, step in enumerate(workflow, 1):
        action = step.get("action")
        if not action:
            results.append(f"Step {i}: Missing 'action' key.")
            continue

        if action not in activity_map:
            results.append(f"Step {i}: Unsupported action '{action}'.")
            continue

        try:
            result = activity_map[action](step, connections, context)
            results.append(f"Step {i} ({action}): {result}")
        except Exception as e:
            results.append(f"Step {i} ({action}): Error - {str(e)}")

    return "\n".join(results)

# Entry point
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(__file__)
    workflow_path = os.path.join(BASE_DIR, "Schema", "workflow.json")
    connections_path = os.path.join(BASE_DIR, "Schema", "connections.json")

    output = execute_workflow(workflow_path, connections_path)
    print(output)
