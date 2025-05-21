import json

def load_connections(file_path="Sahilb/workflow_engine/Schema/connections.json"):
    with open(file_path, "r") as f:
        return json.load(f)
