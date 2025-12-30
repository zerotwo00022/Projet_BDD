import os
import pickle
import csv
from managers.buffer_manager import BufferManager
from managers.disk_manager import DiskManager

# Variable globale : base courante
CURRENT_DB = None

def table_file(dbpath, table):
    """Retourne le chemin complet du fichier binaire d'une table"""
    global CURRENT_DB
    if CURRENT_DB is None:
        raise Exception("Aucune base sélectionnée. Faites CREATE DATABASE ou USE <nom>.")
    return os.path.join(dbpath, CURRENT_DB, f"{table}.bin")

def execute(cmd: dict, dbpath: str):
    global CURRENT_DB
    action = cmd["action"]

    # === CREATE DATABASE ===
    if action == "CREATE_DATABASE":
        db_name = cmd["name"]
        path = os.path.join(dbpath, db_name)
        os.makedirs(path, exist_ok=True)
        # Crée un fichier principal binaire pour cette base
        db_file = os.path.join(path, f"{db_name}.bin")
        open(db_file, "wb").close()
        CURRENT_DB = db_name
        return f"Base {db_name} créée et sélectionnée."

    # === USE DATABASE === (facultatif mais pratique)
    elif action == "USE_DATABASE":
        db_name = cmd["name"]
        path = os.path.join(dbpath, db_name)
        if not os.path.exists(path):
            return f"Base {db_name} introuvable."
        CURRENT_DB = db_name
        return f"Base {db_name} sélectionnée."

    # Vérifie qu'une base est active
    if CURRENT_DB is None:
        raise Exception("Aucune base active. Faites CREATE DATABASE ou USE.")

    # === CREATE TABLE ===
    elif action == "CREATE_TABLE":
        schema = dict(cmd["columns"])
        path = table_file(dbpath, cmd["table"])
        with open(path, "wb") as f:
            pickle.dump({"schema": schema, "rows": []}, f)
        return f"Table {cmd['table']} créée dans la base {CURRENT_DB}."

    # === DROP TABLE ===
    elif action == "DROP_TABLE":
        path = table_file(dbpath, cmd["table"])
        if os.path.exists(path):
            os.remove(path)
            return f"Table {cmd['table']} supprimée."
        return f"Table {cmd['table']} introuvable."

    # === INSERT ===
    elif action == "INSERT":
        path = table_file(dbpath, cmd["table"])
        if not os.path.exists(path):
            return f"Table {cmd['table']} inexistante."
        with open(path, "rb") as f:
            table = pickle.load(f)
        table["rows"].append(cmd["values"])
        with open(path, "wb") as f:
            pickle.dump(table, f)
        return f"Ligne insérée dans {cmd['table']}."

    # === SELECT ===
    elif action == "SELECT":
        path = table_file(dbpath, cmd["table"])
        if not os.path.exists(path):
            return f"Table {cmd['table']} inexistante."
        with open(path, "rb") as f:
            table = pickle.load(f)
        rows = table["rows"]
        if cmd["columns"] == ["*"]:
            return rows
        else:
            schema_keys = list(table["schema"].keys())
            col_indices = [schema_keys.index(c) for c in cmd["columns"]]
            return [[row[i] for i in col_indices] for row in rows]

    # === IMPORT CSV ===
    elif action == "IMPORT_CSV":
        path = table_file(dbpath, cmd["table"])
        if not os.path.exists(path):
            return f"Table {cmd['table']} inexistante."
        with open(path, "rb") as f:
            table = pickle.load(f)
        with open(cmd["file"], newline='', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                table["rows"].append(row)
        with open(path, "wb") as f:
            pickle.dump(table, f)
        return f"CSV importé dans {cmd['table']}."

    else:
        return f"Action {action} non encore implémentée."
