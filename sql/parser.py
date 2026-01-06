import re

def parse(query: str) -> dict:
    raw_query = query.strip().rstrip(";")
    tokens = raw_query.split()
    
    if not tokens:
        return {}

    action = tokens[0].upper()

    # === CREATE TABLE ===
    # Format: CREATE TABLE Nom (Col:Type, ...)
    if action == "CREATE" and len(tokens) > 2 and tokens[1].upper() == "TABLE":
        table_name = tokens[2]
        start = raw_query.find("(")
        end = raw_query.rfind(")")
        if start == -1 or end == -1: raise ValueError("Parenthèses manquantes")
        
        content = raw_query[start+1:end]
        cols_str = content.split(",")
        columns = []
        for c in cols_str:
            parts = c.strip().split(":")
            if len(parts) != 2: raise ValueError(f"Colonne invalide: {c}")
            columns.append((parts[0].strip(), parts[1].strip()))
        return {"action": "CREATE_TABLE", "table": table_name, "columns": columns}

    # === DROP TABLE ===
    if action == "DROP" and len(tokens) > 2 and tokens[1].upper() == "TABLE":
        return {"action": "DROP_TABLE", "table": tokens[2]}

    # === INSERT INTO ===
    if action == "INSERT" and tokens[1].upper() == "INTO":
        table_name = tokens[2]
        start = raw_query.find("(")
        end = raw_query.rfind(")")
        values = [v.strip().strip('"').strip("'") for v in raw_query[start+1:end].split(",")]
        return {"action": "INSERT", "table": table_name, "values": values}

    # === APPEND (CSV Import TP7) ===
    # Format: APPEND INTO Nom ALLRECORDS (Fichier.csv)
    if action == "APPEND":
        clean = raw_query.replace("(", " ").replace(")", " ")
        tks = clean.split()
        if len(tks) >= 5 and tks[3].upper() == "ALLRECORDS":
            return {"action": "IMPORT_CSV", "table": tks[2], "file": tks[4]}

    # === SELECT ===
    # Format: SELECT col1,col2 FROM Table [WHERE ...]
    if action == "SELECT":
        where_clause = None
        main_part = raw_query
        if "WHERE" in raw_query.upper():
            parts = re.split(r'WHERE', raw_query, flags=re.IGNORECASE)
            main_part = parts[0]
            where_clause = parts[1].strip()
            
        # On remplace les virgules par espaces pour simplifier le split
        select_tokens = main_part.replace(",", " ").split()
        try:
            from_idx = [i for i, t in enumerate(select_tokens) if t.upper() == "FROM"][0]
            cols = select_tokens[1:from_idx]
            table = select_tokens[from_idx+1]
            return {"action": "SELECT", "table": table, "columns": cols, "where": where_clause}
        except IndexError: raise ValueError("Syntaxe SELECT incorrecte")

    # === DELETE ===
    # Format: DELETE FROM Table [WHERE ...]
    if action == "DELETE":
        where_clause = None
        main_part = raw_query
        if "WHERE" in raw_query.upper():
            parts = re.split(r'WHERE', raw_query, flags=re.IGNORECASE)
            main_part = parts[0]
            where_clause = parts[1].strip()
        
        tks = main_part.split()
        # Gère "DELETE Table" ou "DELETE FROM Table"
        table = tks[2] if tks[1].upper() == "FROM" else tks[1]
        return {"action": "DELETE", "table": table, "where": where_clause}
    
    # === DESCRIBE (TP6) ===
    # Formats acceptés : "DESCRIBE TABLES" ou "DESCRIBE TABLE Nom"
    if action == "DESCRIBE":
        if len(tokens) > 1 and tokens[1].upper() == "TABLES":
            return {"action": "DESCRIBE_TABLES"}
        elif len(tokens) > 2 and tokens[1].upper() == "TABLE":
            return {"action": "DESCRIBE_TABLE", "table": tokens[2]}

    # === UPDATE ===
    # Format: UPDATE Table SET col=val [WHERE ...]
    if action == "UPDATE":
        where_clause = None
        rest = raw_query
        if "WHERE" in raw_query.upper():
            parts = re.split(r'WHERE', raw_query, flags=re.IGNORECASE)
            rest = parts[0]
            where_clause = parts[1].strip()
            
        if "SET" not in rest.upper(): raise ValueError("UPDATE nécessite SET")
        parts_set = re.split(r'SET', rest, flags=re.IGNORECASE)
        table = parts_set[0].split()[1]
        
        assigns = []
        for pair in parts_set[1].split(","):
            col, val = pair.split("=")
            assigns.append((col.strip(), val.strip().strip('"').strip("'")))
            
        return {"action": "UPDATE", "table": table, "set": assigns, "where": where_clause}
    # === DROP TABLES (TP6) ===
    if action == "DROP" and len(tokens) > 1 and tokens[1].upper() == "TABLES":
         return {"action": "DROP_TABLES"}

    raise ValueError("Commande inconnue")