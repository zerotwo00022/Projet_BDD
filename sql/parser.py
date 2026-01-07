import re

def parse(query: str) -> dict:
    raw_query = query.strip().rstrip(";")
    tokens = raw_query.split()
    
    if not tokens:
        return {}

    action = tokens[0].upper()

    # === CREATE TABLE ===
    # Format: CREATE TABLE Nom 
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

    # === INSERT ===
    if action == "INSERT":
        # Format attendu: INSERT INTO Table VALUES (val1, val2, ...)
        try:
            # On sépare sur le mot clé "VALUES" 
            parts = re.split(r'VALUES', raw_query, flags=re.IGNORECASE)
            if len(parts) != 2:
                raise ValueError("Syntaxe INSERT invalide (manque VALUES)")
            
            #Récupération du nom de la table (partie gauche)
            
            header = parts[0].replace("INSERT", "").replace("INTO", "").strip()
            table_name = header.split()[0] # Le premier mot est la table
            
            # Récupération des valeurs (partie droite)
            values_part = parts[1].strip()
            
            # On vérifie les parenthèses
            if not (values_part.startswith("(") and values_part.endswith(")")):
                raise ValueError("Les valeurs doivent être entre parenthèses: VALUES (...)")
            
            # On retire les parenthèses
            content = values_part[1:-1]
            
            # Découpage intelligent des valeurs par la virgule
        
            raw_values = content.split(",")
            
            clean_values = []
            for v in raw_values:
                v = v.strip()
                # On enlève les guillemets si présents
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                clean_values.append(v)
            
            return {
                "action": "INSERT",
                "table": table_name,
                "values": clean_values
            }
            
        except Exception as e:
            raise ValueError(f"Erreur de parsing INSERT : {str(e)}")
    if action == "APPEND":
        # Syntaxe attendue : APPEND INTO Table ALLRECORDS (Fichier.csv)
        try:
            
            clean_query = raw_query.replace("(", " ").replace(")", " ")
            tokens = clean_query.split()
            
            
            # On cherche l'index de "INTO" pour trouver la table juste après
            if "INTO" in [t.upper() for t in tokens]:
                into_idx = [t.upper() for t in tokens].index("INTO")
                table_name = tokens[into_idx + 1]
            else:
                
                table_name = tokens[1] 

            # Le fichier est censé être le dernier élément ou après ALLRECORDS
            filename = tokens[-1]
            
            return {
                "action": "APPEND",
                "table": table_name,
                "file": filename  
            }
            
        except Exception:
            raise ValueError("Syntaxe APPEND incorrecte. Attendu: APPEND INTO Table ALLRECORDS (Fichier.csv)")

    # === SELECT ===
    # Format: SELECT col1,col2 FROM Table [WHERE ...]
    if action == "SELECT":
        where_clause = None
        main_part = raw_query
        # Séparation du WHERE
        if "WHERE" in raw_query.upper():
            parts = re.split(r'WHERE', raw_query, flags=re.IGNORECASE)
            main_part = parts[0]
            where_clause = parts[1].strip()
            
        select_tokens = main_part.replace(",", " ").split()
        try:
            from_idx = [i for i, t in enumerate(select_tokens) if t.upper() == "FROM"][0]
            cols = select_tokens[1:from_idx]
            table = select_tokens[from_idx+1]
            
            alias = None
            # S'il reste un truc après le nom de la table, c'est l'alias
            if len(select_tokens) > from_idx + 2:
                alias = select_tokens[from_idx + 2]

            return {
                "action": "SELECT", 
                "table": table, 
                "alias": alias,  
                "columns": cols, 
                "where": where_clause
            }
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
        # Gestion de: DELETE FROM Table [Alias] OU DELETE Table [Alias]
        start_idx = 2 if tks[1].upper() == "FROM" else 1
        table = tks[start_idx]
        
        alias = None
        if len(tks) > start_idx + 1:
            alias = tks[start_idx + 1]

        return {"action": "DELETE", "table": table, "alias": alias, "where": where_clause}
    
    # "DESCRIBE TABLES" ou "DESCRIBE TABLE Nom"
    if action == "DESCRIBE":
        if len(tokens) > 1 and tokens[1].upper() == "TABLES":
            return {"action": "DESCRIBE_TABLES"}
        elif len(tokens) > 2 and tokens[1].upper() == "TABLE":
            return {"action": "DESCRIBE_TABLE", "table": tokens[2]}

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
        
        # Extraction Table + Alias 
        header_tks = parts_set[0].split()
        table = header_tks[1]
        alias = header_tks[2] if len(header_tks) > 2 else None
        
        assigns = []
        for pair in parts_set[1].split(","):
            if "=" not in pair: continue
            col, val = pair.split("=")
            assigns.append((col.strip(), val.strip().strip('"').strip("'")))
            
        return {"action": "UPDATE", "table": table, "alias": alias, "set": assigns, "where": where_clause}
    if action == "DROP" and len(tokens) > 1 and tokens[1].upper() == "TABLES":
         return {"action": "DROP_TABLES"}

    raise ValueError("Commande inconnue")