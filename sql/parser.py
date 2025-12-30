def parse(query: str) -> dict:
    query = query.strip().rstrip(";")
    tokens = query.split()
    action = tokens[0].upper()

    if action == "CREATE" and tokens[1].upper() == "DATABASE":
        return {"action": "CREATE_DATABASE", "name": tokens[2]}

    if action == "CREATE" and tokens[1].upper() == "TABLE":
        table = tokens[2]
        cols = query[query.find("(")+1:query.find(")")].split(",")
        cols = [tuple(c.strip().split()) for c in cols]
        return {"action": "CREATE_TABLE", "table": table, "columns": cols}

    if action == "DROP" and tokens[1].upper() == "TABLE":
        return {"action": "DROP_TABLE", "table": tokens[2]}

    if action == "INSERT":
        table = tokens[2]
        values = query[query.find("(")+1:query.find(")")].split(",")
        values = [v.strip().strip('"').strip("'") for v in values]
        return {"action": "INSERT", "table": table, "values": values}

    if action == "SELECT":
        cols = tokens[1].split(",")
        table = tokens[tokens.index("FROM")+1]
        return {"action": "SELECT", "columns": cols, "table": table}

    if action == "IMPORT":
        filename = tokens[2].strip("'").strip('"')
        table = tokens[-1]
        return {"action": "IMPORT_CSV", "file": filename, "table": table}

    raise ValueError("Requête non supportée : " + query)
