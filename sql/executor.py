from managers.relation import Record
from query_engine.relation_scanner import RelationScanner
from query_engine.operators import SelectOperator, ProjectOperator, Condition
import csv
import re

class SQLExecutor:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def execute_command(self, cmd: dict):
        action = cmd["action"]
        try:
            if action == "CREATE_TABLE": return self._create(cmd)
            elif action == "DROP_TABLE": return self._drop(cmd)
            elif action == "INSERT": return self._insert(cmd)
            elif action == "IMPORT_CSV": return self._import(cmd)
            elif action == "SELECT": return self._select(cmd)
            elif action == "DELETE": return self._delete(cmd)
            elif action == "UPDATE": return self._update(cmd)
        except Exception as e:
            return f"Erreur d'exécution ({action}): {e}"
        return "Commande non gérée."

    # --- Helpers ---
    def _get_rel(self, name):
        rel = self.db_manager.GetTable(name)
        if not rel: raise ValueError(f"Table {name} introuvable.")
        return rel

    def _convert_val(self, val, type_col):
        """Convertit une string vers INT/FLOAT selon le schéma."""
        type_col = type_col.upper()
        if type_col == "INT": return int(float(val)) # float() gère "3.0"
        if type_col == "FLOAT": return float(val)
        return str(val)

    def _build_iterator(self, rel, where_clause):
        """Construit la chaîne Scanner -> SelectOperator (si WHERE)."""
        iterator = RelationScanner(rel)
        
        if where_clause:
            # Parsing simple de condition : col op val
            # Supporte : >, <, =, >=, <=, <> [cite: 331]
            match = re.split(r'(>=|<=|<>|=|>|<)', where_clause)
            if len(match) >= 3:
                col_name = match[0].strip()
                op = match[1].strip()
                val_str = match[2].strip().strip('"').strip("'")
                
                # Trouver index et type colonne
                col_idx = -1
                col_type = "STRING"
                for i, (cname, ctype) in enumerate(rel.schema):
                    if cname == col_name:
                        col_idx = i
                        col_type = ctype.split('(')[0]
                        break
                
                if col_idx != -1:
                    cond = Condition(col_idx, op, val_str, col_type)
                    iterator = SelectOperator(iterator, [cond])
        return iterator

    # --- Actions ---
    def _create(self, cmd):
        self.db_manager.CreateTable(cmd["table"], cmd["columns"])
        return f"Table {cmd['table']} créée."

    def _drop(self, cmd):
        self.db_manager.RemoveTable(cmd["table"])
        return f"Table {cmd['table']} supprimée."

    def _insert(self, cmd):
        rel = self._get_rel(cmd["table"])
        vals = [self._convert_val(v, rel.schema[i][1]) for i, v in enumerate(cmd["values"])]
        rel.InsertRecord(Record(vals))
        return "Record inséré."

    def _import(self, cmd):
        rel = self._get_rel(cmd["table"])
        count = 0
        with open(cmd["file"], newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                vals = [self._convert_val(row[i], rel.schema[i][1]) for i in range(len(rel.schema))]
                rel.InsertRecord(Record(vals))
                count += 1
        return f"Succès : {count} records importés."

    def _select(self, cmd):
        rel = self._get_rel(cmd["table"])
        iterator = self._build_iterator(rel, cmd["where"])

        # Projection
        if cmd["columns"] != ["*"]:
            indices = []
            for col in cmd["columns"]:
                for i, (cname, _) in enumerate(rel.schema):
                    if cname == col: indices.append(i)
            iterator = ProjectOperator(iterator, indices)

        res = []
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            # Format: val1 ; val2 . [cite: 315-316]
            res.append(" ; ".join(str(v) for v in rec.values) + " .")
            count += 1
        
        return "\n".join(res) + f"\nTotal selected records={count}"

    def _delete(self, cmd):
        rel = self._get_rel(cmd["table"])
        iterator = self._build_iterator(rel, cmd["where"])
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            # IMPORTANT: Le record doit avoir son RID attaché par le Scanner
            rel.DeleteRecord(rec)
            count += 1
        return f"Total deleted records={count}"

    def _update(self, cmd):
        rel = self._get_rel(cmd["table"])
        iterator = self._build_iterator(rel, cmd["where"])
        
        # Préparer les modifications
        updates = {} # {col_index: new_value}
        for col_name, val_str in cmd["set"]:
            for i, (cname, ctype) in enumerate(rel.schema):
                if cname == col_name:
                    updates[i] = self._convert_val(val_str, ctype)
        
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            
            # Copie des valeurs existantes
            new_vals = list(rec.values)
            for idx, val in updates.items():
                new_vals[idx] = val
                
            rel.UpdateRecord(rec, new_vals)
            count += 1
        return f"Total updated records={count}"