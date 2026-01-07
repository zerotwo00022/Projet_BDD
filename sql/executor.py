from managers.relation import Record
from query_engine.relation_scanner import RelationScanner
from query_engine.operators import SelectOperator, ProjectOperator, Condition
import csv
import re
import os 

class SQLExecutor:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def execute_command(self, cmd: dict):
        action = cmd["action"]
        try:
            if action == "CREATE_TABLE": return self._create(cmd)
            elif action == "DROP_TABLE": return self._drop(cmd)
            elif action == "INSERT": return self._insert(cmd)
            
            # --- MODIFICATION ICI (Pour lier le parser APPEND à la méthode _import) ---
            elif action == "APPEND": return self._import(cmd) 
            # -------------------------------------------------------------------------
            
            elif action == "SELECT": return self._select(cmd)
            elif action == "DELETE": return self._delete(cmd)
            elif action == "UPDATE": return self._update(cmd)
            elif action == "DESCRIBE_TABLE": return self._describe_table(cmd)
            elif action == "DESCRIBE_TABLES": return self._describe_tables(cmd)
            elif action == "DROP_TABLES": return self._drop_tables(cmd)
            
        except Exception as e:
            return f"Erreur d'exécution ({action}): {e}"
        return "Commande non gérée."

    
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
    def _resolve_col(self, col_str, rel_name, alias):
        """Nettoie le nom de la colonne en gérant Table.Col ou Alias.Col"""
        if "." in col_str:
            prefix, col_name = col_str.split(".")
            # Si le préfixe est le nom de la table OU l'alias, on le retire
            if prefix == rel_name or prefix == alias:
                return col_name
        return col_str

    def _build_iterator(self, rel, where_clause, alias=None):
        iterator = RelationScanner(rel)
        if not where_clause: return iterator
        
        sub_clauses = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        conditions = []
        
        for clause in sub_clauses:
            clause = clause.strip()
            match = re.split(r'(>=|<=|<>|=|>|<)', clause)
            if len(match) < 3: continue

            left_raw = match[0].strip()
            op = match[1].strip()
            right_raw = match[2].strip()
            
            # 1. Est-ce que la gauche est une colonne ?
            col_left_name = self._resolve_col(left_raw, rel.name, alias)
            left_idx = -1
            for i, (cname, ctype) in enumerate(rel.schema):
                if cname == col_left_name:
                    left_idx = i
                    left_type = ctype.split('(')[0] # INT, FLOAT, ...
                    break
            
            # 2. Est-ce que la droite est une colonne ?
            col_right_name = self._resolve_col(right_raw, rel.name, alias)
            right_idx = -1
            for i, (cname, ctype) in enumerate(rel.schema):
                if cname == col_right_name:
                    right_idx = i
                    break

            # CAS A : Colonne op Colonne (ex: C4 > C2)
            if left_idx != -1 and right_idx != -1:
                # On passe l'index de droite comme "valeur", et rhs_is_col=True
                conditions.append(Condition(left_idx, op, right_idx, left_type, rhs_is_col=True))
                
            # CAS B : Colonne op Valeur (ex: C4 > 100)
            elif left_idx != -1:
                val_str = right_raw.strip('"').strip("'")
                conditions.append(Condition(left_idx, op, val_str, left_type, rhs_is_col=False))
                
            # CAS C : Valeur op Colonne (Inversion) (ex: 100 < C4)
            elif right_idx != -1:
                # On inverse tout
                val_str = left_raw.strip('"').strip("'")
                inv_ops = {'>':'<', '<':'>', '>=':'<=', '<=':'>=', '=':'=', '<>':'<>'}
                new_op = inv_ops.get(op, op)
                right_type = rel.schema[right_idx][1].split('(')[0]
                conditions.append(Condition(right_idx, new_op, val_str, right_type, rhs_is_col=False))
                
            else:
                print(f"Attention: Condition ignorée '{clause}' (colonnes introuvables)")

        if conditions:
            iterator = SelectOperator(iterator, conditions)
        return iterator


    def _create(self, cmd):
        self.db_manager.CreateTable(cmd["table"], cmd["columns"])
        return f"Table {cmd['table']} créée."

    def _drop(self, cmd):
        self.db_manager.RemoveTable(cmd["table"])
        return f"Table {cmd['table']} supprimée."

    def _insert(self, cmd):
        rel = self._get_rel(cmd["table"])
        
        # SÉCURITÉ : Vérifier que le nombre de valeurs correspond au nombre de colonnes
        if len(cmd["values"]) != len(rel.schema):
            raise ValueError(f"Erreur: La table '{rel.name}' attend {len(rel.schema)} colonnes, mais {len(cmd['values'])} valeurs fournies.")
            
        # ... suite du code d'insertion ...
        vals = [self._convert_val(v, rel.schema[i][1]) for i, v in enumerate(cmd["values"])]
        rel.InsertRecord(Record(vals))
        return "Record inséré."
    def _drop_tables(self, cmd):
        # On récupère la liste des noms pour éviter erreur de modification pendant itération
        tables_to_remove = list(self.db_manager.tables.keys())
        for name in tables_to_remove:
            self.db_manager.RemoveTable(name)
        return "Toutes les tables ont été supprimées."

    def _import(self, cmd):
        rel = self._get_rel(cmd["table"])
        filename = cmd["file"]
        
        # 1. Protection fichier introuvable
        if not os.path.exists(filename):
            return f"Erreur : Le fichier {filename} est introuvable."

        count = 0
        try:
            # On tente de détecter si c'est des virgules ou points-virgules
            # (Optionnel, mais aide si le CSV est bizarre)
            delimiter = ','
            with open(filename, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if ';' in first_line and ',' not in first_line:
                    delimiter = ';'

            with open(filename, newline='', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                
                for row in reader:
                    # 2. PROTECTION : Ignorer les lignes vides
                    if not row:
                        continue
                        
                    # 3. PROTECTION : Ignorer les lignes qui n'ont pas le bon nombre de colonnes
                    # C'est ça qui causait votre "list index out of range"
                    if len(row) != len(rel.schema):
                        # On peut print(f"Ligne ignorée : {row}") pour debug si besoin
                        continue

                    try:
                        # Conversion des valeurs
                        vals = [self._convert_val(row[i], rel.schema[i][1]) for i in range(len(rel.schema))]
                        rel.InsertRecord(Record(vals))
                        count += 1
                    except Exception as e:
                        # Si une conversion échoue (ex: texte dans un INT), on ignore la ligne
                        continue
                        
        except Exception as e:
            return f"Erreur critique lors de l'import : {str(e)}"

        # 4. Format attendu par la prof (selon Scen3)
        return f"Total records loaded={count}"

   
    def _select(self, cmd):
        rel = self._get_rel(cmd["table"])
        alias = cmd.get("alias") 
        
        # On passe l'alias pour que le WHERE fonctionne
        iterator = self._build_iterator(rel, cmd["where"], alias)

        # Projection (Selection des colonnes)
        if cmd["columns"] != ["*"]:
            indices = []
            for col in cmd["columns"]:
                # (ex: s.nom -> nom) 
                col_clean = self._resolve_col(col, rel.name, alias)
                
                found = False
                for i, (cname, _) in enumerate(rel.schema):
                    if cname == col_clean:
                        indices.append(i)
                        found = True
                        break
            
            # On n'applique la projection que si on a trouvé des colonnes valides
            if indices:
                iterator = ProjectOperator(iterator, indices)

        res = []
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            # Format: val1 ; val2 .
            res.append(" ; ".join(str(v) for v in rec.values) + " .")
            count += 1
        
        return "\n".join(res) + f"\nTotal selected records={count}"
    def _delete(self, cmd):
        rel = self._get_rel(cmd["table"])
        alias = cmd.get("alias") # Récupéré du parser
        iterator = self._build_iterator(rel, cmd["where"], alias)
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            rel.DeleteRecord(rec)
            count += 1
        return f"Total deleted records={count}"
    
    def _describe_table(self, cmd):
        rel = self.db_manager.GetTable(cmd["table"])
        if not rel: return f"Erreur : Table {cmd['table']} introuvable."
        
        # Format attendu : Nom (Col1:Type1, Col2:Type2)
        cols = ", ".join([f"{name}:{ctype}" for name, ctype in rel.schema])
        return f"{rel.name} ({cols})"

    def _describe_tables(self, cmd):
        tables = self.db_manager.tables
        count = len(tables)
        
        if count == 0:
            return "0 tables"
        
        lines = []
        for name, rel in tables.items():
            cols = ", ".join([f"{n}:{t}" for n, t in rel.schema])
            lines.append(f"{name} ({cols})")
            
        # --- MODIFICATION ICI (Ajout du compteur final) ---
        return "\n".join(lines) + f"\n{count} tables"

    def _update(self, cmd):
        rel = self._get_rel(cmd["table"])
        alias = cmd.get("alias") # Récupéré du parser
        iterator = self._build_iterator(rel, cmd["where"], alias)
        
        updates = {}
        for col_raw, val_str in cmd["set"]:
            # Nettoyage alias dans le SET (ex: a.avis -> avis)
            col_name = self._resolve_col(col_raw, rel.name, alias)
            
            for i, (cname, ctype) in enumerate(rel.schema):
                if cname == col_name:
                    updates[i] = self._convert_val(val_str, ctype)
        
        count = 0
        while True:
            rec = iterator.GetNextRecord()
            if not rec: break
            new_vals = list(rec.values)
            for idx, val in updates.items():
                new_vals[idx] = val
            rel.UpdateRecord(rec, new_vals)
            count += 1
        return f"Total updated records={count}"