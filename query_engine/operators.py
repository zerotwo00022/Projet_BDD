from query_engine.iterators import IRecordIterator
from managers.relation import Record

class Condition:
    def __init__(self, col_idx, op, val, type_col, rhs_is_col=False):
        self.col_idx = col_idx
        self.op = op
        self.val = val          # Ce sera soit la valeur brute, soit l'INDEX de la 2ème colonne
        self.type_col = type_col
        self.rhs_is_col = rhs_is_col # True si on compare colonne vs colonne

        # Si ce n'est pas une colonne à droite, on convertit la valeur tout de suite
        if not self.rhs_is_col:
            if self.type_col == "INT":
                self.val = int(float(self.val))
            elif self.type_col == "FLOAT":
                self.val = float(self.val)

    def evaluate(self, rec):
        # Récupération valeur gauche
        val_left = rec.values[self.col_idx]
        
        # Récupération valeur droite
        if self.rhs_is_col:
            # Si c'est Col vs Col, on va chercher la valeur dans le record à l'index stocké
            val_right = rec.values[self.val] 
            
            try:
                if isinstance(val_left, (int, float)) and isinstance(val_right, str):
                    val_right = float(val_right)
                elif isinstance(val_left, str) and isinstance(val_right, (int, float)):
                    val_left = float(val_left)
            except:
                pass # Si conversion impossible, je laisse Python comparer
        else:
            # Sinon c'est une constante (déjà convertie dans le __init__)
            val_right = self.val

        # 3. Comparaison
        if self.op == "=": return val_left == val_right
        if self.op == "<>": return val_left != val_right
        if self.op == ">": return val_left > val_right
        if self.op == "<": return val_left < val_right
        if self.op == ">=": return val_left >= val_right
        if self.op == "<=": return val_left <= val_right
        return False

class SelectOperator(IRecordIterator):
    def __init__(self, child_iterator: IRecordIterator, conditions: list):
        self.child = child_iterator
        self.conditions = conditions # Liste d'objets Condition

    def GetNextRecord(self):
        while True:
            rec = self.child.GetNextRecord()
            if rec is None:
                return None
            
            # Vérifie TOUTES les conditions
            match = True
            for cond in self.conditions:
                if not cond.evaluate(rec):
                    match = False
                    break
            
            if match:
                return rec

    def Close(self):
        self.child.Close()

    def Reset(self):
        self.child.Reset()

class ProjectOperator(IRecordIterator):
    def __init__(self, child_iterator: IRecordIterator, keep_indices: list):
        self.child = child_iterator
        self.keep_indices = keep_indices # Liste des index de colonnes à garder

    def GetNextRecord(self):
        rec = self.child.GetNextRecord()
        if rec is None:
            return None
        
        # Crée un nouveau record avec seulement les colonnes demandées
        new_values = [rec.values[i] for i in self.keep_indices]
        return Record(new_values)

    def Close(self):
        self.child.Close()
        
    def Reset(self):
        self.child.Reset()