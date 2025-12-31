from query_engine.iterators import IRecordIterator
from managers.relation import Record

class Condition:
    """Représente une clause WHERE (ex: age > 18)"""
    def __init__(self, col_index, op, value, col_type):
        self.col_index = col_index
        self.op = op
        self.value = value
        self.col_type = col_type

    def evaluate(self, record: Record) -> bool:
        val_rec = record.values[self.col_index]
        
        # Comparaison selon le type
        target_val = self.value
        if self.col_type == "INT": target_val = int(target_val)
        elif self.col_type == "FLOAT": target_val = float(target_val)
        
        if self.op == "=": return val_rec == target_val
        if self.op == ">": return val_rec > target_val
        if self.op == "<": return val_rec < target_val
        if self.op == ">=": return val_rec >= target_val
        if self.op == "<=": return val_rec <= target_val
        if self.op == "<>": return val_rec != target_val
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
            
            # Vérifie TOUTES les conditions (AND implicite)
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