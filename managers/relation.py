import struct

class Record:
    """Représente un tuple (une ligne de données)"""
    def __init__(self, values=None):
        # Liste des valeurs (ex: [1, "Alice", 12.5])
        self.values = values if values is not None else []

    def __repr__(self):
        return f"Record({self.values})"

class Relation:
    """Gère le schéma d'une table et la sérialisation des records"""
    def __init__(self, name: str, schema: dict):
        self.name = name
        # Schema est un dict ou liste : {"Nom": "VARCHAR(20)", "Age": "INT"}
        # Pour simplifier l'ordre, on attend une liste de tuples : [("Nom", "VARCHAR(20)"), ...]
        self.schema = schema 
        
        # Variables pour le stockage (TP5 - on anticipe)
        self.header_page_id = None
        self.disk_manager = None
        self.buffer_manager = None

    def get_record_size(self):
        """Calcule la taille fixe en octets d'un record pour cette relation."""
        size = 0
        for _, type_col in self.schema:
            size += self.get_column_size(type_col)
        return size

    def get_column_size(self, type_col: str):
        """Retourne la taille en octets d'un type donné."""
        type_col = type_col.upper()
        if type_col == "INT":
            return 4
        elif type_col == "FLOAT":
            return 4
        elif type_col.startswith("CHAR") or type_col.startswith("VARCHAR"):
            # Extraction de la taille N dans CHAR(N) ou VARCHAR(N)
            # Ex: "VARCHAR(20)" -> split("(") -> ["VARCHAR", "20)"] -> "20"
            start = type_col.find("(") + 1
            end = type_col.find(")")
            return int(type_col[start:end])
        raise ValueError(f"Type inconnu: {type_col}")

    def writeRecordToBuffer(self, record: Record, buff: bytearray, pos: int):
        """
        Écrit les valeurs du record dans le buffer à la position pos
        """
        cursor = pos
        
        # On itère sur les colonnes du schéma
        for i, (col_name, col_type) in enumerate(self.schema):
            val = record.values[i]
            col_type = col_type.upper()
            
            if col_type == "INT":
                # <i = Little endian integer (4 bytes)
                struct.pack_into('<i', buff, cursor, int(val))
                cursor += 4
                
            elif col_type == "FLOAT":
                # <f = Little endian float (4 bytes)
                struct.pack_into('<f', buff, cursor, float(val))
                cursor += 4
                
            elif col_type.startswith("CHAR") or col_type.startswith("VARCHAR"):
                # Gestion des chaînes : on écrit taille fixe
                max_len = self.get_column_size(col_type)
                
                # Encodage et troncature si trop long
                str_bytes = str(val).encode('utf-8')
                if len(str_bytes) > max_len:
                    str_bytes = str_bytes[:max_len]
                
                # Écriture dans le buffer
                # On copie les octets
                buff[cursor : cursor + len(str_bytes)] = str_bytes
                
                # Padding (remplissage avec des 0) pour atteindre la taille fixe
                # C'est important pour respecter la structure "taille fixe"
                padding_len = max_len - len(str_bytes)
                if padding_len > 0:
                    # On met des zéros pour la suite
                    buff[cursor + len(str_bytes) : cursor + max_len] = b'\x00' * padding_len
                
                cursor += max_len

    def readFromBuffer(self, record: Record, buff: bytearray, pos: int):
        """
        Lit les valeurs depuis le buffer et remplit l'objet record. [cite: 502]
        """
        cursor = pos
        values = []
        
        for col_name, col_type in self.schema:
            col_type = col_type.upper()
            
            if col_type == "INT":
                val = struct.unpack_from('<i', buff, cursor)[0]
                values.append(val)
                cursor += 4
                
            elif col_type == "FLOAT":
                val = struct.unpack_from('<f', buff, cursor)[0]
                values.append(val)
                cursor += 4
                
            elif col_type.startswith("CHAR") or col_type.startswith("VARCHAR"):
                max_len = self.get_column_size(col_type)
                # Lecture brute des octets
                raw_bytes = buff[cursor : cursor + max_len]
                
                # On décode en enlevant les zéros de padding (strip b'\x00')
                val = raw_bytes.rstrip(b'\x00').decode('utf-8')
                values.append(val)
                cursor += max_len
        
        record.values = values