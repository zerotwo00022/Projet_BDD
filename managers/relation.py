import struct
from managers.page_id import PageId
from managers.record_id import RecordId

class Record:
    def __init__(self, values=None):
        self.values = values if values is not None else []
        self.rid = None #  TP7 Stocke l'ID physique (PageId, SlotIdx)

    def __repr__(self):
        return f"Record({self.values})"

class Relation:
    def __init__(self, name: str, schema: list, disk_manager, buffer_manager):
        self.name = name
        self.schema = schema
        self.disk_manager = disk_manager
        self.buffer_manager = buffer_manager
        
        self.header_page_id = None 
        
        # Liste pour suivre les pages de données allouées
        self.allocated_pages = []

        self.record_size = self._compute_record_size()
        # Formule  : N = PageSize / (1 + record_size)
        self.slot_count = self.disk_manager.config.pagesize // (1 + self.record_size)

    
    def GetDataPages(self):
        """Retourne la liste des pages de données de cette relation."""
        return self.allocated_pages

    # Gestion des Pages (TP5 + TP7)
    def add_data_page(self) -> PageId:
        """Alloue une nouvelle page, l'initialise et la stocke dans la liste."""
        page_id = self.disk_manager.AllocPage()
        
        buff = self.buffer_manager.GetPage(page_id)
        # Initialisation à 0 (Bitmap vide)
        for i in range(len(buff)):
            buff[i] = 0
            
        self.buffer_manager.FreePage(page_id, True)
        
        #On mémorise que cette page appartient à la relation
        self.allocated_pages.append(page_id)
        return page_id

    def get_free_data_page_id(self) -> PageId:
        """Cherche une page avec de la place"""
        
        return self.add_data_page()

  
    def write_record_to_data_page(self, record: Record, page_id: PageId) -> RecordId:
        buff = self.buffer_manager.GetPage(page_id)
        
        # Trouver un slot libre dans la Bitmap
        free_slot_idx = -1
        for i in range(self.slot_count):
            if buff[i] == 0:
                free_slot_idx = i
                break
        
        if free_slot_idx == -1:
            self.buffer_manager.FreePage(page_id, False)
            raise Exception("Page pleine !")

        # Marquer occupé
        buff[free_slot_idx] = 1
        
        # Écrire les données
        offset_start = self.slot_count 
        position = offset_start + (free_slot_idx * self.record_size)
        self._write_record_to_buffer(record, buff, position)
        
        self.buffer_manager.FreePage(page_id, True)
        return RecordId(page_id, free_slot_idx)

    def read_record_from_page(self, page_id: PageId, slot_idx: int) -> Record:
        """Lit un record spécifique"""
        buff = self.buffer_manager.GetPage(page_id)
        offset_start = self.slot_count
        position = offset_start + (slot_idx * self.record_size)
        
        rec = Record()
        self._read_from_buffer(rec, buff, position)
        
        self.buffer_manager.FreePage(page_id, False)
        return rec

    
    def DeleteRecord(self, record: Record):
        if record.rid is None:
            raise Exception("Impossible de supprimer : RID manquant")
        
        page_id = record.rid.page_id
        slot_idx = record.rid.slot_idx
        
        buff = self.buffer_manager.GetPage(page_id)
        # On remet le bit à 0 dans la bitmap
        buff[slot_idx] = 0
        self.buffer_manager.FreePage(page_id, True)

    def UpdateRecord(self, record: Record, new_values: list):
        if record.rid is None:
             raise Exception("Impossible d'update : RID manquant")
             
        page_id = record.rid.page_id
        slot_idx = record.rid.slot_idx
        
        buff = self.buffer_manager.GetPage(page_id)
        offset_start = self.slot_count
        position = offset_start + (slot_idx * self.record_size)
        
        # Écrasement des données
        temp_rec = Record(new_values)
        self._write_record_to_buffer(temp_rec, buff, position)
        
        self.buffer_manager.FreePage(page_id, True)

    
    def InsertRecord(self, record: Record) -> RecordId:
        page_id = self.get_free_data_page_id()
        return self.write_record_to_data_page(record, page_id)

    
    def _compute_record_size(self):
        size = 0
        for _, type_col in self.schema:
            size += self._get_column_size(type_col)
        return size

    def _get_column_size(self, type_col: str):
        type_col = type_col.upper()
        if type_col == "INT" or type_col == "FLOAT": return 4
        if "CHAR" in type_col:
            return int(type_col.split("(")[1].split(")")[0])
        raise ValueError(f"Type inconnu: {type_col}")

    def _write_record_to_buffer(self, record, buff, pos):
        cursor = pos
        for i, (col_name, col_type) in enumerate(self.schema):
            val = record.values[i]
            col_type = col_type.upper()
            
            if col_type == "INT":
                struct.pack_into('<i', buff, cursor, int(val))
                cursor += 4
            elif col_type == "FLOAT":
                struct.pack_into('<f', buff, cursor, float(val))
                cursor += 4
            elif "CHAR" in col_type:
                max_len = self._get_column_size(col_type)
                s_bytes = str(val).encode('utf-8')[:max_len]
                buff[cursor:cursor+len(s_bytes)] = s_bytes
                padding = max_len - len(s_bytes)
                if padding > 0:
                    buff[cursor+len(s_bytes):cursor+max_len] = b'\x00'*padding
                cursor += max_len

    def _read_from_buffer(self, record, buff, pos):
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
            elif "CHAR" in col_type:
                max_len = self._get_column_size(col_type)
                raw = buff[cursor:cursor+max_len]
                val = raw.rstrip(b'\x00').decode('utf-8')
                values.append(val)
                cursor += max_len
        record.values = values