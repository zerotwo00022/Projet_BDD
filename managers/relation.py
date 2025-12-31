import struct
from managers.page_id import PageId
from managers.record_id import RecordId

class Record:
    def __init__(self, values=None):
        self.values = values if values is not None else []
    def __repr__(self):
        return f"Record({self.values})"

class Relation:
    def __init__(self, name: str, schema: list, disk_manager, buffer_manager):
        self.name = name
        self.schema = schema
        self.disk_manager = disk_manager
        self.buffer_manager = buffer_manager
        
        # TP5 A1: Header Page ID (Pointeur vers la page d'en-tête de la relation)
        # Pour l'instant, on l'initialisera lors de la création effective
        self.header_page_id = None 

        # Calcul de la taille d'un record
        self.record_size = self._compute_record_size()
        
        # TP5 A2: Calcul du nombre de slots (cases) par page
        # Formule : PageSize >= N * (1 octet de bitmap + record_size)
        # Donc N = PageSize / (1 + record_size)
        self.slot_count = self.disk_manager.config.pagesize // (1 + self.record_size)

    def _compute_record_size(self):
        size = 0
        for _, type_col in self.schema:
            size += self._get_column_size(type_col)
        return size

    def _get_column_size(self, type_col: str):
        type_col = type_col.upper()
        if type_col == "INT" or type_col == "FLOAT":
            return 4
        elif "CHAR" in type_col:
            start = type_col.find("(") + 1
            end = type_col.find(")")
            return int(type_col[start:end])
        raise ValueError(f"Type inconnu: {type_col}")

    # ==========================================
    # TP5 C2: Rajout d'une page de données vide
    # ==========================================
    def add_data_page(self) -> PageId:
        """Alloue une nouvelle page et l'initialise avec une bitmap vide."""
        page_id = self.disk_manager.AllocPage()
        
        # On récupère le buffer pour initialiser la page à 0 (vide)
        buff = self.buffer_manager.GetPage(page_id)
        
        # Remise à zéro explicite (optionnelle si AllocPage renvoie du vide, mais plus sûre)
        for i in range(len(buff)):
            buff[i] = 0
            
        # On marque la page comme modifiée et on la libère
        self.buffer_manager.FreePage(page_id, True)
        return page_id

    # ==========================================
    # TP5 C3: Trouver une page avec de la place
    # ==========================================
    def get_free_data_page_id(self) -> PageId:
        """
        Cherche une page qui a un slot libre (= 0 dans la bitmap).
        (Version simplifiée : parcourt toutes les pages.
         Version TP5 complète : devrait utiliser la Header Page et les listes chaînées)
        """
        # Note: Pour ce stade, on va simplement allouer une nouvelle page à chaque fois
        # si on ne gère pas encore la liste chaînée des pages libres.
        # Pour respecter le TP strictement, il faudrait parcourir la liste stockée dans le HeaderPage.
        # ICI : On simplifie pour faire marcher l'insertion -> On crée une nouvelle page.
        return self.add_data_page()

    # ==========================================
    # TP5 C4: Écrire un Record
    # ==========================================
    def write_record_to_data_page(self, record: Record, page_id: PageId) -> RecordId:
        """Trouve un slot libre dans la page, écrit le record et met à jour la bitmap."""
        buff = self.buffer_manager.GetPage(page_id)
        
        # 1. Parcourir la Bitmap (les 'slot_count' premiers octets)
        free_slot_idx = -1
        for i in range(self.slot_count):
            if buff[i] == 0:  # 0 signifie vide [cite: 12]
                free_slot_idx = i
                break
        
        if free_slot_idx == -1:
            self.buffer_manager.FreePage(page_id, False)
            raise Exception("Page pleine ! Impossible d'insérer.")

        # 2. Mettre à jour la Bitmap (1 = occupé)
        buff[free_slot_idx] = 1
        
        # 3. Calculer la position d'écriture
        # Les données commencent juste après la bitmap
        offset_start = self.slot_count 
        position = offset_start + (free_slot_idx * self.record_size)
        
        # 4. Écrire les données (utilise la méthode du TP4)
        self._write_record_to_buffer(record, buff, position)
        
        self.buffer_manager.FreePage(page_id, True) # True car on a modifié
        return RecordId(page_id, free_slot_idx)

    # ==========================================
    # TP5 C5: Lire tous les records d'une page
    # ==========================================
    def get_records_in_data_page(self, page_id: PageId) -> list:
        """Renvoie la liste des Records présents dans la page."""
        records = []
        buff = self.buffer_manager.GetPage(page_id)
        
        offset_start = self.slot_count
        
        for i in range(self.slot_count):
            # Si le bit est à 1, il y a un record
            if buff[i] == 1:
                position = offset_start + (i * self.record_size)
                rec = Record()
                self._read_from_buffer(rec, buff, position)
                records.append(rec)
                
        self.buffer_manager.FreePage(page_id, False)
        return records

    # ==========================================
    # Méthodes bas niveau (TP4) intégrées ici
    # ==========================================
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
                # Padding avec des 0
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

    # ==========================================
    # API Publique (TP5 C7)
    # ==========================================
    def GetAllRecords(self):
        """Récupère tous les records de toutes les pages de données."""
        # TODO: Il faudra itérer sur toutes les pages allouées à cette relation
        # Pour l'instant, c'est vide car nous n'avons pas connecté la Header Page
        return []

    def InsertRecord(self, record: Record) -> RecordId:
        """API Haut niveau pour insérer un record."""
        page_id = self.get_free_data_page_id()
        return self.write_record_to_data_page(record, page_id)