from query_engine.iterators import IRecordIterator
from managers.page_id import PageId
from managers.record_id import RecordId
from query_engine.iterators import IRecordIterator
from managers.record_id import RecordId
class RelationScanner(IRecordIterator):
    def __init__(self, relation):
        self.relation = relation
        self.current_page_idx = 0
        self.current_slot_idx = 0
        
        # Pour simplifier sans HeaderPage complexe, on récupère la liste des pages allouées
        self.data_pages = self.relation.GetDataPages() 
        self.current_record_idx_in_page_list = 0

    def GetNextRecord(self):
        while self.current_record_idx_in_page_list < len(self.data_pages):
            page_id = self.data_pages[self.current_record_idx_in_page_list]
            buff = self.relation.buffer_manager.GetPage(page_id)
            slot_count = self.relation.slot_count
            
            while self.current_slot_idx < slot_count:
                if buff[self.current_slot_idx] == 1:
                    # Lire le record
                    record = self.relation.read_record_from_page(page_id, self.current_slot_idx)
                    
                    # === AJOUT CRUCIAL TP7 ===
                    # On attache l'ID physique au record pour DELETE/UPDATE
                    record.rid = RecordId(page_id, self.current_slot_idx) 
                    
                    self.current_slot_idx += 1
                    self.relation.buffer_manager.FreePage(page_id, False)
                    return record
                
                self.current_slot_idx += 1
            
            self.relation.buffer_manager.FreePage(page_id, False)
            self.current_record_idx_in_page_list += 1
            self.current_slot_idx = 0
            
        return None

    def Close(self):
        pass

    def Reset(self):
        self.current_record_idx_in_page_list = 0
        self.current_slot_idx = 0