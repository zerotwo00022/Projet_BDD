from managers.page_id import PageId

class RecordId:
    def __init__(self, page_id: PageId, slot_idx: int):
        self.page_id = page_id
        self.slot_idx = slot_idx

    def __repr__(self):
        return f"RecordId({self.page_id}, {self.slot_idx})"
    
    def __eq__(self, other):
        return (self.page_id == other.page_id) and (self.slot_idx == other.slot_idx)