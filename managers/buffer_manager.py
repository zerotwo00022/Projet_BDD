class BufferFrame:
    """Représente une page en mémoire"""
    def __init__(self, page_id, data):
        self.page_id = page_id
        self.data = bytearray(data)
        self.dirty = False  # True si la page a été modifiée
        self.pin_count = 0  # nombre de processus qui utilisent la page

class BufferManager:
    def __init__(self, disk_manager, pool_size=10):       # on met 10 pages pour le buffer pool pour la simplification et le test
        self.disk = disk_manager
        self.pool_size = pool_size
        self.buffer_pool = {}  # De type {page_id: BufferFrame}

    def fetch_page(self, page_id):
        """Charge une page depuis le disque si elle n est pas deja dans le buffer"""
        if page_id in self.buffer_pool:
            frame = self.buffer_pool[page_id]
            frame.pin_count += 1
            return frame

        if len(self.buffer_pool) >= self.pool_size:
            self.evict_page()  # libère une place

        data = self.disk.read_page(page_id)
        frame = BufferFrame(page_id, data)
        frame.pin_count += 1
        self.buffer_pool[page_id] = frame
        return frame

    def mark_dirty(self, frame):
        """Marque une page comme modifiée"""
        frame.dirty = True

    def release_page(self, frame):
        """Libère une page (décrémente pin_count)"""
        frame.pin_count = max(0, frame.pin_count - 1)

    def evict_page(self):
        """Évite une page (LRU simplifié)"""
        for pid, frame in list(self.buffer_pool.items()):
            if frame.pin_count == 0:
                if frame.dirty:
                    print(f"→ Écriture de la page {pid} sur le disque")
                    self.disk.write_page(pid, frame.data)
                del self.buffer_pool[pid]  # supprimer la page du buffer
                return
        raise Exception("Aucune page éjectable dans le buffer pool !")

    def flush_all(self):
        """Écrit toutes les pages modifiées sur le disque"""
        for pid, frame in self.buffer_pool.items():
            if frame.dirty:
                self.disk.write_page(pid, frame.data)
                frame.dirty = False