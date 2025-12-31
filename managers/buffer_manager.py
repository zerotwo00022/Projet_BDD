from managers.page_id import PageId
from managers.disk_manager import DiskManager

class BufferFrame:
    """Une case mémoire (Frame) qui contient une page."""
    def __init__(self, data: bytearray, page_id: PageId):
        self.data = data
        self.page_id = page_id
        self.pin_count = 0
        self.dirty = False

class BufferManager:
    def __init__(self, config, disk_manager: DiskManager):
        self.config = config
        self.disk_manager = disk_manager  # Pointeur vers DiskManager [cite: 576]
        # buffer_pool : Dictionnaire {PageId: BufferFrame}
        # L'ordre d'insertion/accès dans ce dictionnaire servira à gérer le temps pour LRU/MRU.
        self.buffer_pool = {} 

    def GetPage(self, page_id: PageId) -> bytearray:
        """
        Retourne le buffer correspondant à la page demandée.
        Charge la page depuis le disque si nécessaire.
        """
        # 1. Si la page est déjà dans le pool
        if page_id in self.buffer_pool:
            frame = self.buffer_pool[page_id]
            frame.pin_count += 1
            
            # Mise à jour de l'ordre d'accès (on le remet à la fin = "récent")
            # Utile pour LRU et MRU pour savoir qui est le plus récent.
            del self.buffer_pool[page_id]
            self.buffer_pool[page_id] = frame
            
            return frame.data

        # 2. Si la page n'est pas dans le pool
        # Vérifier s'il reste de la place, sinon évincer une page
        if len(self.buffer_pool) >= self.config.bm_buffercount:
            self._evict_page()

        # Charger la page depuis le disque
        # On crée un nouveau bytearray de la taille d'une page
        buff = bytearray(self.config.pagesize)
        self.disk_manager.ReadPage(page_id, buff) # [cite: 131]
        
        # Créer la frame et l'ajouter au pool
        frame = BufferFrame(buff, page_id)
        frame.pin_count = 1
        self.buffer_pool[page_id] = frame
        
        return frame.data

    def FreePage(self, page_id: PageId, valdirty: bool):
        """
        Libère la page (décrémente pin_count).
        Marque la page comme 'dirty' si valdirty est True.
        """
        if page_id in self.buffer_pool:
            frame = self.buffer_pool[page_id]
            if frame.pin_count > 0:
                frame.pin_count -= 1
            if valdirty:
                frame.dirty = True

    def FlushBuffers(self):
        """
        Écrit toutes les pages 'dirty' sur le disque et vide le pool.
       Remise à zéro complète du BufferManager
        """
        for page_id, frame in self.buffer_pool.items():
            if frame.dirty:
                self.disk_manager.WritePage(page_id, frame.data)
        
        # On vide complètement le pool (remise à zéro)
        self.buffer_pool.clear()

    def SetCurrentReplacementPolicy(self, policy: str):
        """Change la politique de remplacement (LRU ou MRU)."""
        self.config.bm_policy = policy

    def _evict_page(self):
        """Algorithme de remplacement (interne)."""
        victim_id = None
        
        # On récupère la liste des items (l'ordre est préservé : début=vieux, fin=récent)
        frames_list = list(self.buffer_pool.items())

        if self.config.bm_policy == "LRU":
            # LRU : On cherche le PLUS ANCIEN (début de liste) avec pin_count == 0
            for pid, frame in frames_list:
                if frame.pin_count == 0:
                    victim_id = pid
                    break
        
        elif self.config.bm_policy == "MRU":
            # MRU : On cherche le PLUS RÉCENT (fin de liste) avec pin_count == 0
            # reversed() permet de parcourir de la fin vers le début
            for pid, frame in reversed(frames_list):
                if frame.pin_count == 0:
                    victim_id = pid
                    break

        if victim_id is None:
            raise Exception("BufferManager : Aucun buffer libérable (tous les pin_count > 0) !")

        # Gestion de la victime
        victim_frame = self.buffer_pool[victim_id]
        if victim_frame.dirty:
            self.disk_manager.WritePage(victim_id, victim_frame.data)
        
        # Suppression du pool
        del self.buffer_pool[victim_id]