import os
import pickle
from managers.page_id import PageId

class DiskManager:
    def __init__(self, db_config):
        self.config = db_config
        # Chemin vers le dossier BinData
        self.bindata_path = os.path.join(self.config.dbpath, "BinData")
        os.makedirs(self.bindata_path, exist_ok=True)

        # Liste des pages désallouées (libres) pour réutilisation
        self.free_pages = [] 
        
        # Initialisation (chargement de l'état si existant)
        self.Init()

    def AllocPage(self) -> PageId:
        """Alloue une page : réutilise une page libre ou en crée une nouvelle"""
        # 1. Priorité : Réutiliser une page désallouée
        if self.free_pages:
            return self.free_pages.pop(0)

        # 2. Sinon : Créer une nouvelle page à la fin du dernier fichier
        # On cherche le fichier courant pour écrire
        for file_id in range(self.config.dm_maxfilecount):
            file_path = self.get_file_path(file_id)
            
            # Si le fichier n'existe pas, on le crée
            if not os.path.exists(file_path):
                with open(file_path, "wb") as f:
                    pass # Crée un fichier vide
            
            # Vérifier la taille du fichier
            file_size = os.path.getsize(file_path)
            # Calculer le nombre de pages actuelles
            num_pages = file_size // self.config.pagesize
            
            
            
            with open(file_path, "ab") as f:
                f.write(bytearray(self.config.pagesize))
                
            return PageId(file_id, num_pages)

        raise Exception("DiskManager : Espace disque saturé (MaxFileCount atteint)")

    def ReadPage(self, page_id: PageId, buff: bytearray):
        """Lit une page du disque vers le buffer fourni"""
        file_path = self.get_file_path(page_id.FileIdx)
        offset = page_id.PageIdx * self.config.pagesize

        if not os.path.exists(file_path):
            raise Exception(f"Fichier introuvable : {file_path}")

        with open(file_path, "rb") as f:
            f.seek(offset)
            data = f.read(self.config.pagesize)
            # Copie les données dans le bytearray fourni (mutable)
            buff[:] = data

    def WritePage(self, page_id: PageId, buff: bytearray):
        """Écrit le contenu du buffer sur le disque"""
        file_path = self.get_file_path(page_id.FileIdx)
        offset = page_id.PageIdx * self.config.pagesize
        
        # "r+b" permet de lire et écrire sans écraser tout le fichier
        with open(file_path, "r+b") as f:
            f.seek(offset)
            f.write(buff)

    def DeallocPage(self, page_id: PageId):
        """Marque une page comme libre pour réutilisation"""
        # On ajoute simplement l'ID à la liste des pages libres
        self.free_pages.append(page_id)

    def get_file_path(self, file_idx):
        """Retourne le chemin complet de DataX.bin"""
        return os.path.join(self.bindata_path, f"Data{file_idx}.bin")

    def Init(self):
        """Charge l'état du DiskManager (pages libres)"""
        save_path = os.path.join(self.bindata_path, "dm_save.bin")
        if os.path.exists(save_path):
            with open(save_path, "rb") as f:
                self.free_pages = pickle.load(f)

    def Finish(self):
        """Sauvegarde l'état du DiskManager (pages libres)"""
        save_path = os.path.join(self.bindata_path, "dm_save.bin")
        with open(save_path, "wb") as f:
            pickle.dump(self.free_pages, f)