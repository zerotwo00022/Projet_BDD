import json
import os

class DBConfig:
    def __init__(self, dbpath: str, pagesize=4096, dm_maxfilecount=4, bm_buffercount=2, bm_policy="LRU"):
        self.dbpath = dbpath
        self.pagesize = pagesize            # Taille d'une page (défaut 4096)
        self.dm_maxfilecount = dm_maxfilecount  # Nombre max de fichiers DataX.bin
        self.bm_buffercount = bm_buffercount  # Nombre max de pages en mémoire 
        self.bm_policy = bm_policy            # "LRU" ou "MRU" 

    @staticmethod
    def LoadDBConfig(fichier_config: str) -> "DBConfig":
        if not os.path.exists(fichier_config):
            # Configuration par défaut si le fichier n'existe pas
            return DBConfig("./databases")
            
        with open(fichier_config, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        
        return DBConfig(
            dbpath=data.get("dbpath", "./databases"),
            pagesize=data.get("pagesize", 4096),
            dm_maxfilecount=data.get("dm_maxfilecount", 4),
            bm_buffercount=data.get("bm_buffercount", 2),
            bm_policy=data.get("bm_policy", "LRU")
        )
   
