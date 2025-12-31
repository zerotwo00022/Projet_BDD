import json
import os

class DBConfig:
    def __init__(self, dbpath: str, pagesize=4096, dm_maxfilecount=4):
        self.dbpath = dbpath
        self.pagesize = pagesize            # Taille d'une page (défaut 4096)
        self.dm_maxfilecount = dm_maxfilecount  # Nombre max de fichiers DataX.bin

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
            dm_maxfilecount=data.get("dm_maxfilecount", 4)
        )
    def __repr__(self):
        return f"<DBConfig dbpath='{self.dbpath}'>"
