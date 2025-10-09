import json

class DBConfig:
    def __init__(self, dbpath: str):
        self.dbpath = dbpath

    @staticmethod
    def LoadDBConfig(fichier_config: str) -> "DBConfig":
        with open(fichier_config, "r", encoding="utf-8") as f:
            data = json.load(f)
        return DBConfig(dbpath=data["dbpath"])

    def __repr__(self):
        return f"<DBConfig dbpath='{self.dbpath}'>"
