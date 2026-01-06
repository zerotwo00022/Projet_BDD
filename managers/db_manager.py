import os
import pickle
from managers.relation import Relation

class DBManager:
    def __init__(self, config, disk_manager, buffer_manager):
        self.config = config
        self.disk_manager = disk_manager
        self.buffer_manager = buffer_manager
        self.tables = {}  # Stocke les objets Relation : { "NomTable": RelationObject }
        
        # Charge les tables existantes au démarrage
        self.LoadState()

    def CreateTable(self, table_name: str, schema: list):
        """Crée une nouvelle table et l'ajoute au catalogue."""
        if table_name in self.tables:
            raise ValueError(f"La table {table_name} existe déjà.")
            
        # Création de la relation
        rel = Relation(table_name, schema, self.disk_manager, self.buffer_manager)
        
        # On peut ici initialiser une Header Page si nécessaire (TP5 A1)
        
        self.tables[table_name] = rel
        return rel

    def GetTable(self, table_name: str) -> Relation:
        """Récupère une table par son nom."""
        return self.tables.get(table_name)

    def RemoveTable(self, table_name: str):
        """Supprime une table."""
        if table_name in self.tables:
            del self.tables[table_name]
        else:
            raise ValueError(f"Table {table_name} introuvable.")

    def SaveState(self):
        """Sauvegarde le catalogue (tables, schémas ET pages allouées)."""
        catalog_data = {}
        for name, rel in self.tables.items():
            catalog_data[name] = {
                "schema": rel.schema,
                "header_page_id": rel.header_page_id,
                "allocated_pages": rel.allocated_pages 
            }
        
        save_path = os.path.join(self.config.dbpath, "tables.sv")
        with open(save_path, "wb") as f:
            pickle.dump(catalog_data, f)

    def LoadState(self):
        """Recharge les tables et leurs pages."""
        save_path = os.path.join(self.config.dbpath, "tables.sv")
        if not os.path.exists(save_path):
            return

        with open(save_path, "rb") as f:
            catalog_data = pickle.load(f)

        for name, info in catalog_data.items():
            rel = Relation(name, info["schema"], self.disk_manager, self.buffer_manager)
            rel.header_page_id = info["header_page_id"]
            
            rel.allocated_pages = info.get("allocated_pages", [])
            
            self.tables[name] = rel