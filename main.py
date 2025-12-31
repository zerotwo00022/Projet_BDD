#!/usr/bin/env python3
from dbconfig import DBConfig
from managers.disk_manager import DiskManager
from managers.buffer_manager import BufferManager
from managers.db_manager import DBManager
from sql.parser import parse
from sql.executor import SQLExecutor

def main():
    print("=== Mini-SGBD ===")
    
    # 1. Initialisation
    config = DBConfig.LoadDBConfig("config.json")
    
    # On garde les noms courts pour simplifier
    disk = DiskManager(config)
    buff = BufferManager(config, disk)
    db = DBManager(config, disk, buff)
    
    executor = SQLExecutor(db)

    print(f"Données dans : {config.dbpath}")
    print("Tapez EXIT pour quitter.")

    # 2. Boucle REPL
    while True:
        try:
            query = input("\nsql> ")
            if not query.strip(): continue
            
            if query.strip().upper() == "EXIT":
                db.SaveState()
                
                buff.FlushBuffers()
                
                disk.Finish()
                
                print("Sauvegarde effectuée. Au revoir !")
                break
            # ======================

            # 3. Traitement
            cmd = parse(query)
            result = executor.execute_command(cmd)
            print(result)

        except Exception as e:
            print(f"Erreur : {e}")

if __name__ == "__main__":
    main()