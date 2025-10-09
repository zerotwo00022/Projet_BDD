#!/usr/bin/env python3
import os
from dbconfig import DBConfig
from sql.parser import parse
from sql.executor import execute

# Charger configuration
config = DBConfig.LoadDBConfig("config.json")
os.makedirs(config.dbpath, exist_ok=True)

def repl():
    print("=== Mini-SGBD en Python (stockage binaire) ===")
    print("Chemin des données :", config.dbpath)
    print("Tapez vos requêtes SQL (CREATE, INSERT, SELECT, etc.)")
    print("Tapez EXIT pour quitter.\n")

    while True:
        query = input("sql> ")
        if query.strip().upper() == "EXIT":
            print("Au revoir !")
            break

        try:
            command = parse(query)
            result = execute(command, config.dbpath)
            if result is not None:
                print(result)
        except Exception as e:
            print("Erreur:", e)

if __name__ == "__main__":
    repl()
