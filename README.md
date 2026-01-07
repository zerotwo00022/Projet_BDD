===========

Petit SGBD pédagogique — scripts pour lancer le projet depuis la ligne de commande.

Prerequis
---------
- Python 3.x installé et disponible dans le `PATH` (`python3` ou `python`).

Lancement
---------
- Windows (PowerShell / cmd) :

powershell
.\run.bat


- Linux / macOS (bash) :

bash
chmod +x run.sh
./run.sh

- Exécution directe (toutes plateformes) :

bash
python main.py


Configuration
-------------
- Le fichier `config.json` à la racine contient les paramètres (chemin `dbpath`, `pagesize`, etc.).

Tests
-----
- Si vous avez `pytest` :
bash
python -m pytest tests


Remarques
---------
- `run.bat` et `run.sh` sont fournis à la racine et rendent le projet portable (ils utilisent le répertoire du script et choisissent `python3`/`python`).