@echo off
REM Se placer dans le répertoire du script pour rendre le batch portable
cd /d "%~dp0"
echo Lancement du mini-SGBD...
python main.py
pause