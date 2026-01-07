#!/bin/bash
echo "Lancement du mini-SGBD..."
# Try python3 first, fall back to python
if command -v python3 >/dev/null 2>&1; then
    python3 main.py
elif command -v python >/dev/null 2>&1; then
    python main.py
else
    echo "Erreur : python3 ou python introuvable dans le PATH."
    exit 1
fi