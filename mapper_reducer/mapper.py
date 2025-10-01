#!/usr/bin/env python3
"""
Mapper pour la détection de churn clients
Lit les logs d'usage et extrait les informations par utilisateur
Format d'entrée: user_id,timestamp,action,duration
Format de sortie: user_id\t1,duration,action
"""

import sys
import csv
from io import StringIO

def main():
    # Lire ligne par ligne depuis stdin
    for line in sys.stdin:
        line = line.strip()
        
        # Ignorer les lignes vides ou les en-têtes
        if not line or line.startswith('user_id'):
            continue
        
        try:
            # Parser la ligne CSV
            # Format: user_id,timestamp,action,duration
            reader = csv.reader(StringIO(line))
            row = next(reader)
            
            if len(row) != 4:
                continue
                
            user_id, timestamp, action, duration = row
            
            # Convertir la durée en entier (0 si vide ou invalide)
            try:
                duration = int(duration)
            except (ValueError, TypeError):
                duration = 0
            
            # Émettre: user_id \t compteur,durée,action
            # Le compteur est toujours 1 (une action par ligne)
            print(f"{user_id}\t1,{duration},{action}")
            
        except (csv.Error, ValueError, IndexError) as e:
            # Ignorer les lignes malformées
            continue

if __name__ == "__main__":
    main()