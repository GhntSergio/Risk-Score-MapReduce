#!/usr/bin/env python3
"""
Générateur de données de test pour la détection de churn
Crée des logs d'usage simulés pour tester le pipeline MapReduce
"""

import random
import datetime
import os
from pathlib import Path

# Récupère le chemin absolu du dossier courant
BASE_DIR = Path(__file__).resolve().parent
print(f"📂 Répertoire de base: {BASE_DIR}")


# Paramètres configurables
NUM_USERS = 150           # Nombre d'utilisateurs à simuler
NUM_DAYS = 20           # Nombre de jours de données
ACTIONS = ['login', 'logout', 'watch_video', 'purchase', 'comment', 'search', 'download']

# Profils d'utilisateurs pour créer des patterns réalistes
USER_PROFILES = {
    'active': {
        'actions_per_day': (8, 15),
        'duration_range': (30, 600),
        'action_weights': {
            'login': 0.2, 'logout': 0.2, 'watch_video': 0.3, 
            'purchase': 0.1, 'comment': 0.1, 'search': 0.05, 'download': 0.05
        }
    },
    'moderate': {
        'actions_per_day': (3, 8),
        'duration_range': (10, 300),
        'action_weights': {
            'login': 0.3, 'logout': 0.3, 'watch_video': 0.2, 
            'purchase': 0.05, 'comment': 0.05, 'search': 0.05, 'download': 0.05
        }
    },
    'at_risk': {
        'actions_per_day': (1, 3),
        'duration_range': (5, 60),
        'action_weights': {
            'login': 0.5, 'logout': 0.5, 'watch_video': 0.0, 
            'purchase': 0.0, 'comment': 0.0, 'search': 0.0, 'download': 0.0
        }
    }
}

def weighted_choice(choices):
    """Sélection pondérée d'une action"""
    total = sum(choices.values())
    r = random.uniform(0, total)
    upto = 0
    for choice, weight in choices.items():
        if upto + weight >= r:
            return choice
        upto += weight
    return list(choices.keys())[-1]

def generate_user_data(user_id, profile_name, num_days):
    """Génère les données pour un utilisateur selon son profil"""
    profile = USER_PROFILES[profile_name]
    user_data = []
    
    for day in range(num_days):
        date = datetime.date.today() - datetime.timedelta(days=num_days - day - 1)
        
        # Nombre d'actions pour ce jour (peut être 0 pour les utilisateurs à risque)
        if profile_name == 'at_risk' and random.random() < 0.3:
            num_actions = 0  # Certains jours sans activité
        else:
            num_actions = random.randint(*profile['actions_per_day'])
        
        for _ in range(num_actions):
            # Générer un timestamp aléatoire dans la journée
            hour = random.randint(6, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            timestamp = f"{date} {hour:02d}:{minute:02d}:{second:02d}"
            
            # Choisir une action selon les poids du profil
            action = weighted_choice(profile['action_weights'])
            
            # Générer une durée (0 pour login/logout)
            if action in ['login', 'logout']:
                duration = 0
            else:
                duration = random.randint(*profile['duration_range'])
            
            user_data.append(f"U{user_id:03d},{timestamp},{action},{duration}")
    
    return user_data

def main():
    print("🔄 Génération des données de test...")
    
    # Répartition des profils utilisateurs
    profile_distribution = {
        'active': int(NUM_USERS * 0.3),      # 30% d'utilisateurs actifs
        'moderate': int(NUM_USERS * 0.5),    # 50% d'utilisateurs modérés
        'at_risk': NUM_USERS - int(NUM_USERS * 0.8)  # 20% d'utilisateurs à risque
    }
    
    all_data = []
    user_profiles = {}
    
    # Générer les données pour chaque utilisateur
    user_id = 1
    for profile_name, count in profile_distribution.items():
        for _ in range(count):
            user_data = generate_user_data(user_id, profile_name, NUM_DAYS)
            all_data.extend(user_data)
            user_profiles[f"U{user_id:03d}"] = profile_name
            user_id += 1
    
    # Mélanger les données pour simuler des logs réels
    random.shuffle(all_data)
    
    # Écrire le fichier de données
    output_file = BASE_DIR / "sample_input.txt"
    with open(output_file, "w") as f:
        # En-tête CSV
        f.write("user_id,timestamp,action,duration\n")
        # Données
        for line in all_data:
            f.write(line + "\n")
    
    # Créer un fichier de référence avec les profils
    profile_file = BASE_DIR / "user_profiles_reference.txt"
    with open(profile_file, "w") as f:
        f.write("user_id,profile,expected_risk\n")
        for user_id, profile in user_profiles.items():
            expected_risk = "RISK" if profile == "at_risk" else "OK"
            f.write(f"{user_id},{profile},{expected_risk}\n")
    
    print(f"✅ Fichier {output_file} généré avec {len(all_data)} lignes de données")
    print(f"✅ Fichier {profile_file} créé pour référence")
    print(f"📊 Répartition des utilisateurs:")
    for profile, count in profile_distribution.items():
        print(f"   - {profile}: {count} utilisateurs")
    
    print(f"\n🎯 Paramètres de détection utilisés:")
    print(f"   - Actions minimales: 5")
    print(f"   - Durée moyenne minimale: 2 minutes")
    print(f"   - Diversité d'actions minimale: 2 types")

if __name__ == "__main__":
    main()