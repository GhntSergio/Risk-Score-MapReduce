#!/usr/bin/env python3
"""
Reducer pour la détection de churn clients
Agrège les données par utilisateur et calcule le score de risque
Format d'entrée: user_id\t1,duration,action
Format de sortie: user_id\tOK|RISK
"""

import sys

# Paramètres de détection de risque (configurables)
MIN_ACTIONS = 5          # Minimum d'actions pour ne pas être à risque
MIN_AVG_DURATION = 2     # Durée moyenne minimale en minutes
MIN_ACTION_DIVERSITY = 2 # Nombre minimum de types d'actions différentes


def calculate_risk_score(total_actions, avg_duration, action_diversity):
    """
    Calcule si un utilisateur est à risque de churn.
    Retourne 'RISK' si au moins un critère n'est pas respecté, sinon 'OK'
    """
    if (total_actions < MIN_ACTIONS or 
        avg_duration < MIN_AVG_DURATION or 
        action_diversity < MIN_ACTION_DIVERSITY):
        return "RISK"
    return "OK"


def main():
    current_user = None
    user_data = {
        'total_actions': 0,
        'total_duration': 0,
        'actions_set': set()
    }

    # Lire ligne par ligne depuis stdin
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            # Parser la ligne: user_id\t1,duration,action
            user_id, data = line.split('\t', 1)
            count, duration, action = data.split(',', 2)

            count = int(count)
            duration = int(duration)

            # Si on change d'utilisateur, traiter le précédent
            if current_user and current_user != user_id:
                avg_duration = (user_data['total_duration'] / user_data['total_actions'] / 60.0 
                                if user_data['total_actions'] > 0 else 0)
                action_diversity = len(user_data['actions_set'])

                # Déterminer le statut de risque
                risk_status = calculate_risk_score(
                    user_data['total_actions'], 
                    avg_duration, 
                    action_diversity
                )

                # Émettre le résultat
                print(f"{current_user}\t{risk_status}")

                # Réinitialiser pour le nouvel utilisateur
                user_data = {
                    'total_actions': 0,
                    'total_duration': 0,
                    'actions_set': set()
                }

            # Mettre à jour les données pour l'utilisateur courant
            current_user = user_id
            user_data['total_actions'] += count
            user_data['total_duration'] += duration
            user_data['actions_set'].add(action)

        except (ValueError, IndexError):
            # Ignorer les lignes malformées
            continue

    # Traiter le dernier utilisateur
    if current_user:
        avg_duration = (user_data['total_duration'] / user_data['total_actions'] / 60.0 
                        if user_data['total_actions'] > 0 else 0)
        action_diversity = len(user_data['actions_set'])

        risk_status = calculate_risk_score(
            user_data['total_actions'], 
            avg_duration, 
            action_diversity
        )

        print(f"{current_user}\t{risk_status}")


if __name__ == "__main__":
    main()
