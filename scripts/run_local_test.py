#!/usr/bin/env python3
"""
Script de test local pour le pipeline MapReduce de détection de churn
Permet de tester mapper et reducer sans Hadoop
"""

import os
import sys
import subprocess
import tempfile
from pathlib import Path

def run_local_test():
    """Exécute un test local du pipeline MapReduce"""
    
    # Chemins des scripts
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    mapper_path = project_dir / "mapper_reducer" / "mapper.py"
    reducer_path = project_dir / "mapper_reducer" / "reducer.py"
    data_dir = project_dir / "data"
    
    print("🧪 Test local du pipeline MapReduce de détection de churn")
    print(f"📁 Répertoire du projet: {project_dir}")
    
    # Vérifier que les fichiers existent
    if not mapper_path.exists():
        print(f"❌ Erreur: Mapper non trouvé à {mapper_path}")
        return False
        
    if not reducer_path.exists():
        print(f"❌ Erreur: Reducer non trouvé à {reducer_path}")
        return False
    
    # Générer des données de test si nécessaire
    test_data_path = data_dir / "sample_input.txt"
    if not test_data_path.exists():
        print("📊 Génération des données de test...")
        generate_script = data_dir / "generate_test_data.py"
        if generate_script.exists():
            subprocess.run([sys.executable, str(generate_script)], cwd=str(data_dir))
        else:
            print(f"❌ Script de génération non trouvé: {generate_script}")
            return False
    
    if not test_data_path.exists():
        print(f"❌ Fichier de données de test non trouvé: {test_data_path}")
        return False
    
    print(f"📄 Utilisation des données: {test_data_path}")
    
    # Étape 1: Exécuter le mapper
    print("🗺️  Exécution du mapper...")
    with open(test_data_path, 'r') as input_file:
        mapper_process = subprocess.Popen(
            [sys.executable, str(mapper_path)],
            stdin=input_file,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        mapper_output, mapper_error = mapper_process.communicate()
    
    if mapper_process.returncode != 0:
        print(f"❌ Erreur dans le mapper: {mapper_error}")
        return False
    
    print(f"✅ Mapper terminé - {len(mapper_output.splitlines())} lignes générées")
    
    # Étape 2: Trier les données (simulation du shuffle Hadoop)
    print("🔄 Tri des données intermédiaires...")
    sorted_data = '\n'.join(sorted(mapper_output.strip().split('\n')))
    
    # Étape 3: Exécuter le reducer
    print("📊 Exécution du reducer...")
    reducer_process = subprocess.Popen(
        [sys.executable, str(reducer_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    reducer_output, reducer_error = reducer_process.communicate(input=sorted_data)
    
    if reducer_process.returncode != 0:
        print(f"❌ Erreur dans le reducer: {reducer_error}")
        return False
    
    print(f"✅ Reducer terminé")
    
    # Analyser les résultats
    results = reducer_output.strip().split('\n')
    results = [line for line in results if line.strip()]
    
    risk_count = sum(1 for line in results if 'RISK' in line)
    ok_count = sum(1 for line in results if 'OK' in line)
    
    print("\n📈 Résultats de l'analyse:")
    print(f"   👥 Total des clients analysés: {len(results)}")
    print(f"   ⚠️  Clients à risque (RISK): {risk_count}")
    print(f"   ✅ Clients OK: {ok_count}")
    
    # Afficher quelques exemples
    print("\n🔍 Exemples de résultats:")
    for i, line in enumerate(results[:10]):
        print(f"   {line}")
    if len(results) > 10:
        print(f"   ... et {len(results) - 10} autres")
    
    # Sauvegarder les résultats
    output_file = project_dir / "local_test_results.txt"
    with open(output_file, 'w') as f:
        f.write(reducer_output)
    
    print(f"\n💾 Résultats sauvegardés dans: {output_file}")
    
    # Comparer avec les profils de référence si disponible
    reference_file = data_dir / "user_profiles_reference.txt"
    if reference_file.exists():
        print("\n🎯 Comparaison avec les profils de référence:")
        compare_with_reference(results, reference_file)
    
    return True

def compare_with_reference(results, reference_file):
    """Compare les résultats avec les profils de référence"""
    
    # Charger les résultats
    result_dict = {}
    for line in results:
        if '\t' in line:
            user_id, status = line.strip().split('\t')
            result_dict[user_id] = status
    
    # Charger la référence
    reference_dict = {}
    try:
        with open(reference_file, 'r') as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 3:
                    user_id, profile, expected = parts[0], parts[1], parts[2]
                    reference_dict[user_id] = expected
    except Exception as e:
        print(f"⚠️  Impossible de lire le fichier de référence: {e}")
        return
    
    # Comparer
    correct = 0
    total = 0
    for user_id, expected in reference_dict.items():
        if user_id in result_dict:
            actual = result_dict[user_id]
            if actual == expected:
                correct += 1
            total += 1
    
    if total > 0:
        accuracy = (correct / total) * 100
        print(f"   🎯 Précision: {correct}/{total} ({accuracy:.1f}%)")
    else:
        print("   ⚠️  Aucune correspondance trouvée")

if __name__ == "__main__":
    success = run_local_test()
    sys.exit(0 if success else 1)