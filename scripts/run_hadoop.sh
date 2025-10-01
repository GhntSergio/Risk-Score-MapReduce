#!/bin/bash

# Script d'exécution du job Hadoop Streaming pour la détection de churn
# Usage: ./run_hadoop.sh [input_path] [output_path]

set -e  # Arrêter en cas d'erreur

# Configuration par défaut
DEFAULT_INPUT="/user/input/churn_data"
DEFAULT_OUTPUT="/user/output/churn_results"
HADOOP_STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"

# Paramètres
INPUT_PATH=${1:-$DEFAULT_INPUT}
OUTPUT_PATH=${2:-$DEFAULT_OUTPUT}

# Chemins des scripts
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MAPPER_PATH="$PROJECT_DIR/mapper_reducer/mapper.py"
REDUCER_PATH="$PROJECT_DIR/mapper_reducer/reducer.py"

echo "🚀 Lancement du job Hadoop Streaming pour la détection de churn"
echo "📁 Répertoire d'entrée: $INPUT_PATH"
echo "📁 Répertoire de sortie: $OUTPUT_PATH"
echo "🗂️ Mapper: $MAPPER_PATH"
echo "🗂️ Reducer: $REDUCER_PATH"

# Vérifier que les fichiers existent
if [ ! -f "$MAPPER_PATH" ]; then
    echo "❌ Erreur: Mapper non trouvé à $MAPPER_PATH"
    exit 1
fi

if [ ! -f "$REDUCER_PATH" ]; then
    echo "❌ Erreur: Reducer non trouvé à $REDUCER_PATH"
    exit 1
fi

# Vérifier que Hadoop est configuré
if [ -z "$HADOOP_HOME" ]; then
    echo "❌ Erreur: HADOOP_HOME n'est pas défini"
    exit 1
fi

# Trouver le JAR Hadoop Streaming
STREAMING_JAR=$(ls $HADOOP_STREAMING_JAR 2>/dev/null | head -1)
if [ -z "$STREAMING_JAR" ]; then
    echo "❌ Erreur: JAR Hadoop Streaming non trouvé dans $HADOOP_STREAMING_JAR"
    exit 1
fi

echo "📦 JAR Streaming: $STREAMING_JAR"

# Supprimer le répertoire de sortie s'il existe
echo "🧹 Nettoyage du répertoire de sortie..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"

# Rendre les scripts exécutables
chmod +x "$MAPPER_PATH"
chmod +x "$REDUCER_PATH"

echo "⚡ Exécution du job MapReduce..."

# Lancer le job Hadoop Streaming
hadoop jar "$STREAMING_JAR" \
    -files "$MAPPER_PATH","$REDUCER_PATH" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH" \
    -cmdenv PYTHONIOENCODING=utf-8

# Vérifier le succès
if [ $? -eq 0 ]; then
    echo "✅ Job terminé avec succès!"
    echo "📊 Résultats disponibles dans: $OUTPUT_PATH"
    
    echo "🔍 Aperçu des résultats:"
    hdfs dfs -cat "$OUTPUT_PATH/part-00000" | head -20
    
    echo ""
    echo "📈 Statistiques:"
    echo "Nombre total de clients analysés:"
    hdfs dfs -cat "$OUTPUT_PATH/part-*" | wc -l
    
    echo "Clients à risque (RISK):"
    hdfs dfs -cat "$OUTPUT_PATH/part-*" | grep -c "RISK" || echo "0"
    
    echo "Clients OK:"
    hdfs dfs -cat "$OUTPUT_PATH/part-*" | grep -c "OK" || echo "0"
    
else
    echo "❌ Échec du job MapReduce"
    exit 1
fi

echo ""
echo "🎯 Pour récupérer tous les résultats:"
echo "hdfs dfs -get $OUTPUT_PATH ./results"
echo ""
echo "🔄 Pour relancer avec d'autres paramètres:"
echo "$0 <chemin_entrée> <chemin_sortie>"