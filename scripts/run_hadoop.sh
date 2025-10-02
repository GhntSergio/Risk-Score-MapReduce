#!/bin/bash
# --------------------------------------------------------
# Script d'exécution du job Hadoop Streaming (détection de churn)
# Usage: ./run_hadoop.sh [input_path_hdfs] [output_path_hdfs]
# --------------------------------------------------------

set -e  # Arrêt en cas d'erreur

# Configuration par défaut
DEFAULT_INPUT="/user/input/churn_data"
DEFAULT_OUTPUT="/user/output/churn_results"
HADOOP_STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"

# Paramètres (priorité aux arguments passés au script)
INPUT_PATH=${1:-$DEFAULT_INPUT}
OUTPUT_PATH=${2:-$DEFAULT_OUTPUT}

# Répertoires des scripts (montés depuis l’hôte dans le conteneur)
SCRIPT_DIR="/scripts"
MAPPER_PATH="$SCRIPT_DIR/mapper.py"
REDUCER_PATH="$SCRIPT_DIR/reducer.py"

echo "🚀 Lancement du job Hadoop Streaming"
echo "📁 Input HDFS: $INPUT_PATH"
echo "📁 Output HDFS: $OUTPUT_PATH"
echo "🗂️ Mapper: $MAPPER_PATH"
echo "🗂️ Reducer: $REDUCER_PATH"

# Vérification des fichiers
[ -f "$MAPPER_PATH" ] || { echo "❌ Mapper introuvable: $MAPPER_PATH"; exit 1; }
[ -f "$REDUCER_PATH" ] || { echo "❌ Reducer introuvable: $REDUCER_PATH"; exit 1; }

# Vérification Hadoop
[ -n "$HADOOP_HOME" ] || { echo "❌ Erreur: HADOOP_HOME non défini"; exit 1; }
[ -f "$HADOOP_STREAMING_JAR" ] || { echo "❌ Erreur: jar Hadoop Streaming introuvable"; exit 1; }

echo "📦 JAR Streaming: $HADOOP_STREAMING_JAR"

# Nettoyage sortie
echo "🧹 Suppression de l’ancienne sortie (si existe)..."
hdfs dfs -rm -r -f -skipTrash "$OUTPUT_PATH" || true

# Rendre exécutables
chmod +x "$MAPPER_PATH" "$REDUCER_PATH"

echo "⚡ Exécution du job..."

# Lancement du job Hadoop Streaming
hadoop jar "$HADOOP_STREAMING_JAR" \
    -files "$MAPPER_PATH","$REDUCER_PATH" \
    -mapper "./mapper.py" \
    -reducer "./reducer.py" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH" \
    -cmdenv PYTHONIOENCODING=utf-8

# Vérification du succès
if [ $? -eq 0 ]; then
    echo "✅ Job terminé avec succès"
    echo "📊 Résultats (aperçu 20 lignes) :"
    hdfs dfs -cat "$OUTPUT_PATH/part-00000" | head -20

    echo ""
    echo "📈 Statistiques globales :"
    hdfs dfs -cat "$OUTPUT_PATH/part-*" | cut -f2 | sort | uniq -c
else
    echo "❌ Échec du job MapReduce"
    exit 1
fi

echo ""
echo "🎯 Pour rapatrier les résultats localement :"
echo "hdfs dfs -get $OUTPUT_PATH ./results"
