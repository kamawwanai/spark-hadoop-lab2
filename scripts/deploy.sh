#!/bin/bash
# Развертывание Spark + Hadoop кластера и загрузка данных в HDFS

set -e

CONFIG=${1:-1dn}
OPTIMIZE=${2:-0}
EXP_NAMES=("exp_1dn_base" "exp_1dn_optimized" "exp_3dn_base" "exp_3dn_optimized")

if [ "$CONFIG" = "1dn" ]; then
  EXP_IDX=$((OPTIMIZE))
  DATANODE_COUNT=1
else
  EXP_IDX=$((2 + OPTIMIZE))
  DATANODE_COUNT=3
fi

EXP_NAME="${EXP_NAMES[$EXP_IDX]}"
echo "======================================"
echo "Развертывание: $EXP_NAME"
echo "КОНФИГ: $CONFIG | ОПТИМИЗАЦИЯ: $OPTIMIZE"
echo "======================================"

# ВАЖНО: Полная очистка всех старых контейнеров
echo "Полная очистка старых контейнеров..."
docker-compose -f docker-compose.yml -f docker-compose.3dn.yml down -v 2>/dev/null || true
sleep 2
docker network prune -f 2>/dev/null || true

echo "Запуск Docker контейнеров..."
if [ "$CONFIG" = "1dn" ]; then
  docker-compose -f docker-compose.yml up -d
else
  docker-compose -f docker-compose.yml -f docker-compose.3dn.yml up -d
fi

echo "Ожидание готовности HDFS (до 60 сек)..."
for i in {1..60}; do
  if curl -s http://localhost:9870 > /dev/null 2>&1; then
    echo "HDFS готов"
    break
  fi
  echo "  попытка $i/60..."
  sleep 1
done

echo "Загрузка данных в HDFS..."
if [ ! -f "./data/train.csv" ]; then
  echo "Ошибка: файл ./data/train.csv не найден!"
  exit 1
fi

docker exec hdfs-master hdfs dfs -rm -r /data/credit-fraud 2>/dev/null || true
docker exec hdfs-master hdfs dfs -mkdir -p /data/credit-fraud
docker cp ./data/train.csv hdfs-master:/tmp/train.csv 2>/dev/null
docker exec hdfs-master hdfs dfs -put /tmp/train.csv /data/credit-fraud/

echo "Данные загружены в hdfs://hdfs-master:9000/data/credit-fraud/"

echo "Ожидание готовности Spark Master (до 30 сек)..."
for i in {1..30}; do
  if curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo "Spark Master готов"
    break
  fi
  echo "  попытка $i/30..."
  sleep 1
done

echo ""
echo "Развертывание завершено!"
echo "======================================"
echo "HDFS NameNode: http://localhost:9870"
echo "Spark Master: http://localhost:8080"
echo "HDFS DataNodes: $DATANODE_COUNT"
echo "======================================"