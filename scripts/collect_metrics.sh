#!/bin/bash
# Сбор метрик из завершенного эксперимента

set -e

EXP_NAME=${1:-exp_1dn_base}
RESULTS_DIR="./experiments/$EXP_NAME/results"

echo "======================================"
echo "Сбор метрик: $EXP_NAME"
echo "======================================"

# Создание директории если её нет
mkdir -p "$RESULTS_DIR"

# Копирование метрик из контейнера
echo "Копирование метрик из контейнера..."
if docker ps | grep -q spark-master; then
  docker cp spark-master:/app/results/metrics_${EXP_NAME}.json "$RESULTS_DIR/" 2>/dev/null || \
  docker cp spark-master:/app/results/metrics.txt "$RESULTS_DIR/metrics_raw.txt" 2>/dev/null || true
else
  echo "Предупреждение: контейнер spark-master не запущен"
fi

# Копирование логов если они доступны
echo "Копирование логов из контейнера..."
docker logs spark-master > "$RESULTS_DIR/spark-master.log" 2>&1 || true
docker logs spark-worker > "$RESULTS_DIR/spark-worker.log" 2>&1 || true

# Проверка наличия JSON метрик
if [ -f "$RESULTS_DIR/metrics_${EXP_NAME}.json" ]; then
  echo "JSON метрики найдены: $RESULTS_DIR/metrics_${EXP_NAME}.json"
else
  echo "Внимание: JSON метрики не найдены"
  
  # Если есть raw метрики, копируем их в summary
  if [ -f "$RESULTS_DIR/metrics_raw.txt" ]; then
    cat "$RESULTS_DIR/metrics_raw.txt" > "$RESULTS_DIR/metrics_summary.txt"
  fi
fi

# Вывод резюме
echo ""
echo "Сбор метрик завершен!"
echo "======================================"
echo "Директория: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/" 2>/dev/null || echo "(директория пуста)"
echo "======================================"