#!/bin/bash
# Запуск всех 4 экспериментов последовательно: базовая и оптимизированная версии на 1 и 3 узлах

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Массив параметров: (конфиг, флаг_оптимизации)
experiments=(
  "1dn|0|main_base"
  "1dn|1|main_optimized"
  "3dn|0|main_base"
  "3dn|1|main_optimized"
)

exp_names=(
  "exp_1dn_base"
  "exp_1dn_optimized"
  "exp_3dn_base"
  "exp_3dn_optimized"
)

echo "===================================================="
echo "Запуск всех 4 экспериментов"
echo "===================================================="
echo ""

for i in "${!experiments[@]}"; do
  IFS='|' read -r config optimize app_version <<< "${experiments[$i]}"
  exp_name="${exp_names[$i]}"
  
  echo ""
  echo "===================================================="
  echo "Эксперимент $((i+1))/4: $exp_name"
  echo "КОНФИГ: $config | ОПТИМИЗАЦИЯ: $optimize | ПРИЛОЖЕНИЕ: $app_version.py"
  echo "===================================================="
  
  # ШАГ 1: Развертывание кластера
  echo ""
  echo "ШАГ 1: Развертывание кластера..."
  cd "$PROJECT_ROOT"
  bash "$SCRIPT_DIR/deploy.sh" "$config" "$optimize" || {
    echo "Ошибка при развертывании!"
    exit 1
  }
  
  # ШАГ 2: Запуск Spark приложения
  echo ""
  echo "ШАГ 2: Запуск Spark приложения ($app_version.py)..."
  
  if [ "$config" = "1dn" ]; then
    datanode_count=1
  else
    datanode_count=3
  fi
  
  # Определение запускаемого приложения
  if [ "$optimize" = "1" ]; then
    app_script="main_optimized"
  else
    app_script="main_base"
  fi
  
  # Отправка Spark задачи
  echo "Отправка задачи на Spark..."
  max_attempts=5
  attempt=1
  
  while [ $attempt -le $max_attempts ]; do
    if docker exec \
      -e EXP_NAME="$exp_name" \
      -e DATANODE_COUNT="$datanode_count" \
      spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1g \
      --executor-memory 1g \
      --py-files /app/code/shared_utils.py \
      /app/code/${app_script}.py \
      2>&1 | tee "$PROJECT_ROOT/experiments/$exp_name/results/spark.log"; then
      echo "Spark приложение завершено успешно"
      break
    else
      echo "Попытка $attempt/$max_attempts не прошла"
      if [ $attempt -lt $max_attempts ]; then
        echo "Ожидание 10 сек перед повтором..."
        sleep 10
      fi
    fi
    attempt=$((attempt + 1))
  done
  
  if [ $attempt -gt $max_attempts ]; then
    echo "Spark приложение не запустилось после $max_attempts попыток"
    exit 1
  fi
  
  # ШАГ 3: Сбор метрик
  echo ""
  echo "ШАГ 3: Сбор метрик..."
  bash "$SCRIPT_DIR/collect_metrics.sh" "$exp_name" || {
    echo "Предупреждение: ошибка при сборе метрик (продолжаем)"
  }
  
  # ШАГ 4: Остановка и очистка контейнеров
  echo ""
  echo "ШАГ 4: Остановка и очистка контейнеров..."
  if [ "$config" = "1dn" ]; then
    docker-compose -f docker-compose.yml down -v 2>/dev/null || true
  else
    docker-compose -f docker-compose.yml -f docker-compose.3dn.yml down -v 2>/dev/null || true
  fi
  
  # Очистка Docker сети для избежания конфликтов
  sleep 2
  docker network prune -f 2>/dev/null || true
  echo "Контейнеры остановлены"
  
  # Охлаждение между экспериментами (кроме последнего)
  if [ $((i+1)) -lt ${#experiments[@]} ]; then
    echo "Охлаждение 30 сек перед следующим экспериментом..."
    sleep 30
  fi
done

echo ""
echo "===================================================="
echo "ВСЕ ЭКСПЕРИМЕНТЫ ЗАВЕРШЕНЫ УСПЕШНО"
echo "===================================================="

# ШАГ 5: Сравнение результатов
echo ""
echo "Сравнение результатов..."
if [ -f "$SCRIPT_DIR/compare_results.py" ]; then
  python3 "$SCRIPT_DIR/compare_results.py" || {
    echo "Предупреждение: ошибка при сравнении результатов"
  }
else
  echo "Предупреждение: compare_results.py не найден"
fi

echo ""
echo "===================================================="
echo "ПОЛНЫЙ ЦИКЛ ЭКСПЕРИМЕНТОВ ЗАВЕРШЕН"
echo "Результаты в: $PROJECT_ROOT/results/"
echo "===================================================="
