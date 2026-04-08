# spark-app/main_base.py
# PySpark пайплайн для анализа поведения кредитных транзакций (базовая версия без оптимизаций)

import logging
import sys
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, month, when, lit,
    sqrt, pow, count, avg, stddev, sum, min, max, countDistinct,
    datediff, unix_timestamp, lag, expr, row_number
)
from pyspark.sql.window import Window
from shared_utils import setup_logging, get_optimal_partitions, save_metrics, StageTimer

logger = setup_logging('CreditCardPipeline-Base')

def preprocess_data(df, logger):
    """Создание временных признаков, категоризация сумм, расстояние между точками"""
    logger.info("Начало предобработки данных...")
    
    try:
        df = df.withColumn('trans_date_trans_time', to_timestamp(col('trans_date_trans_time')))
        df = df.withColumn('hour', hour(col('trans_date_trans_time')))
        df = df.withColumn('day_of_week', dayofweek(col('trans_date_trans_time')))
        df = df.withColumn('is_weekend', when(col('day_of_week').isin([1, 7]), 1).otherwise(0))
        df = df.withColumn('month', month(col('trans_date_trans_time')))

        df = df.withColumn('amount_bin',
            when(col('amt') <= 10, 'tiny')
            .when(col('amt') <= 50, 'small')
            .when(col('amt') <= 100, 'medium')
            .when(col('amt') <= 500, 'large')
            .otherwise('huge')
        )

        df = df.withColumn('geo_distance_km',
            sqrt(pow(col('lat') - col('merch_lat'), 2) + pow(col('long') - col('merch_long'), 2)) * 111
        )
        
        logger.info("Предобработка завершена")
        return df
    except Exception as e:
        logger.error(f"Ошибка при предобработке: {e}")
        raise

def compute_basic_stats(df, logger):
    """Расчет базовой статистики по категориям и часам работы"""
    logger.info("Начало вычисления базовой статистики...")
    
    try:
        category_stats = df.groupBy('category').agg(
            count('trans_num').alias('total_transactions'),
            countDistinct('cc_num').alias('unique_users'),
            avg('amt').alias('avg_amount')
        ).orderBy(col('total_transactions').desc())

        hourly_stats = df.groupBy('hour').agg(
            count('trans_num').alias('transaction_count'),
            avg('amt').alias('avg_amount'),
            countDistinct('cc_num').alias('unique_users')
        )
        
        top_category = category_stats.first()['category']
        logger.info(f"Топ-категория: {top_category}")
        return category_stats, hourly_stats
    except Exception as e:
        logger.error(f"Ошибка при вычислении базовой статистики: {e}")
        raise

def build_user_profiles(df, logger):
    """Построение профилей пользователей с метриками активности и географией"""
    logger.info("Начало построения пользовательских профилей...")
    
    try:
        user_profiles = df.groupBy('cc_num').agg(
            count('trans_num').alias('total_transactions'),
            sum('amt').alias('total_spent'),
            avg('amt').alias('avg_transaction'),
            stddev('amt').alias('std_transaction'),
            countDistinct('category').alias('unique_categories'),
            countDistinct('merchant').alias('unique_merchants'),
            min('trans_date_trans_time').alias('first_transaction'),
            max('trans_date_trans_time').alias('last_transaction'),
            avg('geo_distance_km').alias('avg_geo_distance')
        )

        user_profiles = user_profiles.withColumn('activity_span_days',
            datediff(col('last_transaction'), col('first_transaction'))
        )
        user_profiles = user_profiles.withColumn('transactions_per_day',
            col('total_transactions') / expr("CASE WHEN activity_span_days = 0 THEN 1 ELSE activity_span_days END")
        )

        # Пиковый час активности
        hour_counts = df.groupBy('cc_num', 'hour').count()
        window_spec = Window.partitionBy('cc_num').orderBy(col('count').desc())
        peak_hours = hour_counts.withColumn('rn', expr('row_number() over (partition by cc_num order by count desc)')) \
            .filter(col('rn') == 1) \
            .select('cc_num', col('hour').alias('peak_hour'))

        user_profiles = user_profiles.join(peak_hours, 'cc_num', 'left')

        # Гео-метрики
        geo_stats = df.groupBy('cc_num').agg(
            avg('geo_distance_km').alias('avg_distance'),
            max('geo_distance_km').alias('max_distance'),
            stddev('geo_distance_km').alias('std_distance')
        )
        user_profiles = user_profiles.join(geo_stats, 'cc_num', 'left')
        user_profiles = user_profiles.na.fill(0, subset=['std_transaction', 'std_distance'])

        user_count = user_profiles.count()
        logger.info(f"Профили построены для {user_count:,} пользователей")
        return user_profiles
    except Exception as e:
        logger.error(f"Ошибка при построении профилей: {e}")
        raise

def identify_interesting_users(user_profiles, df, logger):
    """Поиск аномальных пользователей по пяти критериям.
    
    Используются операции DataFrame вместо RDD.collect() для оптимальной производительности.
    Критерии: высокая частота, крупные чеки, географическая мобильность, 
    разнообразие категорий, всплески активности.
    """
    logger.info("Начало выявления интересных пользователей...")
    
    try:
        interesting_dfs = []

        # Критерий 1: Высокая частота транзакций (более 120 в день)
        high_freq = user_profiles.filter(col('transactions_per_day') > 120).select('cc_num')
        interesting_dfs.append(high_freq)
        logger.info(f"  Высокая частота: {high_freq.count():,} пользователей")

        # Критерий 2: Крупные чеки (выше 95-го перцентиля)
        threshold = user_profiles.approxQuantile('avg_transaction', [0.95], 0.0)[0]
        high_amount = user_profiles.filter(col('avg_transaction') > threshold).select('cc_num')
        interesting_dfs.append(high_amount)
        logger.info(f"  Высокие суммы (>{threshold:.0f}): {high_amount.count():,} пользователей")

        # Критерий 3: Высокая географическая мобильность (более 150 км)
        geo_mobile = user_profiles.filter(col('max_distance') > 150).select('cc_num')
        interesting_dfs.append(geo_mobile)
        logger.info(f"  Географическая мобильность: {geo_mobile.count():,} пользователей")

        # Критерий 4: Высокое разнообразие категорий (10 и более)
        diverse = user_profiles.filter(col('unique_categories') >= 10).select('cc_num')
        interesting_dfs.append(diverse)
        logger.info(f"  Разнообразие категорий: {diverse.count():,} пользователей")

        # Критерий 5: Всплески активности (5+ транзакций менее чем за 10 минут)
        window_time = Window.partitionBy('cc_num').orderBy('trans_date_trans_time')
        df_sorted = df.withColumn('time_diff', 
            unix_timestamp(col('trans_date_trans_time')) - unix_timestamp(lag(col('trans_date_trans_time'), 1).over(window_time))
        )
        rapid_tx = df_sorted.filter(col('time_diff') < 600)
        burst_counts = rapid_tx.groupBy('cc_num').count().filter(col('count') >= 5).select('cc_num')
        interesting_dfs.append(burst_counts)
        logger.info(f"  Всплески активности: {burst_counts.count():,} пользователей")

        # Объединение всех критериев и поиск уникальных пользователей
        from functools import reduce
        combined = reduce(lambda d1, d2: d1.union(d2), interesting_dfs)
        unique_interesting = combined.distinct()
        unique_count = unique_interesting.count()
        
        percentage = (unique_count / user_profiles.count() * 100) if user_profiles.count() > 0 else 0
        logger.info(f"Найдено {unique_count:,} уникальных интересных пользователей ({percentage:.2f}%)")
        return unique_interesting
    except Exception as e:
        logger.error(f"Ошибка при выявлении интересных пользователей: {e}")
        raise

def main():
    start_time = time.time()
    logger.info("Запуск Spark-приложения (базовая версия без оптимизаций)")
    
    # Инициализация таймера стадий
    stage_timer = StageTimer(logger)

    spark = None
    try:
        # Определение количества узлов для конфигурации 
        num_datanodes = int(os.getenv('DATANODE_COUNT', '1'))
        
        # Адаптивная конфигурация Spark в зависимости от кластера
        config_builder = SparkSession.builder \
            .appName("CreditCardAnalytics-Base") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Для 3DN увеличиваем параллелизм
        if num_datanodes >= 3:
            config_builder = config_builder \
                .config("spark.sql.shuffle.partitions", "12") \
                .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "50m") \
                .config("spark.shuffle.reduce.bypassMergeThreshold", "200")
            logger.info(f"Конфигурация для {num_datanodes} узлов (увеличенный параллелизм)")
        else:
            logger.info("Конфигурация для 1 узла (стандартный параллелизм)")
        
        spark = config_builder.getOrCreate()
        logger.info("Spark инициализирована")

        # Стадия 1: Загрузка данных
        stage_timer.start_stage("Загрузка данных")
        hdfs_path = "hdfs://hdfs-master:9000/data/credit-fraud/train.csv"
        logger.info(f"Загрузка данных из {hdfs_path}...")
        
        try:
            df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        except Exception as e:
            logger.error(f"Ошибка загрузки из HDFS: {e}")
            logger.info("Попытка загрузить из локального пути...")
            df = spark.read.csv("/app/data/train.csv", header=True, inferSchema=True)
        
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(f"Данные загружены: {row_count:,} строк, {col_count} столбцов")
        stage_timer.end_stage()

        # Стадия 2: Предобработка данных
        stage_timer.start_stage("Предобработка данных")
        df = preprocess_data(df, logger)
        stage_timer.end_stage()

        # Стадия 3: Вычисление базовой статистики
        stage_timer.start_stage("Вычисление базовой статистики")
        compute_basic_stats(df, logger)
        stage_timer.end_stage()

        # Стадия 4: Построение профилей пользователей
        stage_timer.start_stage("Построение профилей пользователей")
        user_profiles = build_user_profiles(df, logger)
        stage_timer.end_stage()

        # Стадия 5: Выявление интересных пользователей
        stage_timer.start_stage("Выявление интересных пользователей")
        identify_interesting_users(user_profiles, df, logger)
        stage_timer.end_stage()

        # Замер общего времени выполнения
        elapsed = time.time() - start_time
        logger.info(f"Общее время выполнения: {elapsed:.2f} сек")

        # Сохранение метрик эксперимента с информацией о стадиях
        exp_name = os.getenv('EXP_NAME', 'exp_1dn_base')
        stages = stage_timer.get_stages()
        save_metrics(spark, df, elapsed, False, logger, exp_name, stages)

        logger.info("Приложение завершено успешно")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession закрыта")

if __name__ == "__main__":
    main()
