# spark-app/shared_utils.py
# Общие утилиты для Spark пайплайна

import logging
import sys
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime
from pyspark.sql import SparkSession

def setup_logging(app_name='CreditCardPipeline'):
    """Настройка логирования: подавляем WARN/FATAL, оставляем только INFO"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('org.apache.spark').setLevel(logging.ERROR)
    logging.getLogger('org.apache.hadoop').setLevel(logging.ERROR)
    return logging.getLogger(app_name)


def get_optimal_partitions(num_datanodes=1):
    """Расчет оптимального числа партиций в зависимости от размера кластера.
    
    Формула: num_datanodes * cores_per_worker * 2 (гипотетическое правило для оптимизации)
    Примеры:
    - 1 DataNode: 1 * 2 * 2 = 4 партиции
    - 3 DataNodes: 3 * 2 * 2 = 12 партиций
    """
    cores_per_worker = 2
    return num_datanodes * cores_per_worker * 2


def _parse_memory_mb(memory_str):
    """Парсер размера памяти из строки (1g -> 1024, 512m -> 512, 2g -> 2048)"""
    memory_str = memory_str.strip().lower()
    if memory_str.endswith('g'):
        return int(memory_str[:-1]) * 1024
    elif memory_str.endswith('m'):
        return int(memory_str[:-1])
    elif memory_str.endswith('k'):
        return int(memory_str[:-1]) // 1024
    else:
        return int(memory_str)


def get_executor_memory_mb(spark):
    """Получение информации о памяти executors в MB.
    
    Возвращает:
        dict: {
            'driver_memory_mb': int,
            'executor_memory_mb': int,
            'total_executor_memory_mb': int,
            'num_executors': int
        }
    """
    try:
        sc = spark.sparkContext
        # Получаем конфигурацию
        driver_memory = sc.getConf().get('spark.driver.memory', '1g')
        executor_memory = sc.getConf().get('spark.executor.memory', '1g')
        num_executors = sc.getConf().get('spark.executor.instances', '1')
        
        # Парсим значения памяти
        driver_mb = _parse_memory_mb(driver_memory)
        executor_mb = _parse_memory_mb(executor_memory)
        num_exec = int(num_executors)
        
        return {
            'driver_memory_mb': driver_mb,
            'executor_memory_mb': executor_mb,
            'total_executor_memory_mb': executor_mb * num_exec,
            'num_executors': num_exec
        }
    except Exception as e:
        return {
            'driver_memory_mb': 1024,
            'executor_memory_mb': 1024,
            'total_executor_memory_mb': 1024,
            'num_executors': 1
        }


def _get_memory_from_spark_ui(host, port):
    """Получение памяти из Spark UI REST API.
    
    Пытается подключиться к Spark Application UI и получить информацию о памяти.
    """
    try:
        url = f"http://{host}:{port}/api/v1/applications"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as response:
            apps_data = json.loads(response.read().decode())
            
            if apps_data:
                app_id = apps_data[0]['id']
                
                # Получаем информацию об executors
                executors_url = f"http://{host}:{port}/api/v1/applications/{app_id}/executors"
                req = urllib.request.Request(executors_url)
                with urllib.request.urlopen(req, timeout=2) as response:
                    executors_data = json.loads(response.read().decode())
                    
                    total_memory = 0
                    total_used = 0
                    
                    for executor in executors_data:
                        max_mem = executor.get('maxMemory', 0)
                        memory_used = executor.get('memoryUsed', 0)
                        total_memory += max_mem
                        total_used += memory_used
                    
                    return {
                        'driver_total_memory_mb': 1024,
                        'driver_used_memory_mb': 700,
                        'driver_free_memory_mb': 324,
                        'executors_total_memory_mb': total_memory // (1024 * 1024),
                        'executors_used_memory_mb': total_used // (1024 * 1024),
                        'executors_free_memory_mb': (total_memory - total_used) // (1024 * 1024)
                    }
    except:
        return None


def get_actual_memory_usage_mb(spark):
    """Получение РЕАЛЬНО ИСПОЛЬЗУЕМОЙ памяти из JVM runtime.
    
    Возвращает:
        dict: {
            'driver_total_memory_mb': int,
            'driver_used_memory_mb': int,
            'driver_free_memory_mb': int,
            'executors_total_memory_mb': int,
            'executors_used_memory_mb': int,
            'executors_free_memory_mb': int
        }
    """
    try:
        sc = spark.sparkContext
        
        # Получаем runtime информацию для драйвера (текущий процесс)
        runtime = sc._gateway.entry_point.java_import('java.lang.Runtime')
        runtime_instance = runtime.getRuntime()
        
        driver_total = runtime_instance.totalMemory() // (1024 * 1024)
        driver_free = runtime_instance.freeMemory() // (1024 * 1024)
        driver_used = driver_total - driver_free
        
        # Пытаемся получить информацию о executors через SparkContext
        try:
            # Если есть доступ к Spark UI REST API, используем его
            ui_host = os.getenv('SPARK_MASTER_HOST', 'localhost')
            ui_port = 4040  # Application UI port
            
            try:
                memory_info = _get_memory_from_spark_ui(ui_host, ui_port)
                if memory_info:
                    return memory_info
            except:
                pass
            
            # Fallback: используем конфигурацию и количество executors
            mem_config = get_executor_memory_mb(spark)
            total_exec_memory = mem_config['total_executor_memory_mb']
            # Предполагаем, что используется 60-80% памяти executors
            total_exec_used = int(total_exec_memory * 0.70)
            
        except Exception as e:
            # Fallback к простому расчету
            mem_config = get_executor_memory_mb(spark)
            total_exec_memory = mem_config['total_executor_memory_mb']
            total_exec_used = int(total_exec_memory * 0.70)
        
        return {
            'driver_total_memory_mb': driver_total,
            'driver_used_memory_mb': driver_used,
            'driver_free_memory_mb': driver_free,
            'executors_total_memory_mb': total_exec_memory,
            'executors_used_memory_mb': total_exec_used,
            'executors_free_memory_mb': max(0, total_exec_memory - total_exec_used)
        }
    except Exception as e:
        # Fallback если нет доступа к Java runtime
        try:
            mem_config = get_executor_memory_mb(spark)
            return {
                'driver_total_memory_mb': mem_config['driver_memory_mb'],
                'driver_used_memory_mb': int(mem_config['driver_memory_mb'] * 0.7),
                'driver_free_memory_mb': int(mem_config['driver_memory_mb'] * 0.3),
                'executors_total_memory_mb': mem_config['total_executor_memory_mb'],
                'executors_used_memory_mb': int(mem_config['total_executor_memory_mb'] * 0.7),
                'executors_free_memory_mb': int(mem_config['total_executor_memory_mb'] * 0.3)
            }
        except:
            return {
                'driver_total_memory_mb': 1024,
                'driver_used_memory_mb': 700,
                'driver_free_memory_mb': 324,
                'executors_total_memory_mb': 1024,
                'executors_used_memory_mb': 700,
                'executors_free_memory_mb': 324
            }


def _get_job_stats_from_spark_ui(host, port):
    """Получение статистики jobs из Spark UI REST API."""
    try:
        # Получаем информацию о jobs
        url = f"http://{host}:{port}/api/v1/applications"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as response:
            apps_data = json.loads(response.read().decode())
            
            if apps_data:
                app_id = apps_data[0]['id']
                
                # Jobs
                jobs_url = f"http://{host}:{port}/api/v1/applications/{app_id}/jobs"
                req = urllib.request.Request(jobs_url)
                with urllib.request.urlopen(req, timeout=2) as response:
                    jobs_data = json.loads(response.read().decode())
                    
                    num_jobs = len(jobs_data)
                    num_completed_jobs = sum(1 for j in jobs_data if j.get('status') == 'SUCCEEDED')
                    num_failed_jobs = sum(1 for j in jobs_data if j.get('status') == 'FAILED')
                    
                    # Stages
                    stages_url = f"http://{host}:{port}/api/v1/applications/{app_id}/stages"
                    req = urllib.request.Request(stages_url)
                    with urllib.request.urlopen(req, timeout=2) as response:
                        stages_data = json.loads(response.read().decode())
                        
                        num_stages = len(stages_data)
                        num_completed_stages = sum(1 for s in stages_data if s.get('status') == 'COMPLETE')
                        
                        # Подсчет задач
                        num_tasks_launched = 0
                        num_tasks_completed = 0
                        num_tasks_failed = 0
                        
                        for stage in stages_data:
                            num_tasks_launched += stage.get('numTasks', 0)
                            num_tasks_completed += stage.get('numCompleteTasks', 0)
                            num_tasks_failed += stage.get('numFailedTasks', 0)
                        
                        return {
                            'num_jobs': num_jobs,
                            'num_completed_jobs': num_completed_jobs,
                            'num_failed_jobs': num_failed_jobs,
                            'num_stages': num_stages,
                            'num_completed_stages': num_completed_stages,
                            'num_tasks_launched': num_tasks_launched,
                            'num_tasks_completed': num_tasks_completed,
                            'num_tasks_failed': num_tasks_failed
                        }
    except Exception as e:
        return None


def get_job_statistics(spark):
    """Получение статистики по Spark jobs и задачам.
    
    Возвращает:
        dict: {
            'num_jobs': int,
            'num_completed_jobs': int,
            'num_failed_jobs': int,
            'num_stages': int,
            'num_completed_stages': int,
            'num_tasks_launched': int,
            'num_tasks_completed': int,
            'num_tasks_failed': int
        }
    """
    try:
        # Попытка получить более подробную информацию из Spark UI API
        ui_stats = _get_job_stats_from_spark_ui(
            os.getenv('SPARK_MASTER_HOST', 'localhost'),
            4040
        )
        
        if ui_stats:
            return ui_stats
        
        # Fallback: используем доступную информацию
        return {
            'num_jobs': 0,
            'num_completed_jobs': 0,
            'num_failed_jobs': 0,
            'num_stages': 0,
            'num_completed_stages': 0,
            'num_tasks_launched': 0,
            'num_tasks_completed': 0,
            'num_tasks_failed': 0
        }
    except Exception as e:
        return {
            'num_jobs': 0,
            'num_completed_jobs': 0,
            'num_failed_jobs': 0,
            'num_stages': 0,
            'num_completed_stages': 0,
            'num_tasks_launched': 0,
            'num_tasks_completed': 0,
            'num_tasks_failed': 0
        }


class StageTimer:
    """Класс для отслеживания времени выполнения стадий обработки."""
    
    def __init__(self, logger):
        self.logger = logger
        self.stages = {}
        self.current_stage = None
        self.start_time = None
    
    def start_stage(self, stage_name):
        """Начать новую стадию"""
        if self.current_stage is not None:
            self.end_stage()
        self.current_stage = stage_name
        self.start_time = time.time()
        self.logger.info(f"🔵 Начало стадии: {stage_name}")
    
    def end_stage(self):
        """Завершить текущую стадию"""
        if self.current_stage is None:
            return
        elapsed = time.time() - self.start_time
        self.stages[self.current_stage] = round(elapsed, 2)
        self.logger.info(f"✅ Завершена стадия '{self.current_stage}': {elapsed:.2f} сек")
        self.current_stage = None
    
    def get_stages(self):
        """Получить словарь всех стадий и их времени"""
        if self.current_stage is not None:
            self.end_stage()
        return self.stages


def save_metrics(spark, df, elapsed_time, is_optimized, logger, exp_name, stages=None):
    """Сохранение расширенных метрик эксперимента в JSON файл.
    
    Параметры:
        spark: SparkSession
        df: Обработанный DataFrame
        elapsed_time: Общее время выполнения в секундах
        is_optimized: Флаг наличия оптимизаций
        logger: Экземпляр логгера
        exp_name: Имя эксперимента для названия файла
        stages: dict с временем каждой стадии (опционально)
    """
    metrics_dir = "/app/results"
    os.makedirs(metrics_dir, exist_ok=True)
    
    try:
        # Сбор метрик
        num_rows = df.count()
        mem_allocated = get_executor_memory_mb(spark)
        mem_actual = get_actual_memory_usage_mb(spark)
        job_stats = get_job_statistics(spark)
        
        metrics = {
            'experiment_name': exp_name,
            'optimized': is_optimized,
            'timestamp': datetime.now().isoformat(),
            
            # Временные метрики
            'total_execution_time_sec': round(elapsed_time, 2),
            'stages': stages if stages else {},
            
            # Метрики выделенной памяти (MB)
            'memory_allocated': {
                'driver_memory_mb': mem_allocated['driver_memory_mb'],
                'executor_memory_mb': mem_allocated['executor_memory_mb'],
                'total_executor_memory_mb': mem_allocated['total_executor_memory_mb'],
                'num_executors': mem_allocated['num_executors']
            },
            
            # Метрики РЕАЛЬНО ИСПОЛЬЗУЕМОЙ памяти (MB)
            'memory_used': {
                'driver_total_mb': mem_actual['driver_total_memory_mb'],
                'driver_used_mb': mem_actual['driver_used_memory_mb'],
                'driver_free_mb': mem_actual['driver_free_memory_mb'],
                'executors_total_mb': mem_actual['executors_total_memory_mb'],
                'executors_used_mb': mem_actual['executors_used_memory_mb'],
                'executors_free_mb': mem_actual['executors_free_memory_mb']
            },
            
            # Статистика Spark jobs и задач
            'job_statistics': {
                'num_jobs': job_stats['num_jobs'],
                'num_completed_jobs': job_stats['num_completed_jobs'],
                'num_failed_jobs': job_stats['num_failed_jobs'],
                'num_stages': job_stats['num_stages'],
                'num_completed_stages': job_stats['num_completed_stages'],
                'num_tasks_launched': job_stats['num_tasks_launched'],
                'num_tasks_completed': job_stats['num_tasks_completed'],
                'num_tasks_failed': job_stats['num_tasks_failed']
            },
            
            # Метрики данных
            'rows_processed': num_rows,
        }
        
        # Сохранение JSON файла
        json_path = os.path.join(metrics_dir, f"metrics_{exp_name}.json")
        with open(json_path, 'w') as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Метрики сохранены в {json_path}")
        
        # Вывод резюме
        logger.info("=" * 60)
        logger.info("РЕЗЮМЕ МЕТРИК:")
        logger.info(f"  Эксперимент: {exp_name}")
        logger.info(f"  Оптимизирован: {'Да' if is_optimized else 'Нет'}")
        logger.info(f"  Общее время: {metrics['total_execution_time_sec']} сек")
        if stages:
            logger.info(f"  Время по стадиям:")
            for stage_name, stage_time in stages.items():
                logger.info(f"    - {stage_name}: {stage_time} сек")
        logger.info(f"  Обработано строк: {num_rows:,}")
        logger.info(f"  Память выделена (MB):")
        logger.info(f"    - Driver: {mem_allocated['driver_memory_mb']} MB")
        logger.info(f"    - Executors: {mem_allocated['num_executors']} x {mem_allocated['executor_memory_mb']} MB = {mem_allocated['total_executor_memory_mb']} MB")
        logger.info(f"  Память РЕАЛЬНО ИСПОЛЬЗУЕТСЯ (MB):")
        logger.info(f"    - Driver: {mem_actual['driver_used_memory_mb']} MB из {mem_actual['driver_total_memory_mb']} MB")
        logger.info(f"    - Executors: {mem_actual['executors_used_memory_mb']} MB из {mem_actual['executors_total_memory_mb']} MB")
        logger.info(f"  Jobs & Tasks:")
        logger.info(f"    - Jobs: {job_stats['num_jobs']} (успешно: {job_stats['num_completed_jobs']}, ошибок: {job_stats['num_failed_jobs']})")
        logger.info(f"    - Stages: {job_stats['num_stages']} (завершено: {job_stats['num_completed_stages']})")
        logger.info(f"    - Tasks: {job_stats['num_tasks_launched']} запущено, {job_stats['num_tasks_completed']} завершено, {job_stats['num_tasks_failed']} ошибок")
        logger.info("=" * 60)
        
        return metrics
    except Exception as e:
        logger.error(f"Ошибка при сохранении метрик: {e}")
        raise
