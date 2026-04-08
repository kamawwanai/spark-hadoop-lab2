#!/usr/bin/env python3
# Сравнение результатов всех 4 экспериментов и создание графиков

import json
import os
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
EXPERIMENTS_DIR = PROJECT_ROOT / 'experiments'
RESULTS_DIR = PROJECT_ROOT / 'results'
PLOTS_DIR = RESULTS_DIR / 'plots'

# Ensure plots directory exists
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def load_metrics():
    """Загрузка метрик из всех 4 экспериментов"""
    experiments = [
        'exp_1dn_base',
        'exp_1dn_optimized',
        'exp_3dn_base',
        'exp_3dn_optimized'
    ]
    
    metrics_data = []
    
    for exp_name in experiments:
        exp_dir = EXPERIMENTS_DIR / exp_name / 'results'
        
        json_file = exp_dir / f'metrics_{exp_name}.json'
        txt_file = exp_dir / 'metrics_summary.txt'
        
        data = {
            'experiment': exp_name,
            'config': 'CONFIG',
            'optimization': 'OPT',
            'execution_time': None,
            'rows_processed': None,
            'num_executors': None,
            'driver_memory_mb': None,
            'executor_memory_mb': None,
            'total_executor_memory_mb': None,
            'stages': {}
        }
        
        # Определение конфига и оптимизации из имени
        if '1dn' in exp_name:
            data['config'] = '1 DataNode'
            data['nodes'] = 1
        else:
            data['config'] = '3 DataNodes'
            data['nodes'] = 3
            
        if 'optimized' in exp_name:
            data['optimization'] = 'Оптимизированная'
        else:
            data['optimization'] = 'Базовая'
        
        # Загрузка из JSON
        if json_file.exists():
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    # Новая структура метрик
                    data['execution_time'] = json_data.get('total_execution_time_sec')
                    data['rows_processed'] = json_data.get('rows_processed')
                    
                    # Информация о памяти
                    storage = json_data.get('storage', {})
                    data['driver_memory_mb'] = storage.get('driver_memory_mb')
                    data['executor_memory_mb'] = storage.get('executor_memory_mb')
                    data['total_executor_memory_mb'] = storage.get('total_executor_memory_mb')
                    data['num_executors'] = storage.get('num_executors')
                    
                    # Информация о стадиях
                    data['stages'] = json_data.get('stages', {})
                    
                    if data['execution_time'] is not None:
                        print(f"✓ JSON метрики загружены: {exp_name}")
                    else:
                        print(f"⚠ Предупреждение: метрики в {json_file} содержат пустые значения")
            except Exception as e:
                print(f"✗ Ошибка загрузки JSON из {json_file}: {e}")
        
        # Загрузка из TXT (для обратной совместимости)
        if data['execution_time'] is None and txt_file.exists():
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.startswith('execution_time_sec='):
                            data['execution_time'] = float(line.split('=')[1].strip())
                        elif line.startswith('rows_processed='):
                            data['rows_processed'] = int(line.split('=')[1].strip())
                    print(f"✓ TXT метрики загружены из {txt_file}")
            except Exception as e:
                print(f"✗ Ошибка загрузки TXT из {txt_file}: {e}")
        
        if data['execution_time'] is not None:
            metrics_data.append(data)
        else:
            print(f"⚠ Внимание: метрики не найдены для {exp_name}")
    
    if not metrics_data:
        print("✗ Ошибка: метрики не найдены ни для одного эксперимента!")
        return None
    
    return pd.DataFrame(metrics_data)

def calculate_speedup(df):
    """Calculate speedup factors (3DN vs 1DN, optimized vs base)
    Расчет коэффициентов ускорения (3DN vs 1DN, оптимизированная vs базовая)"""
    
    if df is None or len(df) < 4:
        print("Недостаточно данных для расчета ускорения")
        return df
    
    try:
        # Получение времени для каждой конфигурации
        df_1dn_base = df[df['experiment'] == 'exp_1dn_base']['execution_time'].values
        df_1dn_opt = df[df['experiment'] == 'exp_1dn_optimized']['execution_time'].values
        df_3dn_base = df[df['experiment'] == 'exp_3dn_base']['execution_time'].values
        df_3dn_opt = df[df['experiment'] == 'exp_3dn_optimized']['execution_time'].values
        
        speedup_data = {
            'metric': [
                '1DN Базовая vs Оптимизированная',
                '3DN Базовая vs Оптимизированная',
                '1DN vs 3DN (Базовая)',
                '1DN vs 3DN (Оптимизированная)'
            ],
            'speedup': []
        }
        
        if len(df_1dn_base) > 0 and len(df_1dn_opt) > 0:
            speedup_data['speedup'].append(df_1dn_base[0] / df_1dn_opt[0])
        if len(df_3dn_base) > 0 and len(df_3dn_opt) > 0:
            speedup_data['speedup'].append(df_3dn_base[0] / df_3dn_opt[0])
        if len(df_1dn_base) > 0 and len(df_3dn_base) > 0:
            speedup_data['speedup'].append(df_1dn_base[0] / df_3dn_base[0])
        if len(df_1dn_opt) > 0 and len(df_3dn_opt) > 0:
            speedup_data['speedup'].append(df_1dn_opt[0] / df_3dn_opt[0])
        
        return pd.DataFrame(speedup_data)
    except Exception as e:
        print(f"Ошибка при расчете ускорения")

def create_plots(df):
    """Создание сравнительных графиков"""
    if df is None or len(df) == 0:
        print("Ошибка: данных нет для построения графиков")
        return
    
    print("Создание графиков...")
    
    # 1. Сравнение времени выполнения
    try:
        plt.figure(figsize=(10, 6))
        colors = ['#FF6B6B' if 'Базовая' in opt else '#4ECDC4' for opt in df['optimization']]
        bars = plt.bar(df['experiment'], df['execution_time'], color=colors, alpha=0.7, edgecolor='black')
        
        # Добавление значений на столбцы
        for bar in bars:
            height = bar.get_height()
            if height:
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}s', ha='center', va='bottom')
        
        plt.xlabel('Эксперимент', fontsize=12, fontweight='bold')
        plt.ylabel('Время выполнения (сек)', fontsize=12, fontweight='bold')
        plt.title('Сравнение времени выполнения', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(PLOTS_DIR / 'execution_time.png', dpi=150, bbox_inches='tight')
        print(f"Сохранено: {PLOTS_DIR / 'execution_time.png'}")
        plt.close()
    except Exception as e:
        print(f"Ошибка создания графика времени выполнения: {e}")
    
    # 2. Сравнение коэффициентов ускорения
    try:
        speedup_df = calculate_speedup(df)
        if speedup_df is not None and len(speedup_df) > 0:
            plt.figure(figsize=(10, 6))
            colors_speedup = ['#95E1D3'] * len(speedup_df)
            bars = plt.barh(speedup_df['metric'], speedup_df['speedup'], color=colors_speedup, alpha=0.7, edgecolor='black')
            
            # Линия без ускорения
            plt.axvline(x=1.0, color='red', linestyle='--', linewidth=2, label='Ускорения нет')
            
            # Добавление значений
            for i, (bar, val) in enumerate(zip(bars, speedup_df['speedup'])):
                plt.text(val + 0.05, bar.get_y() + bar.get_height()/2., 
                        f'{val:.2f}x', va='center', fontweight='bold')
            
            plt.xlabel('Коэффициент ускорения', fontsize=12, fontweight='bold')
            plt.title('Коэффициенты ускорения', fontsize=14, fontweight='bold')
            plt.grid(axis='x', alpha=0.3)
            plt.legend()
            plt.tight_layout()
            plt.savefig(PLOTS_DIR / 'speedup_factors.png', dpi=150, bbox_inches='tight')
            print(f"Сохранено: {PLOTS_DIR / 'speedup_factors.png'}")
            plt.close()
    except Exception as e:
        print(f"Ошибка создания графика ускорения: {e}")
    
    # 3. Сравнение базовой и оптимизированной версий
    try:
        base_1dn = df[df['experiment'] == 'exp_1dn_base']['execution_time'].values
        opt_1dn = df[df['experiment'] == 'exp_1dn_optimized']['execution_time'].values
        base_3dn = df[df['experiment'] == 'exp_3dn_base']['execution_time'].values
        opt_3dn = df[df['experiment'] == 'exp_3dn_optimized']['execution_time'].values
        
        categories = ['1 DataNode', '3 DataNodes']
        base_times = [base_1dn[0] if len(base_1dn) > 0 else 0, 
                      base_3dn[0] if len(base_3dn) > 0 else 0]
        opt_times = [opt_1dn[0] if len(opt_1dn) > 0 else 0, 
                     opt_3dn[0] if len(opt_3dn) > 0 else 0]
        
        x = range(len(categories))
        width = 0.35
        
        plt.figure(figsize=(10, 6))
        bars1 = plt.bar([i - width/2 for i in x], base_times, width, label='Базовая', color='#FF6B6B', alpha=0.7, edgecolor='black')
        bars2 = plt.bar([i + width/2 for i in x], opt_times, width, label='Оптимизированная', color='#4ECDC4', alpha=0.7, edgecolor='black')
        
        # Добавление значений
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height:
                    plt.text(bar.get_x() + bar.get_width()/2., height,
                            f'{height:.1f}s', ha='center', va='bottom', fontsize=10)
        
        plt.xlabel('Конфигурация', fontsize=12, fontweight='bold')
        plt.ylabel('Время выполнения (сек)', fontsize=12, fontweight='bold')
        plt.title('Базовая vs Оптимизированная версия', fontsize=14, fontweight='bold')
        plt.xticks(x, categories)
        plt.legend()
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(PLOTS_DIR / 'base_vs_optimized.png', dpi=150, bbox_inches='tight')
        print(f"Сохранено: {PLOTS_DIR / 'base_vs_optimized.png'}")
        plt.close()
    except Exception as e:
        print(f"Ошибка создания графика сравнения: {e}")

def create_summary_csv(df):
    """Создание файла со сводкой результатов"""
    if df is None or len(df) == 0:
        print("Ошибка: данных нет для создания сводки")
        return
    
    try:
        summary_file = RESULTS_DIR / 'summary.csv'
        df.to_csv(summary_file, index=False)
        print(f"Сводка сохранена: {summary_file}")
    except Exception as e:
        print(f"Ошибка при сохранении сводки: {e}")

def create_markdown_report(df):
    """Создание отчета в Markdown с интерпретацией результатов"""
    if df is None or len(df) == 0:
        print("Ошибка: данных нет для создания отчета")
        return
    
    try:
        report_file = RESULTS_DIR / 'comparison.md'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# Отчет о сравнении экспериментов Spark-Hadoop\n\n")
            f.write("## Сводка метрик\n\n")
            
            # Таблица сводки с расширенной информацией
            f.write("| Эксперимент | Конфиг | Вид | Время (сек) | Строк | Executors | Driver RAM | Exec RAM |\n")
            f.write("|---|---|---|---|---|---|---|---|\n")
            
            for _, row in df.iterrows():
                driver_ram = f"{int(row['driver_memory_mb'])} MB" if row['driver_memory_mb'] else "-"
                exec_ram = f"{int(row['executor_memory_mb'])} MB" if row['executor_memory_mb'] else "-"
                f.write(f"| {row['experiment']} | {row['config']} | {row['optimization']} | " +
                       f"{row['execution_time']:.2f} | {row['rows_processed']:,} | {int(row['num_executors']) if row['num_executors'] else '-'} | " +
                       f"{driver_ram} | {exec_ram} |\n")
            
            f.write("\n## Анализ производительности\n\n")
            
            # Расчет и вывод результатов
            df_1dn_base = df[df['experiment'] == 'exp_1dn_base']
            df_1dn_opt = df[df['experiment'] == 'exp_1dn_optimized']
            df_3dn_base = df[df['experiment'] == 'exp_3dn_base']
            df_3dn_opt = df[df['experiment'] == 'exp_3dn_optimized']
            
            if len(df_1dn_base) > 0:
                time_1dn_base = df_1dn_base.iloc[0]['execution_time']
                f.write(f"### 1 DataNode\n\n")
                f.write(f"- **Базовая версия**: {time_1dn_base:.2f} сек\n")
                
                if len(df_1dn_opt) > 0:
                    time_1dn_opt = df_1dn_opt.iloc[0]['execution_time']
                    speedup = time_1dn_base / time_1dn_opt
                    improvement = (1 - time_1dn_opt / time_1dn_base) * 100
                    f.write(f"- **Оптимизированная версия**: {time_1dn_opt:.2f} сек\n")
                    f.write(f"- **Ускорение оптимизации**: {speedup:.2f}x ({improvement:.1f}% улучшение)\n")
                    
                    # Показываем стадии если они есть
                    if isinstance(df_1dn_opt.iloc[0]['stages'], dict) and len(df_1dn_opt.iloc[0]['stages']) > 0:
                        f.write(f"\n**Время по стадиям (оптимизированная):**\n")
                        for stage, stage_time in df_1dn_opt.iloc[0]['stages'].items():
                            f.write(f"  - {stage}: {stage_time:.2f} сек\n")
            
            if len(df_3dn_base) > 0:
                time_3dn_base = df_3dn_base.iloc[0]['execution_time']
                f.write(f"\n### 3 DataNodes\n\n")
                f.write(f"- **Базовая версия**: {time_3dn_base:.2f} сек\n")
                
                if len(df_3dn_opt) > 0:
                    time_3dn_opt = df_3dn_opt.iloc[0]['execution_time']
                    speedup = time_3dn_base / time_3dn_opt
                    improvement = (1 - time_3dn_opt / time_3dn_base) * 100
                    f.write(f"- **Оптимизированная версия**: {time_3dn_opt:.2f} сек\n")
                    f.write(f"- **Ускорение оптимизации**: {speedup:.2f}x ({improvement:.1f}% улучшение)\n")
                    
                    # Показываем стадии если они есть
                    if isinstance(df_3dn_opt.iloc[0]['stages'], dict) and len(df_3dn_opt.iloc[0]['stages']) > 0:
                        f.write(f"\n**Время по стадиям (оптимизированная):**\n")
                        for stage, stage_time in df_3dn_opt.iloc[0]['stages'].items():
                            f.write(f"  - {stage}: {stage_time:.2f} сек\n")
            
            f.write("\n### Масштабирование кластера\n\n")
            if len(df_1dn_base) > 0 and len(df_3dn_base) > 0:
                time_1dn_base = df_1dn_base.iloc[0]['execution_time']
                time_3dn_base = df_3dn_base.iloc[0]['execution_time']
                speedup_3dn = time_1dn_base / time_3dn_base
                f.write(f"- **Базовая версия (1DN vs 3DN)**: {speedup_3dn:.2f}x ускорение\n")
            
            if len(df_1dn_opt) > 0 and len(df_3dn_opt) > 0:
                time_1dn_opt = df_1dn_opt.iloc[0]['execution_time']
                time_3dn_opt = df_3dn_opt.iloc[0]['execution_time']
                speedup_3dn_opt = time_1dn_opt / time_3dn_opt
                f.write(f"- **Оптимизированная версия (1DN vs 3DN)**: {speedup_3dn_opt:.2f}x ускорение\n")
            
            f.write("\n## Информация о памяти\n\n")
            for _, row in df.iterrows():
                if row['driver_memory_mb'] and row['executor_memory_mb']:
                    total_mem = row['driver_memory_mb'] + row['total_executor_memory_mb']
                    f.write(f"**{row['experiment']}:**\n")
                    f.write(f"  - Driver: {int(row['driver_memory_mb'])} MB\n")
                    f.write(f"  - Executors: {int(row['num_executors'])} x {int(row['executor_memory_mb'])} MB\n")
                    f.write(f"  - Total: {int(total_mem)} MB\n\n")
            
            f.write("\n## Созданные графики\n\n")
            f.write("- `execution_time.png` - Время выполнения для всех экспериментов\n")
            f.write("- `speedup_factors.png` - Сравнение коэффициентов ускорения\n")
            f.write("- `base_vs_optimized.png` - Сравнение базовой и оптимизированной версий\n")
        
        print(f"✓ Отчет сохранен: {report_file}")
    except Exception as e:
        print(f"✗ Ошибка при создании отчета: {e}")

def main():
    print("====================================================")
    print("Сравнение результатов экспериментов")
    print("====================================================\n")
    
    # Загрузка метрик
    print("Загрузка метрик из экспериментов...")
    df = load_metrics()
    
    if df is None or len(df) == 0:
        print("Ошибка: метрики не найдены. Запустите эксперименты сначала.")
        sys.exit(1)
    
    print(f"Загружены метрики для {len(df)} экспериментов\n")
    print(df)
    print()
    
    # Создание выходных данных
    create_summary_csv(df)
    create_plots(df)
    create_markdown_report(df)
    
    print("\n====================================================")
    print("Сравнение завершено успешно")
    print("Результаты в: results/")
    print("====================================================")

if __name__ == "__main__":
    main()
