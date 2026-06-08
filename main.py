# -*- coding: utf-8 -*-
"""
一键运行入口 - main.py

执行完整的数据流处理流水线：
1. 数据获取（爬虫 + 模拟数据生成）
2. 数据质量保障
3. ODS层导入
4. DWD层处理
5. DWS层计算
6. 实时流计算（Flink）
7. 输出结果

使用方法：
    python main.py
"""

import os
import sys
import time
import json
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from src.scraper import VehicleDataScraper
from src.data_generator import CANDataGenerator
from src.kafka_sim import KafkaSimulator
from src.data_quality import DataQualityProcessor, save_quality_report
from src.spark_warehouse import run_warehouse_pipeline, PandasWarehouseBuilder
from src.flink_stream import PyFlinkProcessor
from src.metrics import CoreMetricsCalculator


class VehicleDataPipeline:
    """
    车辆数据处理完整流水线
    
    协调各个模块完成数据处理任务
    """
    
    def __init__(self):
        self.start_time = datetime.now()
        self.results = {}
        self.pipeline_stats = {}
        
    def log(self, message: str, level: str = "INFO"):
        """日志记录"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")
        
    def step(self, step_name: str, func, *args, **kwargs):
        """
        执行流水线步骤
        
        Args:
            step_name: 步骤名称
            func: 执行函数
            *args, **kwargs: 函数参数
            
        Returns:
            函数执行结果
        """
        self.log(f"开始步骤: {step_name}")
        step_start = time.time()
        
        try:
            result = func(*args, **kwargs)
            step_duration = time.time() - step_start
            self.pipeline_stats[step_name] = {
                "status": "success",
                "duration": round(step_duration, 2)
            }
            self.log(f"完成步骤: {step_name} (耗时: {step_duration:.2f}秒)")
            return result
        except Exception as e:
            step_duration = time.time() - step_start
            self.pipeline_stats[step_name] = {
                "status": "failed",
                "error": str(e),
                "duration": round(step_duration, 2)
            }
            self.log(f"步骤失败: {step_name} - {e}", "ERROR")
            raise
            
    def run(self):
        """
        运行完整流水线
        """
        print("\n" + "="*70)
        print("智能网联车辆实时指标计算与数仓建设项目")
        print("="*70)
        print(f"开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"项目目录: {config.PROJECT_ROOT}")
        print("="*70)
        
        try:
            # ========== 步骤1: 数据获取 ==========
            print("\n" + "-"*70)
            print("【步骤1】数据获取")
            print("-"*70)
            
            df = self.step("data_acquisition", self._data_acquisition)
            self.results["raw_data"] = df
            
            # ========== 步骤2: 数据质量检查与清洗 ==========
            print("\n" + "-"*70)
            print("【步骤2】数据质量检查与清洗")
            print("-"*70)
            
            cleaned_df, quality_report = self.step("data_quality", self._data_quality, df)
            self.results["cleaned_data"] = cleaned_df
            self.results["quality_report"] = quality_report
            
            # ========== 步骤3: ODS层导入 ==========
            print("\n" + "-"*70)
            print("【步骤3】ODS层 - 操作数据存储")
            print("-"*70)
            
            ods_path = self.step("ods_layer", self._save_ods, cleaned_df)
            self.results["ods_path"] = ods_path
            
            # ========== 步骤4: DWD层处理 ==========
            print("\n" + "-"*70)
            print("【步骤4】DWD层 - 明细数据层")
            print("-"*70)
            
            dwd_df, dwd_path = self.step("dwd_layer", self._build_dwd, cleaned_df)
            self.results["dwd_data"] = dwd_df
            self.results["dwd_path"] = dwd_path
            
            # ========== 步骤5: DWS层计算 ==========
            print("\n" + "-"*70)
            print("【步骤5】DWS层 - 数据服务层（8个核心指标）")
            print("-"*70)
            
            dws_metrics, dws_path = self.step("dws_layer", self._build_dws, dwd_df)
            self.results["dws_metrics"] = dws_metrics
            self.results["dws_path"] = dws_path
            
            # ========== 步骤6: 实时流计算 ==========
            print("\n" + "-"*70)
            print("【步骤6】实时流计算 (PyFlink)")
            print("-"*70)
            
            realtime_metrics = self.step("flink_stream", self._run_flink, cleaned_df)
            self.results["realtime_metrics"] = realtime_metrics
            
            # ========== 步骤7: 生成报告 ==========
            print("\n" + "-"*70)
            print("【步骤7】生成报告与可视化数据")
            print("-"*70)
            
            self.step("report_generation", self._generate_reports, dws_metrics, quality_report)
            
            # ========== 完成 ==========
            self._print_summary()
            
        except Exception as e:
            self.log(f"流水线执行失败: {e}", "ERROR")
            raise
            
        return self.results
        
    def _data_acquisition(self):
        """
        数据获取步骤
        
        优先从已有文件读取，否则生成模拟数据
        """
        scraper = VehicleDataScraper()
        
        # 检查已有数据
        existing_df = scraper.check_existing_data()
        if existing_df is not None:
            self.log("使用已有数据文件")
            return existing_df
            
        # 尝试获取真实数据
        df, source = scraper.fetch_real_data()
        if df is not None:
            self.log(f"获取到真实数据: {source}")
            return df
            
        # 生成模拟数据
        self.log("生成模拟CAN数据...")
        generator = CANDataGenerator()
        output_path = os.path.join(config.RAW_DIR, "vehicle_can_data.csv")
        df = generator.generate_fleet_data(output_path)
        self.log(f"模拟数据已保存: {output_path}")
        
        return df
        
    def _data_quality(self, df):
        """
        数据质量检查与清洗
        
        Args:
            df: 原始数据
            
        Returns:
            (清洗后数据, 质量报告)
        """
        self.log(f"待处理记录数: {len(df)}")
        
        processor = DataQualityProcessor(df)
        cleaned_df, quality_report = processor.clean()
        
        # 保存质量报告
        report_path = os.path.join(config.REPORTS_DIR, f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        save_quality_report(quality_report, report_path)
        
        return cleaned_df, quality_report
        
    def _save_ods(self, df):
        """
        保存ODS层数据
        
        Args:
            df: 数据DataFrame
            
        Returns:
            ODS文件路径
        """
        output_path = os.path.join(config.ODS_DIR, "ods_data.csv")
        df.to_csv(output_path, index=False)
        self.log(f"ODS数据已保存: {output_path}")
        self.log(f"ODS记录数: {len(df)}")
        return output_path
        
    def _build_dwd(self, df):
        """
        构建DWD层
        
        Args:
            df: ODS数据
            
        Returns:
            (DWD数据, DWD文件路径)
        """
        # 数据质量处理已经在_qualit中完成，这里直接使用清洗后的数据
        # 添加派生字段
        self.log("添加派生字段...")
        
        # 行驶状态
        import numpy as np
        conditions = [
            df['speed'] == 0,
            df['speed'] < 60,
            df['speed'] < 100,
        ]
        choices = ['idle', 'normal', 'fast']
        df['driving_status'] = np.select(conditions, choices, default='highway')
        
        # 能耗等级
        conditions = [
            df['energy_consumption'] < 10,
            df['energy_consumption'] < 15,
            df['energy_consumption'] < 20,
        ]
        choices = ['A_excellent', 'B_good', 'C_average']
        df['energy_category'] = np.select(conditions, choices, default='D_poor')
        
        # 温度状态
        conditions = [
            df['battery_temperature'] < 10,
            df['battery_temperature'] < 30,
            df['battery_temperature'] < 45,
        ]
        choices = ['cold', 'normal', 'warm']
        df['temp_status'] = np.select(conditions, choices, default='hot')
        
        # 电池状态
        def get_battery_status(row):
            if row['soc'] < 20:
                return 'low_power'
            elif row['soc'] > 80:
                return 'high_power'
            elif row['battery_temperature'] > 45:
                return 'high_temp_warning'
            return 'normal'
            
        df['battery_status'] = df.apply(get_battery_status, axis=1)
        
        output_path = os.path.join(config.DWD_DIR, "dwd_data.csv")
        df.to_csv(output_path, index=False)
        self.log(f"DWD数据已保存: {output_path}")
        self.log(f"DWD记录数: {len(df)}")
        self.log(f"新增字段: driving_status, energy_category, temp_status, battery_status")
        
        return df, output_path
        
    def _build_dws(self, df):
        """
        构建DWS层 - 计算8个核心指标
        
        Args:
            df: DWD数据
            
        Returns:
            (指标字典, DWS文件路径)
        """
        # 确保时间列
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        # 计算8个核心指标
        self.log("计算8个核心指标...")
        calculator = CoreMetricsCalculator(df)
        metrics = calculator.calculate_all_metrics()
        
        # 保存各指标
        metrics_dir = os.path.join(config.DWS_DIR, "metrics")
        os.makedirs(metrics_dir, exist_ok=True)
        
        for metric_name, metric_df in metrics.items():
            if metric_df is not None and not metric_df.empty:
                metric_path = os.path.join(metrics_dir, f"{metric_name}.csv")
                metric_df.to_csv(metric_path, index=False)
                self.log(f"  {metric_name}: {len(metric_df)} 条记录 -> {metric_path}")
        
        # 保存完整DWS
        dfs = [m for m in metrics.values() if m is not None and not m.empty]
        if dfs:
            dws_df = pd.concat(dfs, ignore_index=True)
            dws_df['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dws_path = os.path.join(config.DWS_DIR, "dws_data.csv")
            dws_df.to_csv(dws_path, index=False)
        else:
            dws_df = None
            dws_path = None
            
        # 打印指标汇总
        self.log("\n指标汇总:")
        summary = calculator.get_summary_statistics(metrics)
        if not summary.empty:
            for _, row in summary.iterrows():
                if row['column'] == 'daily_energy_consumption' or row['column'] == 'average_speed':
                    self.log(f"  {row['metric']}.{row['column']}: 均值={row['mean']}, 最大={row['max']}")
        
        return metrics, dws_path
        
    def _run_flink(self, df):
        """
        运行PyFlink实时流计算
        
        Args:
            df: 源数据
            
        Returns:
            实时指标列表
        """
        # 取部分数据进行实时计算
        sample_size = min(5000, len(df))
        sample_df = df.head(sample_size)
        
        self.log(f"实时计算样本数: {sample_size}")
        
        # 使用Flink处理器
        processor = PyFlinkProcessor()
        
        # 转换为字典列表
        records = sample_df.to_dict('records')
        
        # 计算实时指标
        output_path = os.path.join(config.OUTPUT_DIR, "realtime_metrics.csv")
        metrics = processor.start(records, output_path)
        
        if metrics is None:
            metrics = []
            
        self.log(f"实时指标已保存: {output_path}")
        self.log(f"计算了 {len(metrics)} 个窗口指标")
        
        # 打印示例
        if metrics:
            self.log("\n实时指标示例:")
            for m in metrics[:3]:
                self.log(f"  [{m['vehicle_id']}] {m['window_end_time']}")
                self.log(f"    瞬时功率: {m['instantaneous_power']} kW")
                self.log(f"    电池温升速率: {m['battery_temp_rate']} °C/s")
                self.log(f"    平均车速: {m['avg_speed']} km/h")
        
        return metrics
        
    def _generate_reports(self, dws_metrics, quality_report):
        """
        生成报告与可视化数据
        
        Args:
            dws_metrics: DWS层指标
            quality_report: 质量报告
        """
        # 生成可视化数据
        viz_data = {}
        
        # 1. 车辆汇总数据
        if dws_metrics:
            # 合并所有指标
            dfs = [m for m in dws_metrics.values() if m is not None and not m.empty]
            if dfs:
                all_metrics = pd.concat(dfs, ignore_index=True)
                
                # 按车辆汇总 - 动态查找数值列
                numeric_cols = [c for c in all_metrics.columns 
                               if c not in ['vehicle_id', 'date', 'update_time', 'metric_name']
                               and all_metrics[c].dtype in ['float64', 'int64']]
                
                agg_dict = {}
                for col in numeric_cols[:5]:  # 只取前5个数值列
                    agg_dict[col] = 'mean'
                
                if agg_dict:
                    vehicle_summary = all_metrics.groupby('vehicle_id').agg(agg_dict).reset_index()
                    viz_data['vehicle_summary'] = vehicle_summary.to_dict('records')
                    
                # 保存指标汇总
                if numeric_cols:
                    summary_data = {}
                    for col in numeric_cols:
                        summary_data[col] = {
                            'mean': round(float(all_metrics[col].mean()), 2),
                            'min': round(float(all_metrics[col].min()), 2),
                            'max': round(float(all_metrics[col].max()), 2),
                        }
                    viz_data['metrics_summary'] = summary_data
                
        # 2. 质量统计
        if quality_report:
            viz_data['quality_stats'] = {
                'original_count': quality_report.get('original_quality', {}).get('original_count', 0),
                'final_count': quality_report.get('final_record_count', 0),
                'completeness': quality_report.get('final_quality', {}).get('completeness', {}).get('completeness', 0),
                'duplicate_rate': quality_report.get('original_quality', {}).get('duplicates', {}).get('duplicate_rate', 0),
            }
            
        # 保存可视化数据
        viz_path = os.path.join(config.OUTPUT_DIR, "visualization_data.json")
        with open(viz_path, 'w', encoding='utf-8') as f:
            json.dump(viz_data, f, ensure_ascii=False, indent=2)
        self.log(f"可视化数据已保存: {viz_path}")
        
    def _print_summary(self):
        """打印执行摘要"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        print("\n" + "="*70)
        print("流水线执行完成")
        print("="*70)
        print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {total_duration:.2f}秒")
        
        print("\n【步骤执行统计】")
        for step, stats in self.pipeline_stats.items():
            status = stats['status']
            duration = stats['duration']
            status_icon = "✓" if status == "success" else "✗"
            print(f"  {status_icon} {step}: {status} ({duration}秒)")
            
        print("\n【输出文件】")
        for key, path in [('ODS', 'ods_path'), ('DWD', 'dwd_path'), ('DWS', 'dws_path')]:
            if path in self.results and self.results[path]:
                print(f"  {key}: {self.results[path]}")
                
        print("\n【数据概况】")
        if 'cleaned_data' in self.results:
            print(f"  清洗后记录数: {len(self.results['cleaned_data'])}")
        if 'dws_metrics' in self.results:
            print(f"  核心指标数量: {len(self.results['dws_metrics'])}")
        if 'realtime_metrics' in self.results:
            print(f"  实时窗口指标: {len(self.results['realtime_metrics'])}")
            
        print("\n" + "="*70)
        print("所有任务执行完成！")
        print("="*70)


def main():
    """主函数 - 一键运行"""
    print("\n" + "█"*70)
    print("█" + " "*68 + "█")
    print("█" + "    智能网联车辆实时指标计算与数仓建设项目    ".center(68) + "█")
    print("█" + "    Vehicle Realtime Warehouse Pipeline    ".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70 + "\n")
    
    # 创建pandas别名
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    # 运行流水线
    pipeline = VehicleDataPipeline()
    results = pipeline.run()
    
    return results


if __name__ == "__main__":
    # 确保导入pandas
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    main()
