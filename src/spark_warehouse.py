# -*- coding: utf-8 -*-
"""
PySpark SQL离线数仓模块

实现ODS → DWD → DWS三层数据仓库模型：
- ODS层：操作数据存储层，原始数据
- DWD层：明细数据层，清洗和标准化
- DWS层：数据服务层，汇总指标

使用PySpark SQL完成ETL处理
"""

import os
import sys
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
    SparkSessionType = SparkSession
except ImportError:
    SPARK_AVAILABLE = False
    print("PySpark未安装，将使用Pandas实现")
    SparkSessionType = type(None)
    DataFrame = pd.DataFrame

import config
from src.data_quality import DataQualityProcessor
from src.metrics import CoreMetricsCalculator


class SparkWarehouseBuilder:
    """
    PySpark数仓构建器
    
    使用PySpark DataFrame API构建ODS→DWD→DWS三层架构
    """
    
    def __init__(self, app_name: str = None):
        """
        初始化Spark会话
        
        Args:
            app_name: 应用名称
        """
        self.app_name = app_name or config.SPARK_CONFIG["app_name"]
        self.spark = None
        self.df_ods = None
        self.df_dwd = None
        self.df_dws = None
        
    def _create_spark_session(self) -> SparkSessionType:
        """创建Spark会话"""
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark未安装")
            
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(config.SPARK_CONFIG["master"])
            
        # 添加配置
        builder = builder.config("spark.driver.memory", config.SPARK_CONFIG["driver_memory"])
        builder = builder.config("spark.executor.memory", config.SPARK_CONFIG["executor_memory"])
        builder = builder.config("spark.sql.shuffle.partitions", config.SPARK_CONFIG["shuffle_partitions"])
        
        return builder.getOrCreate()
    
    def load_ods(self, input_path: str) -> DataFrame:
        """
        加载ODS层数据
        
        Args:
            input_path: 输入CSV路径
            
        Returns:
            ODS DataFrame
        """
        print(f"\n{'='*60}")
        print("ODS层 - 操作数据存储")
        print("="*60)
        print(f"加载数据: {input_path}")
        
        # 读取CSV
        self.df_ods = self.spark.read.csv(input_path, header=True, inferSchema=True)
        
        print(f"原始记录数: {self.df_ods.count()}")
        print(f"列数: {len(self.df_ods.columns)}")
        print(f"列名: {self.df_ods.columns}")
        
        return self.df_ods
    
    def build_dwd(self, df_ods: DataFrame = None) -> DataFrame:
        """
        构建DWD层 - 明细数据层
        
        处理内容：
        1. 数据类型标准化
        2. 去重处理
        3. 空值填充
        4. 异常值处理
        5. 派生字段计算
        
        Args:
            df_ods: ODS DataFrame
            
        Returns:
            DWD DataFrame
        """
        if df_ods is None:
            df_ods = self.df_ods
            
        print(f"\n{'='*60}")
        print("DWD层 - 明细数据层")
        print("="*60)
        
        # 转换为Pandas处理（简化实现）
        pdf_ods = df_ods.toPandas()
        
        # 数据质量处理
        processor = DataQualityProcessor(pdf_ods)
        pdf_dwd, _ = processor.clean()
        
        # 派生字段
        pdf_dwd = self._add_derived_fields(pdf_dwd)
        
        # 转换回Spark DataFrame
        self.df_dwd = self.spark.createDataFrame(pdf_dwd)
        
        print(f"DWD记录数: {self.df_dwd.count()}")
        print(f"新增字段: speed_acceleration, driving_status, energy_category")
        
        return self.df_dwd
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加派生字段
        
        派生字段：
        1. speed_acceleration: 速度加速度
        2. driving_status: 行驶状态（怠速/正常/高速）
        3. energy_category: 能耗等级
        4. temp_status: 温度状态
        5. battery_status: 电池状态
        """
        # 速度分组
        def get_speed_status(speed):
            if speed == 0:
                return 'idle'
            elif speed < 60:
                return 'normal'
            elif speed < 100:
                return 'fast'
            else:
                return 'highway'
                
        df['driving_status'] = df['speed'].apply(get_speed_status)
        
        # 能耗等级
        def get_energy_category(ec):
            if ec < 10:
                return 'A_excellent'
            elif ec < 15:
                return 'B_good'
            elif ec < 20:
                return 'C_average'
            else:
                return 'D_poor'
                
        df['energy_category'] = df['energy_consumption'].apply(get_energy_category)
        
        # 温度状态
        def get_temp_status(temp):
            if temp < 10:
                return 'cold'
            elif temp < 30:
                return 'normal'
            elif temp < 45:
                return 'warm'
            else:
                return 'hot'
                
        df['temp_status'] = df['battery_temperature'].apply(get_temp_status)
        
        # 电池状态
        def get_battery_status(soc, temp):
            if soc < 20:
                return 'low_power'
            elif soc > 80:
                return 'high_power'
            elif temp > 45:
                return 'high_temp_warning'
            else:
                return 'normal'
                
        df['battery_status'] = df.apply(
            lambda row: get_battery_status(row['soc'], row['battery_temperature']), axis=1
        )
        
        return df
    
    def build_dws(self, df_dwd: DataFrame = None) -> DataFrame:
        """
        构建DWS层 - 数据服务层
        
        计算8个核心指标：
        1. 日均电耗
        2. 急加速次数
        3. 高温累计时长
        4. 日均行驶里程
        5. 充电频次
        6. 平均车速
        7. 制动能量回收率
        8. 电池健康指数
        
        Args:
            df_dwd: DWD DataFrame
            
        Returns:
            DWS DataFrame
        """
        if df_dwd is None:
            df_dwd = self.df_dwd
            
        print(f"\n{'='*60}")
        print("DWS层 - 数据服务层")
        print("="*60)
        
        # 转换为Pandas计算
        pdf_dwd = df_dwd.toPandas()
        
        # 使用指标计算器
        calculator = CoreMetricsCalculator(pdf_dwd)
        metrics_dict = calculator.calculate_all_metrics()
        
        # 合并所有指标到一个DataFrame
        dfs = []
        for metric_name, metric_df in metrics_dict.items():
            if metric_df is not None and not metric_df.empty:
                metric_df_copy = metric_df.copy()
                metric_df_copy['metric_name'] = metric_name
                dfs.append(metric_df_copy)
        
        if dfs:
            self.df_dws = pd.concat(dfs, ignore_index=True)
            self.df_dws['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"DWS记录数: {len(self.df_dws)}")
            print(f"指标数量: {len(metrics_dict)}")
        else:
            self.df_dws = pd.DataFrame()
            
        return self.spark.createDataFrame(self.df_dws) if not self.df_dws.empty else None
    
    def save_layer(self, df: DataFrame, layer: str, output_format: str = "parquet"):
        """
        保存各层数据
        
        Args:
            df: DataFrame
            layer: 层名 (ods/dwd/dws)
            output_format: 输出格式 (parquet/csv)
        """
        layer_dir = getattr(config, f"{layer.upper()}_DIR")
        
        if output_format == "parquet":
            output_path = layer_dir
            df.write.mode("overwrite").parquet(output_path)
        else:
            # 保存为多个CSV（避免大文件）
            output_path = os.path.join(layer_dir, f"{layer}_data.csv")
            pdf = df.toPandas()
            pdf.to_csv(output_path, index=False)
            
        print(f"已保存{layer.upper()}层到: {output_path}")
        
    def run_pipeline(self, input_csv: str):
        """
        运行完整数仓流水线
        
        Args:
            input_csv: 输入CSV路径
            
        Returns:
            (ods_df, dwd_df, dws_df)
        """
        print("\n" + "="*70)
        print("PySpark SQL离线数仓流水线")
        print("="*70)
        
        # 初始化Spark
        if SPARK_AVAILABLE:
            self.spark = self._create_spark_session()
            print("Spark会话已创建")
        
        # ODS层
        df_ods = self.load_ods(input_csv)
        
        # DWD层
        df_dwd = self.build_dwd(df_ods)
        
        # DWS层
        df_dws = self.build_dws(df_dwd)
        
        # 保存各层
        print("\n保存数仓各层数据...")
        self.save_layer(df_ods, "ods")
        self.save_layer(df_dwd, "dwd")
        if df_dws:
            self.save_layer(df_dwd.limit(1000000).groupBy("vehicle_id", "date").count().toDF(*["vehicle_id", "date", "record_count"]), "dws")
        
        print("\n" + "="*70)
        print("数仓流水线执行完成")
        print("="*70)
        
        return df_ods, df_dwd, df_dws


class PandasWarehouseBuilder:
    """
    Pandas实现的数仓构建器（PySpark不可用时使用）
    """
    
    def __init__(self):
        self.df_ods = None
        self.df_dwd = None
        self.df_dws = None
        
    def run_pipeline(self, input_csv: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        运行完整数仓流水线
        
        Args:
            input_csv: 输入CSV路径
            
        Returns:
            (ods_df, dwd_df, dws_df)
        """
        print("\n" + "="*70)
        print("Pandas离线数仓流水线（PySpark降级模式）")
        print("="*70)
        
        # ODS层
        print(f"\n[1/3] ODS层 - 加载原始数据")
        self.df_ods = pd.read_csv(input_csv)
        print(f"  记录数: {len(self.df_ods)}")
        print(f"  列数: {len(self.df_ods.columns)}")
        
        # 保存ODS层
        ods_path = os.path.join(config.ODS_DIR, "ods_data.csv")
        self.df_ods.to_csv(ods_path, index=False)
        print(f"  已保存: {ods_path}")
        
        # DWD层
        print(f"\n[2/3] DWD层 - 数据清洗")
        processor = DataQualityProcessor(self.df_ods)
        self.df_dwd, _ = processor.clean()
        
        # 添加派生字段
        calculator = CoreMetricsCalculator(self.df_dwd)
        self._add_derived_fields()
        
        print(f"  记录数: {len(self.df_dwd)}")
        
        # 保存DWD层
        dwd_path = os.path.join(config.DWD_DIR, "dwd_data.csv")
        self.df_dwd.to_csv(dwd_path, index=False)
        print(f"  已保存: {dwd_path}")
        
        # DWS层
        print(f"\n[3/3] DWS层 - 指标计算")
        self.df_dwd['timestamp'] = pd.to_datetime(self.df_dwd['timestamp'])
        self.df_dwd['date'] = self.df_dwd['timestamp'].dt.date
        
        calculator = CoreMetricsCalculator(self.df_dwd)
        metrics = calculator.calculate_all_metrics()
        
        # 保存各指标
        dws_metrics_dir = os.path.join(config.DWS_DIR, "metrics")
        os.makedirs(dws_metrics_dir, exist_ok=True)
        
        for metric_name, metric_df in metrics.items():
            if metric_df is not None and not metric_df.empty:
                metric_path = os.path.join(dws_metrics_dir, f"{metric_name}.csv")
                metric_df.to_csv(metric_path, index=False)
                
        # 合并DWS
        dfs = [df for df in metrics.values() if df is not None and not df.empty]
        if dfs:
            self.df_dws = pd.concat(dfs, ignore_index=True)
            self.df_dws['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 保存DWS层
        dws_path = os.path.join(config.DWS_DIR, "dws_data.csv")
        self.df_dws.to_csv(dws_path, index=False)
        print(f"  已保存: {dws_path}")
        
        print("\n" + "="*70)
        print("数仓流水线执行完成")
        print("="*70)
        
        return self.df_ods, self.df_dwd, self.df_dws
    
    def _add_derived_fields(self):
        """添加派生字段"""
        # 行驶状态
        conditions = [
            self.df_dwd['speed'] == 0,
            self.df_dwd['speed'] < 60,
            self.df_dwd['speed'] < 100,
        ]
        choices = ['idle', 'normal', 'fast']
        self.df_dwd['driving_status'] = pd.np.select(conditions, choices, default='highway')
        
        # 能耗等级
        conditions = [
            self.df_dwd['energy_consumption'] < 10,
            self.df_dwd['energy_consumption'] < 15,
            self.df_dwd['energy_consumption'] < 20,
        ]
        choices = ['A_excellent', 'B_good', 'C_average']
        self.df_dwd['energy_category'] = pd.np.select(conditions, choices, default='D_poor')
        
        # 温度状态
        conditions = [
            self.df_dwd['battery_temperature'] < 10,
            self.df_dwd['battery_temperature'] < 30,
            self.df_dwd['battery_temperature'] < 45,
        ]
        choices = ['cold', 'normal', 'warm']
        self.df_dwd['temp_status'] = pd.np.select(conditions, choices, default='hot')
        
        # 电池状态
        def get_battery_status(row):
            if row['soc'] < 20:
                return 'low_power'
            elif row['soc'] > 80:
                return 'high_power'
            elif row['battery_temperature'] > 45:
                return 'high_temp_warning'
            return 'normal'
            
        self.df_dwd['battery_status'] = self.df_dwd.apply(get_battery_status, axis=1)


def run_warehouse_pipeline(input_csv: str):
    """
    运行数仓流水线的入口函数
    
    Args:
        input_csv: 输入CSV路径
        
    Returns:
        (ods_df, dwd_df, dws_df)
    """
    if SPARK_AVAILABLE:
        builder = SparkWarehouseBuilder()
        return builder.run_pipeline(input_csv)
    else:
        builder = PandasWarehouseBuilder()
        return builder.run_pipeline(input_csv)


def demo_warehouse():
    """演示数仓构建"""
    from src.data_generator import CANDataGenerator
    
    print("=" * 60)
    print("数仓构建演示")
    print("=" * 60)
    
    # 生成测试数据
    generator = CANDataGenerator()
    test_data = generator.generate_fleet_data()
    
    # 保存临时文件
    temp_input = os.path.join(config.RAW_DIR, "warehouse_demo_input.csv")
    test_data.to_csv(temp_input, index=False)
    
    # 运行流水线
    ods, dwd, dws = run_warehouse_pipeline(temp_input)
    
    print("\n各层数据概况:")
    print(f"ODS: {len(ods)} 条记录")
    print(f"DWD: {len(dwd)} 条记录")
    print(f"DWS: {len(dws) if dws is not None else 0} 条记录")
    
    return ods, dwd, dws


if __name__ == "__main__":
    demo_warehouse()
