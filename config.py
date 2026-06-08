# -*- coding: utf-8 -*-
"""
配置文件 - 项目参数配置
定义数据源、Kafka、数仓、指标计算等核心参数
"""

import os

# 项目根目录
PROJECT_ROOT = "/app/data/所有对话/主对话/Vehicle_Realtime_Warehouse"

# 数据目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
ODS_DIR = os.path.join(DATA_DIR, "ods")
DWD_DIR = os.path.join(DATA_DIR, "dwd")
DWS_DIR = os.path.join(DATA_DIR, "dws")

# 输出目录
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")
REPORTS_DIR = os.path.join(OUTPUT_DIR, "reports")

# 确保目录存在
for dir_path in [DATA_DIR, RAW_DIR, ODS_DIR, DWD_DIR, DWS_DIR, OUTPUT_DIR, FIGURES_DIR, REPORTS_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# ============== 数据源配置 ==============
DATA_SOURCE = {
    "vehicle_count": 10,           # 车辆数量
    "records_per_vehicle": 50000,  # 每辆车记录数
    "interval_seconds": 5,        # 数据采集间隔（秒）
}

# CAN总线信号字段定义
CAN_FIELDS = {
    "timestamp": "时间戳",
    "vehicle_id": "车辆ID",
    "speed": "车速(km/h)",
    "battery_voltage": "电池电压(V)",
    "battery_current": "电池电流(A)",
    "battery_temperature": "电池温度(°C)",
    "motor_temperature": "电机温度(°C)",
    "motor_rpm": "电机转速(rpm)",
    "soc": "电池SOC(%)",
    "throttle": "油门开度(%)",
    "brake": "制动信号",
    "charging_status": "充电状态",
    "energy_consumption": "能耗(kWh/100km)",
    "cabin_temperature": "舱内温度(°C)",
    "latitude": "纬度",
    "longitude": "经度"
}

# 数值字段列表
NUMERIC_FIELDS = [
    "speed", "battery_voltage", "battery_current", "battery_temperature",
    "motor_temperature", "motor_rpm", "soc", "throttle", "brake",
    "energy_consumption", "cabin_temperature", "latitude", "longitude"
]

# ============== 物理范围约束 ==============
PHYSICAL_RANGES = {
    "speed": (0, 200),                    # 车速: 0-200 km/h
    "battery_voltage": (200, 450),         # 电池电压: 200-450V
    "battery_current": (-200, 200),        # 电池电流: -200-200A（负值表示充电）
    "battery_temperature": (-20, 60),      # 电池温度: -20-60°C
    "motor_temperature": (-20, 150),       # 电机温度: -20-150°C
    "motor_rpm": (0, 15000),               # 电机转速: 0-15000 rpm
    "soc": (0, 100),                       # SOC: 0-100%
    "throttle": (0, 100),                  # 油门: 0-100%
    "brake": (0, 1),                       # 制动: 0-1
    "energy_consumption": (0, 50),         # 能耗: 0-50 kWh/100km
    "cabin_temperature": (-10, 50),        # 舱内温度: -10-50°C
    "latitude": (18, 54),                  # 中国纬度范围
    "longitude": (73, 135)                 # 中国经度范围
}

# ============== Kafka配置 ==============
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic_can_data": "vehicle_can_data",        # CAN原始数据主题
    "topic_realtime_metrics": "realtime_metrics", # 实时指标主题
    "consumer_group": "flink_consumer_group",
    "auto_offset_reset": "latest",
}

# ============== Flink配置 ==============
FLINK_CONFIG = {
    "window_size_seconds": 5,           # 滑动窗口大小（秒）
    "slide_size_seconds": 1,            # 滑动步长（秒）
    "checkpoint_interval": 30,          # 检查点间隔（秒）
    "parallelism": 1,                   # 并行度
}

# ============== Spark配置 ==============
SPARK_CONFIG = {
    "app_name": "VehicleRealtimeWarehouse",
    "master": "local[*]",
    "driver_memory": "2g",
    "executor_memory": "2g",
    "shuffle_partitions": 8,
}

# ============== 数据质量配置 ==============
DATA_QUALITY_CONFIG = {
    "dedup_key": ["vehicle_id", "timestamp"],  # 去重键
    "completeness_threshold": 99.0,              # 完整性阈值（%）
    "duplicate_threshold": 5.0,                 # 重复率阈值（%）
    "null_fill_strategy": {                     # 空值填充策略
        "speed": "ffill",                         # 前向填充
        "soc": "ffill",
        "default": "median"                       # 默认：中位数填充
    },
    "outlier_method": "iqr",                     # 异常值检测方法：iqr 或 physical
}

# ============== 8个核心指标配置 ==============
CORE_METRICS = {
    "daily_energy_consumption": {
        "name": "日均电耗",
        "unit": "kWh/100km",
        "description": "每日每百公里平均能耗"
    },
    "rapid_acceleration_count": {
        "name": "急加速次数",
        "unit": "次",
        "description": "加速度超过阈值的次数"
    },
    "high_temp_duration": {
        "name": "高温累计时长",
        "unit": "小时",
        "description": "电池温度超过安全阈值(45°C)的累计时长"
    },
    "daily_mileage": {
        "name": "日均行驶里程",
        "unit": "km",
        "description": "每日平均行驶里程"
    },
    "charging_frequency": {
        "name": "充电频次",
        "unit": "次",
        "description": "每日充电次数"
    },
    "average_speed": {
        "name": "平均车速",
        "unit": "km/h",
        "description": "统计周期内平均车速"
    },
    "brake_energy_recovery_rate": {
        "name": "制动能量回收率",
        "unit": "%",
        "description": "制动能量回收占总能耗的比例"
    },
    "battery_health_index": {
        "name": "电池健康指数",
        "unit": "%",
        "description": "基于SOH和衰减规律的电池健康度"
    }
}

# ============== Streamlit配置 ==============
STREAMLIT_CONFIG = {
    "page_title": "智能网联车辆实时数据监控平台",
    "page_icon": "🚗",
    "layout": "wide",
    "theme": {
        "primaryColor": "#1f77b4",
        "backgroundColor": "#f0f2f6",
        "secondaryBackgroundColor": "#ffffff",
        "textColor": "#262730",
    }
}

# ============== 爬虫配置 ==============
SCRAPER_CONFIG = {
    "timeout": 30,                    # 请求超时（秒）
    "retry_times": 3,                 # 重试次数
    "retry_delay": 5,                 # 重试延迟（秒）
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
}

# ============== 日志配置 ==============
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S"
}
