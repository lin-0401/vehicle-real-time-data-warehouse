# -*- coding: utf-8 -*-
"""
指标计算函数模块

定义8个核心指标的的计算逻辑：
1. 日均电耗 (daily_energy_consumption)
2. 急加速次数 (rapid_acceleration_count)
3. 高温累计时长 (high_temp_duration)
4. 日均行驶里程 (daily_mileage)
5. 充电频次 (charging_frequency)
6. 平均车速 (average_speed)
7. 制动能量回收率 (brake_energy_recovery_rate)
8. 电池健康指数 (battery_health_index)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

import config


class CoreMetricsCalculator:
    """
    核心指标计算器
    
    提供8个核心车辆运营指标的离线计算
    """
    
    # 急加速阈值：加速度超过此值认为是急加速 (km/h/s)
    RAPID_ACCEL_THRESHOLD = 5.0
    
    # 高温阈值：电池温度超过此值认为是高温 (°C)
    HIGH_TEMP_THRESHOLD = 45.0
    
    # 制动能量回收的SOC回升阈值
    BRAKE_SOC_RECOVERY_THRESHOLD = 0.5  # %
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化计算器
        
        Args:
            df: 车辆CAN数据DataFrame，需要包含timestamp列
        """
        self.df = df.copy()
        self._ensure_time_columns()
        
    def _ensure_time_columns(self):
        """确保时间列存在"""
        if 'timestamp' in self.df.columns:
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
            
        if 'date' not in self.df.columns:
            self.df['date'] = self.df['timestamp'].dt.date
            
    def calculate_all_metrics(self) -> Dict[str, pd.DataFrame]:
        """
        计算所有核心指标
        
        Returns:
            指标名称到DataFrame的映射
        """
        metrics = {}
        
        print("\n计算8个核心指标...")
        
        # 1. 日均电耗
        print("  [1/8] 日均电耗...")
        metrics['daily_energy_consumption'] = self._calc_daily_energy_consumption()
        
        # 2. 急加速次数
        print("  [2/8] 急加速次数...")
        metrics['rapid_acceleration_count'] = self._calc_rapid_acceleration()
        
        # 3. 高温累计时长
        print("  [3/8] 高温累计时长...")
        metrics['high_temp_duration'] = self._calc_high_temp_duration()
        
        # 4. 日均行驶里程
        print("  [4/8] 日均行驶里程...")
        metrics['daily_mileage'] = self._calc_daily_mileage()
        
        # 5. 充电频次
        print("  [5/8] 充电频次...")
        metrics['charging_frequency'] = self._calc_charging_frequency()
        
        # 6. 平均车速
        print("  [6/8] 平均车速...")
        metrics['average_speed'] = self._calc_average_speed()
        
        # 7. 制动能量回收率
        print("  [7/8] 制动能量回收率...")
        metrics['brake_energy_recovery_rate'] = self._calc_brake_energy_recovery()
        
        # 8. 电池健康指数
        print("  [8/8] 电池健康指数...")
        metrics['battery_health_index'] = self._calc_battery_health()
        
        return metrics
    
    def _calc_daily_energy_consumption(self) -> pd.DataFrame:
        """
        计算日均电耗
        
        公式: 日均电耗 = Σ(电压 × 电流 × 时间间隔) / Σ(行驶里程) × 100
        单位: kWh/100km
        
        Returns:
            包含vehicle_id, date, daily_energy_consumption的DataFrame
        """
        # 计算每条记录的能耗 (kWh)
        # 功率(kW) = 电压(V) × 电流(A) / 1000
        # 能耗(kWh) = 功率(kW) × 时间(h)
        interval_hours = config.DATA_SOURCE["interval_seconds"] / 3600
        
        self.df['power'] = self.df['battery_voltage'] * abs(self.df['battery_current']) / 1000
        self.df['energy'] = self.df['power'] * interval_hours
        
        # 计算每条记录的行驶里程 (km)
        self.df['distance'] = self.df['speed'] * interval_hours
        
        # 按车辆和日期聚合
        result = self.df.groupby(['vehicle_id', 'date']).agg({
            'energy': 'sum',           # 总能耗
            'distance': 'sum',        # 总里程
            'speed': 'mean'           # 平均速度
        }).reset_index()
        
        # 计算百公里电耗
        result['daily_energy_consumption'] = np.where(
            result['distance'] > 0,
            result['energy'] / result['distance'] * 100,
            0
        )
        
        result = result.rename(columns={
            'energy': 'total_energy_kwh',
            'distance': 'total_distance_km'
        })
        
        return result[['vehicle_id', 'date', 'total_energy_kwh', 'total_distance_km', 'daily_energy_consumption']]
    
    def _calc_rapid_acceleration(self) -> pd.DataFrame:
        """
        计算急加速次数
        
        急加速定义：加速度超过阈值的次数
        加速度 = (当前速度 - 前一速度) / 时间间隔
        
        Returns:
            包含vehicle_id, date, rapid_acceleration_count的DataFrame
        """
        # 按车辆排序
        df = self.df.sort_values(['vehicle_id', 'timestamp'])
        
        # 计算加速度
        df['speed_diff'] = df.groupby('vehicle_id')['speed'].diff()
        df['time_diff'] = df.groupby('vehicle_id')['timestamp'].diff().dt.total_seconds()
        df['acceleration'] = df['speed_diff'] / (df['time_diff'] + 0.001)
        
        # 判断急加速
        df['is_rapid_accel'] = df['acceleration'] > self.RAPID_ACCEL_THRESHOLD
        
        # 按日期聚合
        result = df.groupby(['vehicle_id', 'date']).agg({
            'is_rapid_accel': 'sum'
        }).reset_index()
        
        result = result.rename(columns={'is_rapid_accel': 'rapid_acceleration_count'})
        result['rapid_acceleration_count'] = result['rapid_acceleration_count'].astype(int)
        
        return result
    
    def _calc_high_temp_duration(self) -> pd.DataFrame:
        """
        计算高温累计时长
        
        高温定义：电池温度超过HIGH_TEMP_THRESHOLD的累计时长
        
        Returns:
            包含vehicle_id, date, high_temp_duration_hours的DataFrame
        """
        interval_hours = config.DATA_SOURCE["interval_seconds"] / 3600
        
        # 判断高温
        self.df['is_high_temp'] = self.df['battery_temperature'] > self.HIGH_TEMP_THRESHOLD
        
        # 按日期聚合
        result = self.df.groupby(['vehicle_id', 'date']).agg({
            'is_high_temp': 'sum'
        }).reset_index()
        
        # 转换为小时
        result['high_temp_duration_hours'] = result['is_high_temp'] * interval_hours
        result = result.drop('is_high_temp', axis=1)
        
        return result
    
    def _calc_daily_mileage(self) -> pd.DataFrame:
        """
        计算日均行驶里程
        
        Returns:
            包含vehicle_id, date, total_mileage_km, driving_time_hours的DataFrame
        """
        interval_hours = config.DATA_SOURCE["interval_seconds"] / 3600
        
        # 只有车速>0才算行驶
        self.df['is_driving'] = self.df['speed'] > 0
        
        # 计算行驶里程
        self.df['distance'] = np.where(
            self.df['is_driving'],
            self.df['speed'] * interval_hours,
            0
        )
        
        # 按日期聚合
        result = self.df.groupby(['vehicle_id', 'date']).agg({
            'distance': 'sum',
            'is_driving': 'sum'
        }).reset_index()
        
        result = result.rename(columns={
            'distance': 'total_mileage_km',
            'is_driving': 'driving_records'
        })
        result['driving_time_hours'] = result['driving_records'] * interval_hours
        result = result.drop('driving_records', axis=1)
        
        return result
    
    def _calc_charging_frequency(self) -> pd.DataFrame:
        """
        计算充电频次
        
        充电频次定义：一天内充电状态从非充电变为充电的次数
        
        Returns:
            包含vehicle_id, date, charging_frequency的DataFrame
        """
        # 按车辆排序
        df = self.df.sort_values(['vehicle_id', 'timestamp'])
        
        # 检测充电状态变化
        df['charging_status_change'] = df.groupby('vehicle_id')['charging_status'].diff()
        
        # 充电开始：状态从0变为1
        df['charging_start'] = (df['charging_status_change'] == 1)
        
        # 按日期聚合
        result = df.groupby(['vehicle_id', 'date']).agg({
            'charging_start': 'sum'
        }).reset_index()
        
        result = result.rename(columns={'charging_start': 'charging_frequency'})
        result['charging_frequency'] = result['charging_frequency'].astype(int)
        
        return result
    
    def _calc_average_speed(self) -> pd.DataFrame:
        """
        计算平均车速
        
        Returns:
            包含vehicle_id, date, average_speed, max_speed, min_speed的DataFrame
        """
        result = self.df.groupby(['vehicle_id', 'date']).agg({
            'speed': ['mean', 'max', 'min', 'std']
        }).reset_index()
        
        result.columns = ['vehicle_id', 'date', 'average_speed', 'max_speed', 'min_speed', 'speed_std']
        result['average_speed'] = result['average_speed'].round(2)
        result['max_speed'] = result['max_speed'].round(2)
        result['min_speed'] = result['min_speed'].round(2)
        result['speed_std'] = result['speed_std'].round(2)
        
        return result
    
    def _calc_brake_energy_recovery(self) -> pd.DataFrame:
        """
        计算制动能量回收率
        
        公式: 制动能量回收率 = 充电消耗能量 / 总能耗 × 100%
        制动时电池电流为负（充电）
        
        Returns:
            包含vehicle_id, date, brake_energy_recovery_rate, total_recovered_energy的DataFrame
        """
        interval_hours = config.DATA_SOURCE["interval_seconds"] / 3600
        
        # 计算功率
        self.df['power'] = self.df['battery_voltage'] * self.df['battery_current'] / 1000
        
        # 制动能量回收：电流为负时的能量（充电）
        self.df['recovered_energy'] = np.where(
            self.df['battery_current'] < 0,
            abs(self.df['power']) * interval_hours,
            0
        )
        
        # 总能耗
        self.df['total_energy'] = np.where(
            self.df['battery_current'] > 0,
            self.df['power'] * interval_hours,
            0
        )
        
        # 按日期聚合
        result = self.df.groupby(['vehicle_id', 'date']).agg({
            'recovered_energy': 'sum',
            'total_energy': 'sum'
        }).reset_index()
        
        result['total_recovered_energy'] = result['recovered_energy']
        result['total_consumed_energy'] = result['total_energy']
        
        # 计算回收率
        result['brake_energy_recovery_rate'] = np.where(
            result['total_consumed_energy'] > 0,
            result['total_recovered_energy'] / result['total_consumed_energy'] * 100,
            0
        )
        result['brake_energy_recovery_rate'] = result['brake_energy_recovery_rate'].round(2)
        
        return result[['vehicle_id', 'date', 'total_recovered_energy', 
                       'total_consumed_energy', 'brake_energy_recovery_rate']]
    
    def _calc_battery_health(self) -> pd.DataFrame:
        """
        计算电池健康指数 (SOH - State of Health)
        
        基于以下因素估算：
        1. SOC循环次数
        2. 高温暴露时长
        3. 深充深放次数
        
        简化模型: SOH = 100 - 衰减因子
        
        Returns:
            包含vehicle_id, date, battery_health_index的DataFrame
        """
        # 基准SOH
        base_soh = 100.0
        
        # 1. 高温衰减因子
        high_temp_hours = self._calc_high_temp_duration()
        high_temp_factor = high_temp_hours.set_index(['vehicle_id', 'date'])['high_temp_duration_hours']
        
        # 2. 计算SOC循环
        df = self.df.sort_values(['vehicle_id', 'timestamp'])
        df['soc_diff'] = df.groupby('vehicle_id')['soc'].diff().abs()
        df['soc_cycle'] = df['soc_diff'] > 20  # 充放电超过20%算一个循环
        
        soc_cycles = df.groupby(['vehicle_id', 'date']).agg({
            'soc_cycle': 'sum'
        }).reset_index()
        soc_cycles.columns = ['vehicle_id', 'date', 'soc_cycles']
        
        # 3. 合并计算SOH
        result = high_temp_hours.merge(soc_cycles, on=['vehicle_id', 'date'], how='left')
        result['soc_cycles'] = result['soc_cycles'].fillna(0)
        
        # SOH计算公式（简化）
        # 每高温小时衰减0.1%，每次SOC循环衰减0.01%
        result['battery_health_index'] = (
            base_soh 
            - result['high_temp_duration_hours'] * 0.1 
            - result['soc_cycles'] * 0.01
        )
        result['battery_health_index'] = result['battery_health_index'].clip(lower=0, upper=100)
        result['battery_health_index'] = result['battery_health_index'].round(2)
        
        return result[['vehicle_id', 'date', 'battery_health_index', 
                       'high_temp_duration_hours', 'soc_cycles']]
    
    def get_summary_statistics(self, metrics: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        获取所有指标的汇总统计
        
        Args:
            metrics: calculate_all_metrics返回的指标字典
            
        Returns:
            汇总统计DataFrame
        """
        summary_data = []
        
        for metric_name, df in metrics.items():
            if df is None or df.empty:
                continue
                
            # 获取主要数值列
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col not in ['vehicle_id', 'date']:
                    summary_data.append({
                        'metric': metric_name,
                        'column': col,
                        'mean': round(df[col].mean(), 2),
                        'std': round(df[col].std(), 2),
                        'min': round(df[col].min(), 2),
                        'max': round(df[col].max(), 2),
                        'median': round(df[col].median(), 2)
                    })
                    
        return pd.DataFrame(summary_data)


def calculate_realtime_metric(record: Dict, prev_record: Optional[Dict] = None) -> Dict:
    """
    计算单条记录的实时指标
    
    Args:
        record: 当前CAN记录
        prev_record: 前一条记录（用于计算变化率）
        
    Returns:
        实时指标字典
    """
    # 瞬时功率
    power = record['battery_voltage'] * record['battery_current'] / 1000  # kW
    
    metrics = {
        'timestamp': record['timestamp'],
        'vehicle_id': record['vehicle_id'],
        'instantaneous_power_kw': round(power, 2),
        'soc': record['soc'],
        'battery_temp': record['battery_temperature'],
        'motor_temp': record['motor_temperature'],
        'speed': record['speed']
    }
    
    if prev_record:
        # 时间差
        try:
            curr_time = datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S")
            prev_time = datetime.strptime(prev_record['timestamp'], "%Y-%m-%d %H:%M:%S")
            dt = (curr_time - prev_time).total_seconds()
        except:
            dt = config.DATA_SOURCE["interval_seconds"]
            
        if dt > 0:
            # 温升速率
            metrics['battery_temp_rate'] = round(
                (record['battery_temperature'] - prev_record['battery_temperature']) / dt, 4
            )
            metrics['motor_temp_rate'] = round(
                (record['motor_temperature'] - prev_record['motor_temperature']) / dt, 4
            )
            metrics['soc_rate_per_hour'] = round(
                (record['soc'] - prev_record['soc']) / dt * 3600, 4
            )
            
    return metrics


def demo_metrics():
    """演示指标计算"""
    from src.data_generator import CANDataGenerator
    
    print("=" * 60)
    print("核心指标计算演示")
    print("=" * 60)
    
    # 生成测试数据
    generator = CANDataGenerator()
    test_data = generator.generate_fleet_data()
    
    # 取样计算
    sample_size = 10000
    sample_data = test_data.head(sample_size)
    
    # 计算指标
    calculator = CoreMetricsCalculator(sample_data)
    metrics = calculator.calculate_all_metrics()
    
    # 打印汇总
    print("\n指标汇总统计:")
    summary = calculator.get_summary_statistics(metrics)
    print(summary.to_string(index=False))
    
    return metrics


if __name__ == "__main__":
    demo_metrics()
