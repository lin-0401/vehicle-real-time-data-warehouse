# -*- coding: utf-8 -*-
"""
模拟CAN流数据生成器 - 生成真实的车辆CAN总线数据
基于真实驾驶规律，包括城市拥堵、高速巡航、急加速、充电等场景
"""

import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import config


class VehicleSimulator:
    """
    车辆模拟器 - 模拟单辆车的行驶数据
    
    基于真实驾驶规律设计，包含：
    - 城市驾驶：频繁启停、加减速
    - 高速巡航：稳定高速行驶
    - 急加速/急减速场景
    - 充电场景：电压电流变化
    - 温度变化：电池和电机的温升
    """
    
    def __init__(self, vehicle_id: str, seed: Optional[int] = None):
        self.vehicle_id = vehicle_id
        if seed is not None:
            np.random.seed(seed)
        
        # 车辆基础参数
        self.battery_capacity = np.random.uniform(60, 100)  # 电池容量 kWh
        self.initial_soc = np.random.uniform(20, 90)        # 初始SOC
        self.max_speed = np.random.uniform(140, 180)        # 最高车速 km/h
        
        # 当前状态
        self.current_soc = self.initial_soc
        self.current_speed = 0
        self.total_distance = 0
        
    def _get_scenario(self, timestamp_idx: int, total_records: int) -> str:
        """
        根据时间索引确定驾驶场景
        
        场景分布（约）：
        - 0-20%: 城市驾驶（拥堵）
        - 20-40%: 城市驾驶（正常）
        - 40-60%: 高速巡航
        - 60-80%: 郊区行驶
        - 80-100%: 到达/充电场景
        """
        progress = timestamp_idx / total_records
        
        if progress < 0.05:
            return "startup"          # 启动
        elif progress < 0.20:
            return "city_congestion"   # 城市拥堵
        elif progress < 0.40:
            return "city_normal"      # 城市正常
        elif progress < 0.60:
            return "highway"          # 高速巡航
        elif progress < 0.75:
            return "suburban"         # 郊区行驶
        elif progress < 0.85:
            return "sport"            # 运动模式（急加速）
        elif progress < 0.95:
            return "driving_home"     # 回家途中
        else:
            return "charging"         # 充电场景
            
    def _generate_speed_profile(self, scenario: str, prev_speed: float) -> float:
        """根据场景生成目标速度"""
        base_noise = np.random.normal(0, 2)
        
        if scenario == "startup":
            target = np.random.uniform(0, 30)
        elif scenario == "city_congestion":
            # 拥堵：低速，频繁启停
            if np.random.random() < 0.2:  # 20%概率急刹
                target = 0
            else:
                target = np.random.uniform(10, 40) + base_noise
        elif scenario == "city_normal":
            # 城市正常：中等速度
            target = np.random.uniform(30, 60) + base_noise
        elif scenario == "highway":
            # 高速：高速稳定
            target = np.random.uniform(100, 130) + base_noise
        elif scenario == "suburban":
            # 郊区：中等速度
            target = np.random.uniform(50, 80) + base_noise
        elif scenario == "sport":
            # 运动模式：速度波动大
            target = prev_speed + np.random.uniform(-20, 30)
            target = np.clip(target, 0, self.max_speed)
        elif scenario == "driving_home":
            # 回家：速度下降
            target = np.random.uniform(30, 70) + base_noise
        elif scenario == "charging":
            # 充电：静止
            target = 0
        else:
            target = prev_speed
            
        return np.clip(target, 0, self.max_speed)
    
    def _generate_battery_params(self, speed: float, scenario: str, 
                                  battery_temp: float, dt: float) -> Dict:
        """
        生成电池参数
        
        物理模型：
        - 放电电流与车速成正比
        - 充电电流为负值
        - 温度随充放电而升高
        """
        if scenario == "charging":
            # 充电模式
            voltage = np.random.uniform(350, 420)
            # 快充vs慢充
            if np.random.random() < 0.3:  # 30%快充
                current = np.random.uniform(-150, -80)  # 快充电流
                soc_change = np.random.uniform(0.3, 0.5)  # 每5秒SOC增加
            else:
                current = np.random.uniform(-30, -15)   # 慢充电流
                soc_change = np.random.uniform(0.1, 0.2)
            
            self.current_soc = min(100, self.current_soc + soc_change)
            temp_change = np.random.uniform(-0.1, 0.2)  # 充电时温度缓慢上升
            
        else:
            # 放电模式（行驶）
            power_factor = speed / 100  # 功率因子
            base_current = 20 + power_factor * 80  # 基础放电电流
            
            # 急加速时电流增大
            if scenario == "sport":
                base_current += np.random.uniform(20, 50)
            
            # 电流波动
            current = base_current + np.random.normal(0, 10)
            current = np.clip(current, -50, 150)
            
            # 电压随SOC变化
            voltage = 300 + (self.current_soc / 100) * 100 + np.random.normal(0, 5)
            voltage = np.clip(voltage, 280, 430)
            
            # SOC消耗
            energy_consumed = abs(current * voltage * dt / 3600) / 1000  # kWh
            soc_change = energy_consumed / self.battery_capacity * 100
            self.current_soc = max(0, self.current_soc - soc_change)
            
            # 温度变化：放电产生热量
            heat_generation = abs(current) / 100 * 0.5  # 热生成
            cooling = 0.2 * (25 - battery_temp)  # 散热（假设环境25°C）
            temp_change = heat_generation - cooling + np.random.normal(0, 0.1)
            
        battery_temp = max(-20, min(60, battery_temp + temp_change))
        
        return {
            "voltage": voltage,
            "current": current,
            "soc": self.current_soc,
            "temperature": battery_temp
        }
    
    def _generate_motor_params(self, speed: float, motor_temp: float, 
                                battery_current: float, dt: float) -> Dict:
        """生成电机参数"""
        # 电机转速与车速成正比
        gear_ratio = 8  # 减速比
        base_rpm = speed * gear_ratio / 0.3  # 估算公式
        
        # 急加速时转速波动
        if np.random.random() < 0.1:
            rpm = base_rpm * np.random.uniform(1.1, 1.5)
        else:
            rpm = base_rpm + np.random.normal(0, 100)
        rpm = np.clip(rpm, 0, 15000)
        
        # 电机温度跟随电池和负载
        heat_from_current = abs(battery_current) / 200 * 0.3
        heat_from_rpm = rpm / 15000 * 0.2
        cooling = 0.15 * (25 - motor_temp)
        motor_temp_change = heat_from_current + heat_from_rpm - cooling + np.random.normal(0, 0.1)
        motor_temp = max(-20, min(150, motor_temp + motor_temp_change))
        
        return {
            "rpm": rpm,
            "temperature": motor_temp
        }
    
    def generate_record(self, timestamp_idx: int, base_time: datetime, 
                        total_records: int) -> Dict:
        """
        生成单条CAN记录
        
        Args:
            timestamp_idx: 时间索引
            base_time: 基准时间
            total_records: 总记录数
            
        Returns:
            包含所有CAN字段的字典
        """
        dt = config.DATA_SOURCE["interval_seconds"]  # 时间间隔（秒）
        timestamp = base_time + timedelta(seconds=timestamp_idx * dt)
        
        scenario = self._get_scenario(timestamp_idx, total_records)
        
        # 生成速度
        speed = self._generate_speed_profile(scenario, self.current_speed)
        self.current_speed = speed
        
        # 更新里程
        distance = speed * dt / 3600  # km
        self.total_distance += distance
        
        # 生成电池参数
        battery_params = self._generate_battery_params(
            speed, scenario, 
            self._last_battery_temp if hasattr(self, '_last_battery_temp') else 25,
            dt
        )
        self._last_battery_temp = battery_params["temperature"]
        
        # 生成电机参数
        motor_params = self._generate_motor_params(
            speed,
            self._last_motor_temp if hasattr(self, '_last_motor_temp') else 25,
            battery_params["current"],
            dt
        )
        self._last_motor_temp = motor_params["temperature"]
        
        # 能耗计算
        power = battery_params["voltage"] * abs(battery_params["current"]) / 1000  # kW
        energy_consumption = (power * dt / 3600) / (distance + 0.001) * 100  # kWh/100km
        
        # 油门和制动
        if scenario == "sport":
            throttle = np.random.uniform(60, 100)
            brake = 0 if np.random.random() < 0.7 else np.random.uniform(0, 0.5)
        elif scenario == "city_congestion" and speed < 20:
            throttle = np.random.uniform(0, 30)
            brake = np.random.uniform(0, 0.8) if np.random.random() < 0.3 else 0
        else:
            throttle = np.random.uniform(20, 70) if speed > 0 else 0
            brake = 0 if np.random.random() < 0.8 else np.random.uniform(0, 0.3)
        
        # 充电状态
        charging_status = 1 if scenario == "charging" else 0
        
        # 舱内温度
        cabin_temp = 22 + np.random.normal(0, 2) + (1 - charging_status) * 0.01
        
        # 位置（模拟在城市范围内移动）
        if not hasattr(self, '_lat') or not hasattr(self, '_lon'):
            self._lat = np.random.uniform(30, 32)  # 初始化位置（模拟成都区域）
            self._lon = np.random.uniform(103, 105)
        
        # 位置随速度变化
        if speed > 0:
            direction = np.random.uniform(-1, 1)
            self._lat += direction * speed * dt / 3600 / 111 * 0.01
            self._lon += (1 - abs(direction)) * speed * dt / 3600 / (111 * np.cos(np.radians(30))) * 0.01
            # 保持在合理范围内
            self._lat = np.clip(self._lat, 29, 33)
            self._lon = np.clip(self._lon, 102, 106)
        
        return {
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "vehicle_id": self.vehicle_id,
            "speed": round(speed, 2),
            "battery_voltage": round(battery_params["voltage"], 1),
            "battery_current": round(battery_params["current"], 1),
            "battery_temperature": round(battery_params["temperature"], 1),
            "motor_temperature": round(motor_params["temperature"], 1),
            "motor_rpm": round(motor_params["rpm"], 0),
            "soc": round(battery_params["soc"], 1),
            "throttle": round(throttle, 1),
            "brake": round(brake, 2),
            "charging_status": charging_status,
            "energy_consumption": round(max(0, energy_consumption), 2),
            "cabin_temperature": round(cabin_temp, 1),
            "latitude": round(self._lat, 6),
            "longitude": round(self._lon, 6)
        }


class CANDataGenerator:
    """
    CAN总线数据生成器 - 生成多车辆完整数据集
    """
    
    def __init__(self):
        self.vehicle_count = config.DATA_SOURCE["vehicle_count"]
        self.records_per_vehicle = config.DATA_SOURCE["records_per_vehicle"]
        self.interval_seconds = config.DATA_SOURCE["interval_seconds"]
        
    def generate_fleet_data(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        生成车队数据
        
        Args:
            output_path: 输出CSV路径，None则不保存
            
        Returns:
            完整的车辆数据DataFrame
        """
        print(f"开始生成 {self.vehicle_count} 辆车、每车 {self.records_per_vehicle} 条记录的数据...")
        
        all_records = []
        base_time = datetime.now() - timedelta(hours=self.records_per_vehicle * self.interval_seconds / 3600)
        
        for vehicle_idx in range(self.vehicle_count):
            vehicle_id = f"EV_{vehicle_idx + 1:03d}"  # EV_001, EV_002, ...
            
            # 每辆车使用不同的随机种子，保证可重复性
            simulator = VehicleSimulator(vehicle_id, seed=42 + vehicle_idx)
            
            print(f"  生成车辆 {vehicle_id} 的数据...")
            
            for record_idx in range(self.records_per_vehicle):
                record = simulator.generate_record(record_idx, base_time, self.records_per_vehicle)
                all_records.append(record)
        
        df = pd.DataFrame(all_records)
        print(f"共生成 {len(df)} 条记录")
        
        # 保存到CSV
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"数据已保存到: {output_path}")
        
        return df
    
    def generate_stream_sample(self, vehicle_ids: List[str], 
                                num_records: int = 100) -> pd.DataFrame:
        """
        生成模拟实时流数据样本（用于演示）
        
        Args:
            vehicle_ids: 车辆ID列表
            num_records: 每辆车生成记录数
            
        Returns:
            实时流数据DataFrame
        """
        all_records = []
        base_time = datetime.now()
        
        for vehicle_id in vehicle_ids:
            simulator = VehicleSimulator(vehicle_id, seed=hash(vehicle_id) % 10000)
            
            for record_idx in range(num_records):
                record = simulator.generate_record(record_idx, base_time, num_records)
                all_records.append(record)
        
        return pd.DataFrame(all_records)


def main():
    """主函数 - 测试数据生成"""
    print("=" * 60)
    print("CAN总线数据生成器测试")
    print("=" * 60)
    
    generator = CANDataGenerator()
    
    # 生成完整数据集
    output_path = os.path.join(config.RAW_DIR, "vehicle_can_data.csv")
    df = generator.generate_fleet_data(output_path)
    
    # 显示数据概况
    print("\n数据概况:")
    print(df.describe())
    print(f"\n数据列: {list(df.columns)}")
    print(f"\n车辆ID分布:\n{df['vehicle_id'].value_counts()}")
    
    return df


if __name__ == "__main__":
    main()
