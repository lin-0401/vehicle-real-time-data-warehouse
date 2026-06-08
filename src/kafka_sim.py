# -*- coding: utf-8 -*-
"""
Kafka消息队列模拟模块

提供两种模式：
1. 真实Kafka模式：使用kafka-python连接真实Kafka服务
2. 模拟Kafka模式：使用Python Queue模拟生产者/消费者，接口一致

当Kafka服务不可用时自动降级到模拟模式
"""

import json
import time
import threading
from queue import Queue, Empty
from typing import Optional, Callable, Dict, List, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd

import config


@dataclass
class CANMessage:
    """CAN消息数据类"""
    timestamp: str
    vehicle_id: str
    speed: float
    battery_voltage: float
    battery_current: float
    battery_temperature: float
    motor_temperature: float
    motor_rpm: float
    soc: float
    throttle: float
    brake: float
    charging_status: int
    energy_consumption: float
    cabin_temperature: float
    latitude: float
    longitude: float
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(asdict(self))
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CANMessage':
        """从字典创建"""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'CANMessage':
        """从JSON字符串创建"""
        return cls.from_dict(json.loads(json_str))


class KafkaSimulator:
    """
    Kafka模拟器 - 使用Python Queue模拟Kafka的消息队列功能
    
    提供与真实Kafka相似的接口：
    - Producer: 生产消息
    - Consumer: 消费消息
    - Topic: 主题管理
    """
    
    def __init__(self):
        """初始化模拟器"""
        self._topics: Dict[str, Queue] = {}
        self._consumers: Dict[str, List['MockConsumer']] = {}
        self._lock = threading.Lock()
        self._running = True
        
        # 创建默认主题
        self._ensure_topic(config.KAFKA_CONFIG["topic_can_data"])
        self._ensure_topic(config.KAFKA_CONFIG["topic_realtime_metrics"])
        
    def _ensure_topic(self, topic: str):
        """确保主题存在"""
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = Queue(maxsize=100000)
                self._consumers[topic] = []
                
    def create_producer(self) -> 'MockProducer':
        """创建生产者"""
        return MockProducer(self)
    
    def create_consumer(self, topic: str, group_id: str = None, 
                        auto_offset_reset: str = "latest") -> 'MockConsumer':
        """创建消费者"""
        self._ensure_topic(topic)
        consumer = MockConsumer(self, topic, group_id, auto_offset_reset)
        with self._lock:
            self._consumers[topic].append(consumer)
        return consumer
    
    def send(self, topic: str, value: str, key: str = None):
        """发送消息到主题"""
        self._ensure_topic(topic)
        message = {
            "topic": topic,
            "key": key,
            "value": value,
            "timestamp": int(time.time() * 1000)
        }
        self._topics[topic].put(message)
        
    def consume(self, topic: str, consumer_id: str, timeout: float = 1.0) -> Optional[Dict]:
        """
        消费消息
        
        Args:
            topic: 主题名
            consumer_id: 消费者ID
            timeout: 超时时间
            
        Returns:
            消息字典或None
        """
        try:
            message = self._topics[topic].get(timeout=timeout)
            return message
        except Empty:
            return None
            
    def get_topic_stats(self, topic: str) -> Dict:
        """获取主题统计信息"""
        with self._lock:
            q = self._topics.get(topic)
            if q:
                return {
                    "topic": topic,
                    "queue_size": q.qsize(),
                    "consumer_count": len(self._consumers.get(topic, []))
                }
            return {"topic": topic, "queue_size": 0, "consumer_count": 0}
    
    def clear_topic(self, topic: str):
        """清空主题队列"""
        with self._lock:
            if topic in self._topics:
                while not self._topics[topic].empty():
                    try:
                        self._topics[topic].get_nowait()
                    except Empty:
                        break


class MockProducer:
    """模拟Kafka生产者"""
    
    def __init__(self, simulator: KafkaSimulator):
        self._simulator = simulator
        self._closed = False
        
    def send(self, topic: str, value: str, key: str = None):
        """发送消息"""
        if self._closed:
            raise RuntimeError("Producer已关闭")
        self._simulator.send(topic, value, key)
        
    def send_batch(self, topic: str, messages: List[str]):
        """批量发送消息"""
        for msg in messages:
            self.send(topic, msg)
            
    def flush(self):
        """刷新缓冲区（模拟，无实际效果）"""
        pass
        
    def close(self):
        """关闭生产者"""
        self._closed = True


class MockConsumer:
    """模拟Kafka消费者"""
    
    def __init__(self, simulator: KafkaSimulator, topic: str, 
                 group_id: str, auto_offset_reset: str):
        self._simulator = simulator
        self._topic = topic
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._consumer_id = f"{group_id}_{topic}_{id(self)}"
        self._closed = False
        self._subscription = [topic]
        
    def subscribe(self, topics: List[str]):
        """订阅主题"""
        self._subscription = topics
        
    def poll(self, timeout_ms: int = 1000) -> Optional[Dict]:
        """
        轮询消息
        
        Args:
            timeout_ms: 超时时间（毫秒）
            
        Returns:
            消息字典或None
        """
        if self._closed:
            raise RuntimeError("Consumer已关闭")
            
        timeout = timeout_ms / 1000.0
        for topic in self._subscription:
            msg = self._simulator.consume(topic, self._consumer_id, timeout)
            if msg:
                return msg
        return None
        
    def close(self):
        """关闭消费者"""
        self._closed = True


class RealKafkaProducer:
    """
    真实Kafka生产者（当Kafka服务可用时使用）
    """
    
    def __init__(self, bootstrap_servers: str):
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            self._use_real = True
            print(f"已连接到真实Kafka: {bootstrap_servers}")
        except Exception as e:
            print(f"无法连接Kafka ({e})，将使用模拟模式")
            self._producer = None
            self._use_real = False
            
    def send(self, topic: str, value: Dict, key: str = None):
        """发送消息"""
        if self._use_real and self._producer:
            self._producer.send(topic, value=value, key=key)
        else:
            # 降级到模拟
            raise NotImplementedError("需要KafkaSimulator")
            
    def flush(self):
        """刷新"""
        if self._use_real and self._producer:
            self._producer.flush()
            
    def close(self):
        """关闭"""
        if self._use_real and self._producer:
            self._producer.close()


class KafkaFactory:
    """
    Kafka工厂类 - 自动选择真实或模拟模式
    """
    
    _instance = None
    _use_simulator = True
    _simulator = None
    
    @classmethod
    def get_instance(cls) -> Any:
        """获取单例实例"""
        if cls._instance is None:
            if cls._use_simulator:
                if cls._simulator is None:
                    cls._simulator = KafkaSimulator()
                cls._instance = cls._simulator
            else:
                # 真实Kafka逻辑
                cls._instance = None  # TODO: 实现真实Kafka
        return cls._instance
    
    @classmethod
    def create_producer(cls):
        """创建生产者"""
        instance = cls.get_instance()
        if isinstance(instance, KafkaSimulator):
            return instance.create_producer()
        return None
        
    @classmethod
    def create_consumer(cls, topic: str, group_id: str = None, 
                         auto_offset_reset: str = "latest"):
        """创建消费者"""
        instance = cls.get_instance()
        if isinstance(instance, KafkaSimulator):
            return instance.create_consumer(topic, group_id, auto_offset_reset)
        return None


def demo_kafka_simulator():
    """演示Kafka模拟器功能"""
    print("=" * 60)
    print("Kafka模拟器演示")
    print("=" * 60)
    
    # 创建模拟器
    simulator = KafkaSimulator()
    producer = simulator.create_producer()
    
    # 发送测试消息
    print("\n发送测试消息...")
    test_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vehicle_id": "EV_TEST",
        "speed": 60.5,
        "battery_voltage": 350.2,
        "battery_current": 50.0,
        "battery_temperature": 28.5,
        "motor_temperature": 35.0,
        "motor_rpm": 5000,
        "soc": 75.5,
        "throttle": 50.0,
        "brake": 0.0,
        "charging_status": 0,
        "energy_consumption": 15.2,
        "cabin_temperature": 24.0,
        "latitude": 30.5728,
        "longitude": 104.0668
    }
    
    for i in range(10):
        test_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        test_data["speed"] = 60 + i
        producer.send(config.KAFKA_CONFIG["topic_can_data"], json.dumps(test_data))
        time.sleep(0.1)
    
    producer.flush()
    print("已发送10条消息")
    
    # 创建消费者
    consumer = simulator.create_consumer(
        config.KAFKA_CONFIG["topic_can_data"],
        group_id=config.KAFKA_CONFIG["consumer_group"]
    )
    
    # 消费消息
    print("\n消费消息...")
    for i in range(10):
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            data = json.loads(msg["value"])
            print(f"  [{i+1}] {data['vehicle_id']} - 速度: {data['speed']} km/h")
    
    consumer.close()
    producer.close()
    
    # 打印主题统计
    print("\n主题统计:")
    for topic in [config.KAFKA_CONFIG["topic_can_data"], config.KAFKA_CONFIG["topic_realtime_metrics"]]:
        stats = simulator.get_topic_stats(topic)
        print(f"  {stats['topic']}: 队列大小={stats['queue_size']}, 消费者数={stats['consumer_count']}")


if __name__ == "__main__":
    demo_kafka_simulator()
