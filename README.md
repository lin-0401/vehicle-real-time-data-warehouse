# 智能网联车辆实时指标计算与数仓建设项目

## 📖 项目概述

这是一个基于车辆CAN总线数据的实时数据流处理与离线数仓建设项目，完整实现了：

- **实时计算链路**：PyFlink + Kafka模拟，实现5秒滑动窗口的瞬时能耗、温升速率等实时指标计算
- **离线数仓链路**：PySpark SQL（降级Pandas）实现ODS→DWD→DWS三层数据仓库
- **数据质量保障**：去重、空值填充、异常值检测，保障数据完整性≥99%
- **可视化展示**：Streamlit + Plotly 仪表盘，替代传统Power BI

## 🏗️ 技术架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           智能网联车辆数据平台                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  数据源 ──┬── CAN总线数据 (真实采集)                                         │
│           ├── 数据爬虫 (Figshare/Zenodo API)                                │
│           └── 数据生成器 (模拟CAN流数据)                                      │
│                                                                             │
│           ┌────────────────────────────────────────────────────┐             │
│           │              Kafka消息队列 (模拟/真实)              │             │
│           └────────────────────────────────────────────────────┘             │
│                            │                                               │
│           ┌────────────────┼────────────────┐                              │
│           ▼                ▼                ▼                               │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                     │
│  │ PyFlink实时计算│ │    ODS层       │ │    ODS层       │                     │
│  │ (5秒滑动窗口)  │ │  (原始数据)    │ │  (数据质量)    │                     │
│  └────────────────┘ └────────────────┘ └────────────────┘                     │
│                            │                                               │
│                            ▼                                               │
│                   ┌────────────────┐                                       │
│                   │   DWD层明细数据  │                                       │
│                   │  (清洗+派生字段) │                                       │
│                   └────────────────┘                                       │
│                            │                                               │
│                            ▼                                               │
│                   ┌────────────────┐                                       │
│                   │   DWS层汇总指标  │                                       │
│                   │  (8个核心指标)  │                                       │
│                   └────────────────┘                                       │
│                            │                                               │
│                            ▼                                               │
│                   ┌────────────────┐                                       │
│                   │ Streamlit可视化 │                                       │
│                   │  (5个页面)      │                                       │
│                   └────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 技术栈

| 层次 | 技术 | 说明 |
|------|------|------|
| 实时计算 | PyFlink | 5秒滑动窗口，瞬时能耗、温升速率计算 |
| 消息队列 | Kafka模拟 | 支持真实Kafka和Python Queue模拟 |
| 离线数仓 | PySpark SQL | 本地模式，ODS→DWD→DWS三层架构 |
| 数据质量 | 自研模块 | 去重、空值填充、异常值检测 |
| 可视化 | Streamlit + Plotly | 实时监控、指标看板、质量报告 |

## 📋 环境要求

### Python版本
- Python 3.8 - 3.11

### 安装依赖

```bash
pip install -r requirements.txt
```

### 关键依赖说明

| 依赖 | 必需 | 说明 |
|------|------|------|
| pandas | ✅ | 数据处理核心 |
| numpy | ✅ | 数值计算 |
| pyflink | ❌ | 可选，无则使用简化实现 |
| pyspark | ❌ | 可选，无则降级为Pandas |
| streamlit | ✅ | Web可视化 |
| plotly | ✅ | 图表绘制 |
| requests | ✅ | 网络请求（爬虫） |

## 🚀 快速开始

### 一键运行（推荐）

```bash
cd /app/data/所有对话/主对话/Vehicle_Realtime_Warehouse/
python main.py
```

这将自动执行完整流水线：
1. 数据获取（爬虫 + 模拟数据生成）
2. 数据质量检查与清洗
3. ODS层导入
4. DWD层处理
5. DWS层计算（8个核心指标）
6. PyFlink实时流计算
7. 生成报告与可视化数据

### 启动可视化仪表盘

```bash
# 方法1：直接运行
streamlit run app.py

# 方法2：指定端口
streamlit run app.py --server.port 8501

# 方法3：指定主机（远程访问）
streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

然后在浏览器中打开 http://localhost:8501

### 分步运行

```bash
# 1. 生成模拟数据
python -c "from src.data_generator import CANDataGenerator; g=CANDataGenerator(); g.generate_fleet_data()"

# 2. 数据质量检查
python -c "from src.data_quality import demo_data_quality; demo_data_quality()"

# 3. 运行数仓流水线
python -c "from src.spark_warehouse import demo_warehouse; demo_warehouse()"

# 4. 运行Flink实时计算
python -c "from src.flink_stream import run_flink_pipeline; run_flink_pipeline('data/raw/vehicle_can_data.csv', 'output/realtime_metrics.csv')"

# 5. 启动可视化
streamlit run app.py
```

## 🗃️ 数仓三层模型

### ODS层 (Operational Data Store)

**职责**：接收原始CAN数据，只做格式统一，不做清洗

**存储路径**：`data/ods/ods_data.csv`

**表结构**：

| 字段名 | 数据类型 | 说明 |
|--------|----------|------|
| timestamp | string | 时间戳，格式：YYYY-MM-DD HH:MM:SS |
| vehicle_id | string | 车辆ID，格式：EV_XXX |
| speed | float | 实时车速，单位：km/h |
| battery_voltage | float | 电池总电压，单位：V |
| battery_current | float | 电池总电流，单位：A |
| battery_temperature | float | 电池温度，单位：°C |
| motor_temperature | float | 电机温度，单位：°C |
| motor_rpm | float | 电机转速，单位：rpm |
| soc | float | 电池荷电状态，单位：% |
| throttle | float | 油门开度，单位：% |
| brake | float | 制动信号，0-1 |
| charging_status | int | 充电状态，0-停止，1-充电 |
| energy_consumption | float | 瞬时能耗，单位：kWh/100km |
| cabin_temperature | float | 舱内温度，单位：°C |
| latitude | float | 纬度坐标 |
| longitude | float | 经度坐标 |

### DWD层 (Data Warehouse Detail)

**职责**：明细数据清洗和标准化

**存储路径**：`data/dwd/dwd_data.csv`

**处理内容**：

1. **去重**：基于 `(vehicle_id, timestamp)` 组合键去重
2. **空值填充**：
   - 前向填充(ffill)：speed, soc
   - 中位数填充：其他数值字段
3. **异常值处理**：物理范围约束（见下表）

| 字段 | 物理范围 | 处理方式 |
|------|----------|----------|
| speed | 0-200 km/h | 裁剪 |
| battery_voltage | 200-450 V | 裁剪 |
| battery_current | -200-200 A | 裁剪 |
| battery_temperature | -20-60 °C | 裁剪 |
| motor_temperature | -20-150 °C | 裁剪 |
| motor_rpm | 0-15000 rpm | 裁剪 |
| soc | 0-100 % | 裁剪 |
| throttle | 0-100 % | 裁剪 |
| energy_consumption | 0-50 kWh/100km | 裁剪 |

**新增派生字段**：

| 字段名 | 说明 |
|--------|------|
| driving_status | 行驶状态：idle/normal/fast/highway |
| energy_category | 能耗等级：A_excellent/B_good/C_average/D_poor |
| temp_status | 温度状态：cold/normal/warm/hot |
| battery_status | 电池状态：low_power/high_power/high_temp_warning/normal |

### DWS层 (Data Warehouse Service)

**职责**：汇总数据服务层，计算8个核心指标

**存储路径**：`data/dws/dws_data.csv`
**指标文件**：`data/dws/metrics/*.csv`

## 📊 8个核心指标

### 1. 日均电耗 (daily_energy_consumption)

**定义**：每日每百公里平均能耗

**计算公式**：
```
日均电耗 = Σ(电压 × 电流 × 时间间隔) / Σ(行驶里程) × 100
```

**单位**：kWh/100km

---

### 2. 急加速次数 (rapid_acceleration_count)

**定义**：急加速事件发生次数

**判定条件**：加速度 > 5 km/h/s

**计算公式**：
```
加速度 = (当前速度 - 前一时刻速度) / 时间间隔
急加速 = count(加速度 > 5)
```

**单位**：次

---

### 3. 高温累计时长 (high_temp_duration)

**定义**：电池温度超过安全阈值(45°C)的累计时长

**判定条件**：battery_temperature > 45°C

**计算公式**：
```
高温时长 = Σ(高温记录数 × 采样间隔)
```

**单位**：小时

---

### 4. 日均行驶里程 (daily_mileage)

**定义**：每日平均行驶里程

**计算公式**：
```
日均里程 = Σ(速度 × 时间间隔)
```

**单位**：km

---

### 5. 充电频次 (charging_frequency)

**定义**：每日充电次数

**判定条件**：charging_status 从0变为1的次数

**计算公式**：
```
充电频次 = count(Δcharging_status = +1)
```

**单位**：次

---

### 6. 平均车速 (average_speed)

**定义**：统计周期内车速的算术平均值

**计算公式**：
```
平均车速 = Σ(速度) / 记录数
```

**单位**：km/h

---

### 7. 制动能量回收率 (brake_energy_recovery_rate)

**定义**：制动时回收能量占总能耗的比例

**判定条件**：battery_current < 0（充电状态）

**计算公式**：
```
制动能量回收率 = 充电消耗能量 / 总能耗 × 100%
```

**单位**：%

---

### 8. 电池健康指数 (battery_health_index)

**定义**：基于SOH(Slide of Health)和衰减规律的电池健康度

**计算公式**：
```
电池健康指数 = 100 
             - 高温小时数 × 0.1 
             - SOC循环次数 × 0.01
```

**单位**：%

---

## 🔄 Kafka模拟说明

项目实现了两种Kafka模式，自动检测切换：

### 模式1：模拟Kafka（默认）

使用Python `queue.Queue` 模拟消息队列，接口与真实Kafka一致：

```python
from src.kafka_sim import KafkaSimulator

simulator = KafkaSimulator()
producer = simulator.create_producer()
consumer = simulator.create_consumer("topic_name", group_id="group1")

# 发送消息
producer.send("topic_name", json.dumps(data))

# 消费消息
msg = consumer.poll(timeout_ms=1000)
```

**特点**：
- 无需安装Kafka
- 接口完全兼容
- 适合开发和测试

### 模式2：真实Kafka

如果环境中安装了Kafka服务，并配置了 `kafka-python` 依赖：

```python
# 自动检测并连接真实Kafka
# 使用 kafka-python 库
from kafka import KafkaProducer, KafkaConsumer
```

**配置方式**：修改 `config.py` 中的 `KAFKA_CONFIG`

## 📁 项目结构

```
Vehicle_Realtime_Warehouse/
├── README.md                    # 项目说明文档（本文件）
├── requirements.txt             # Python依赖清单
├── config.py                    # 项目配置参数
├── main.py                      # 一键运行入口
├── app.py                       # Streamlit可视化仪表盘
│
├── src/                         # 源代码目录
│   ├── __init__.py              # 包初始化
│   ├── scraper.py               # 数据爬虫（Figshare/Zenodo API）
│   ├── data_generator.py        # CAN数据生成器
│   ├── kafka_sim.py             # Kafka消息队列模拟
│   ├── flink_stream.py          # PyFlink实时流计算
│   ├── spark_warehouse.py        # PySpark离线数仓
│   ├── data_quality.py          # 数据质量保障模块
│   └── metrics.py               # 指标计算函数
│
├── data/                        # 数据目录
│   ├── raw/                     # 原始数据（爬虫/生成）
│   │   └── vehicle_can_data.csv
│   ├── ods/                     # ODS层数据
│   │   └── ods_data.csv
│   ├── dwd/                     # DWD层数据
│   │   └── dwd_data.csv
│   └── dws/                     # DWS层数据
│       ├── dws_data.csv         # 汇总数据
│       └── metrics/             # 各指标明细
│           ├── daily_energy_consumption.csv
│           ├── rapid_acceleration_count.csv
│           ├── high_temp_duration.csv
│           ├── daily_mileage.csv
│           ├── charging_frequency.csv
│           ├── average_speed.csv
│           ├── brake_energy_recovery_rate.csv
│           └── battery_health_index.csv
│
└── output/                      # 输出目录
    ├── figures/                 # 可视化图表
    └── reports/                 # 报告文件
        ├── quality_report_*.json
        └── visualization_data.json
```

## 🎨 Streamlit可视化页面

### 1. 首页概览

- 系统架构图
- 数据概况统计
- 车辆数据分布
- 技术栈说明

### 2. 实时监控

- 实时CAN数据流展示
- 5秒滑动窗口指标曲线
- 车速、温度、电流、SOC实时曲线
- 自动刷新（1秒间隔）

### 3. 数仓指标

- 8个核心指标卡片展示
- 时间趋势图
- 车辆分布柱状图
- 数据表格预览

### 4. 数据质量

- 质量概览统计
- 数据完整性分析
- 异常值检测结果
- 改善效果对比

### 5. 架构说明

- 技术栈表格
- 数仓模型文档
- 指标计算公式
- 项目结构说明

## 🔧 常见问题

### Q1: PyFlink/PySpark未安装怎么办？

项目设计了自动降级机制：
- PyFlink不可用 → 使用Python简化实现
- PySpark不可用 → 使用Pandas实现

所有核心功能均可正常工作。

### Q2: 如何生成更大规模的数据？

修改 `config.py` 中的参数：

```python
DATA_SOURCE = {
    "vehicle_count": 10,           # 车辆数量
    "records_per_vehicle": 50000,  # 每辆车记录数
    "interval_seconds": 5,         # 采集间隔
}
```

### Q3: 如何接入真实Kafka？

1. 安装Kafka服务
2. 安装依赖：`pip install kafka-python`
3. 修改 `config.py` 中的 `KAFKA_CONFIG`
4. 确保Kafka服务运行

### Q4: 可视化页面无法显示数据？

确保已运行 `python main.py` 生成数据，然后再启动 `streamlit run app.py`

### Q5: 如何查看数据质量报告？

报告位于 `output/reports/` 目录，以JSON格式存储：
- `quality_report_*.json` - 每次运行生成的质量报告
- `visualization_data.json` - 可视化数据

## 📞 联系方式

项目作者：Vehicle Realtime Warehouse Team

版本：1.0.0

最后更新：2024年

---

**版权声明**：本项目仅供学习和研究使用。
