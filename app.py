# -*- coding: utf-8 -*-
"""
Streamlit可视化仪表盘 - app.py

提供以下页面：
1. 首页概览 - 项目架构图、数据概况
2. 实时监控 - 模拟实时CAN数据流、5秒窗口指标曲线
3. 数仓指标 - 8个核心指标的图表展示
4. 数据质量 - 质量报告、异常检测可视化
5. 架构说明 - 技术栈和数仓模型文档

使用方法：
    streamlit run app.py
    或
    python -m streamlit run app.py
"""

import os
import sys
import json
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config

# ============== 页面配置 ==============
st.set_page_config(
    page_title=config.STREAMLIT_CONFIG["page_title"],
    page_icon=config.STREAMLIT_CONFIG["page_icon"],
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #333;
        padding: 0.5rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
    .status-online {
        color: #28a745;
        font-weight: bold;
    }
    .status-offline {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


# ============== 数据加载函数 ==============
@st.cache_data(ttl=300)
def load_data(file_path: str) -> Optional[pd.DataFrame]:
    """加载数据文件"""
    if os.path.exists(file_path):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            st.warning(f"加载数据失败: {e}")
            return None
    return None


@st.cache_data(ttl=60)
def load_all_metrics() -> Dict[str, pd.DataFrame]:
    """加载所有DWS层指标"""
    metrics = {}
    metrics_dir = os.path.join(config.DWS_DIR, "metrics")

    if os.path.exists(metrics_dir):
        for f in os.listdir(metrics_dir):
            if f.endswith('.csv'):
                metric_name = f.replace('.csv', '')
                path = os.path.join(metrics_dir, f)
                try:
                    metrics[metric_name] = pd.read_csv(path)
                except:
                    pass

    return metrics


def load_visualization_data() -> Dict:
    """加载可视化数据"""
    viz_path = os.path.join(config.OUTPUT_DIR, "visualization_data.json")
    if os.path.exists(viz_path):
        with open(viz_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def load_quality_report() -> Dict:
    """加载最新质量报告"""
    reports_dir = config.REPORTS_DIR
    if os.path.exists(reports_dir):
        files = [f for f in os.listdir(reports_dir) if f.startswith('quality_report') and f.endswith('.json')]
        if files:
            files.sort(reverse=True)
            latest = os.path.join(reports_dir, files[0])
            with open(latest, 'r', encoding='utf-8') as f:
                return json.load(f)
    return {}


# ============== 模拟实时数据生成器 ==============
class RealtimeDataSimulator:
    """实时数据模拟器"""

    def __init__(self):
        self.vehicles = [f"EV_{i:03d}" for i in range(1, 11)]
        self.base_time = datetime.now()

    def generate_record(self, vehicle_id: str = None) -> Dict:
        """生成单条模拟记录"""
        if vehicle_id is None:
            vehicle_id = random.choice(self.vehicles)

        # 基于时间生成有趋势的数据
        elapsed = (datetime.now() - self.base_time).total_seconds()

        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "vehicle_id": vehicle_id,
            "speed": max(0, 60 + 20 * np.sin(elapsed / 30) + random.gauss(0, 5)),
            "battery_voltage": 350 + 10 * np.sin(elapsed / 60) + random.gauss(0, 2),
            "battery_current": 50 + 30 * np.cos(elapsed / 20) + random.gauss(0, 5),
            "battery_temperature": 30 + 5 * np.sin(elapsed / 45) + random.gauss(0, 1),
            "motor_temperature": 35 + 5 * np.sin(elapsed / 40) + random.gauss(0, 1),
            "soc": max(0, min(100, 75 - elapsed / 100 + random.gauss(0, 0.5))),
            "throttle": max(0, 50 + 30 * np.sin(elapsed / 25) + random.gauss(0, 5)),
        }


# ============== 页面组件 ==============
def render_metric_card(value: str, label: str, delta: str = None):
    """渲染指标卡片"""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
        {f'<div style="color: green;">{delta}</div>' if delta else ''}
    </div>
    """, unsafe_allow_html=True)


def render_architecture_diagram():
    """渲染架构图"""
    st.markdown("""
    ### 数据流架构

    ```text
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                           智能网联车辆数据平台                                │
    ├─────────────────────────────────────────────────────────────────────────────┤
    │                                                                             │
    │  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      │
    │  │   CAN总线数据    │      │   数据爬虫      │      │   数据生成器    │      │
    │  │   (真实采集)    │      │   (Figshare)   │      │   (模拟)        │      │
    │  └────────┬────────┘      └────────┬────────┘      └────────┬────────┘      │
    │           │                        │                        │               │
    │           └────────────────────────┼────────────────────────┘               │
    │                                    ▼                                          │
    │                          ┌─────────────────┐                                │
    │                          │   Kafka消息队列   │                                │
    │                          │  (实时数据流)     │                                │
    │                          └────────┬────────┘                                │
    │                                   │                                          │
    │           ┌───────────────────────┼───────────────────────┐                │
    │           ▼                       ▼                       ▼                │
    │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
    │  │  PyFlink实时计算  │    │   ODS层(原始)   │    │   ODS层(原始)   │         │
    │  │  - 5秒滑动窗口   │    │                 │    │                 │         │
    │  │  - 瞬时能耗      │    └────────┬────────┘    └────────┬────────┘         │
    │  │  - 温升速率      │             │                      │                  │
    │  └─────────────────┘             ▼                      ▼                  │
    │                         ┌─────────────────────────────────────────┐         │
    │                         │           数据质量保障模块               │         │
    │                         │  - 去重  - 空值填充  - 异常值检测       │         │
    │                         └────────┬────────────────────────────────┘         │
    │                                  │                                           │
    │                    ┌─────────────┼─────────────┐                            │
    │                    ▼             ▼             ▼                            │
    │             ┌───────────┐ ┌───────────┐ ┌───────────┐                       │
    │             │  ODS层     │ │  DWD层    │ │  DWS层    │                       │
    │             │  原始数据   │ │  明细数据  │ │  汇总指标  │                       │
    │             │            │ │  +清洗    │ │  +8核心指标│                       │
    │             └────────────┘ └───────────┘ └───────────┘                      │
    │                                      │                                       │
    │                                      ▼                                       │
    │                         ┌─────────────────────┐                              │
    │                         │  Streamlit可视化    │                              │
    │                         │  + Plotly图表       │                              │
    │                         └─────────────────────┘                              │
    └─────────────────────────────────────────────────────────────────────────────┘
    ```
    """)


# ============== 页面定义 ==============
def page_home():
    """首页概览"""
    st.markdown('<p class="main-header">🚗 智能网联车辆实时数据监控平台</p>', unsafe_allow_html=True)

    # 架构图
    with st.expander("📊 系统架构图", expanded=True):
        render_architecture_diagram()

    # 数据概况
    st.markdown("### 📈 数据概况")

    col1, col2, col3, col4 = st.columns(4)

    # 加载数据统计
    ods_data = load_data(os.path.join(config.ODS_DIR, "ods_data.csv"))
    dwd_data = load_data(os.path.join(config.DWD_DIR, "dwd_data.csv"))
    viz_data = load_visualization_data()

    with col1:
        if ods_data is not None:
            st.metric("原始数据", f"{len(ods_data):,} 条")
        else:
            st.metric("原始数据", "未生成")

    with col2:
        if dwd_data is not None:
            st.metric("清洗后数据", f"{len(dwd_data):,} 条")
        else:
            st.metric("清洗后数据", "未生成")

    with col3:
        metrics = load_all_metrics()
        st.metric("核心指标", f"{len(metrics)} 个")

    with col4:
        quality = load_quality_report()
        if quality:
            completeness = quality.get('final_quality', {}).get('completeness', {})
            st.metric("数据完整率", f"{completeness.get('completeness', 0):.1f}%")
        else:
            st.metric("数据完整率", "未检测")

    # 车辆分布
    st.markdown("### 🚙 车辆分布")

    if dwd_data is not None:
        vehicle_counts = dwd_data['vehicle_id'].value_counts().head(10)

        fig = px.bar(
            x=vehicle_counts.index,
            y=vehicle_counts.values,
            title="各车辆数据记录数",
            labels={"x": "车辆ID", "y": "记录数"},
            color=vehicle_counts.values,
            color_continuous_scale="Blues"
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无数据，请先运行 main.py 生成数据")

    # 技术栈
    st.markdown("### 🛠️ 技术栈")

    tech_col1, tech_col2 = st.columns(2)

    with tech_col1:
        st.markdown("""
        **实时计算**
        - PyFlink: 5秒滑动窗口流计算
        - Kafka模拟: 消息队列生产/消费
        - 实时指标: 瞬时能耗、温升速率
        """)

    with tech_col2:
        st.markdown("""
        **离线数仓**
        - PySpark SQL: ODS→DWD→DWS三层模型
        - 数据质量: 去重、空值填充、异常值检测
        - 8个核心指标聚合计算
        """)


def page_realtime():
    """实时监控页面"""
    st.markdown('<p class="sub-header">📡 实时CAN数据监控</p>', unsafe_allow_html=True)

    # 初始化模拟器
    if 'simulator' not in st.session_state:
        st.session_state.simulator = RealtimeDataSimulator()
        st.session_state.realtime_data = []

    # 实时数据区域
    st.markdown("#### 实时数据流")

    col1, col2, col3 = st.columns(3)

    # 生成新数据
    simulator = st.session_state.simulator
    new_record = simulator.generate_record()
    st.session_state.realtime_data.append(new_record)

    # 保持最近100条数据
    if len(st.session_state.realtime_data) > 100:
        st.session_state.realtime_data = st.session_state.realtime_data[-100:]

    # 显示实时指标
    with col1:
        st.metric("当前车速", f"{new_record['speed']:.1f} km/h")
    with col2:
        st.metric("电池温度", f"{new_record['battery_temperature']:.1f} °C")
    with col3:
        st.metric("SOC", f"{new_record['soc']:.1f} %")

    # 实时曲线
    st.markdown("#### 实时指标曲线")

    if len(st.session_state.realtime_data) > 1:
        df_realtime = pd.DataFrame(st.session_state.realtime_data)
        df_realtime['timestamp'] = pd.to_datetime(df_realtime['timestamp'])

        # 创建子图
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("车速 (km/h)", "电池温度 (°C)", "电池电流 (A)", "SOC (%)"),
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )

        # 车速
        fig.add_trace(
            go.Scatter(x=df_realtime['timestamp'], y=df_realtime['speed'],
                       name="车速", line=dict(color='#1f77b4')),
            row=1, col=1
        )

        # 电池温度
        fig.add_trace(
            go.Scatter(x=df_realtime['timestamp'], y=df_realtime['battery_temperature'],
                       name="电池温度", line=dict(color='#ff7f0e')),
            row=1, col=2
        )

        # 电池电流
        fig.add_trace(
            go.Scatter(x=df_realtime['timestamp'], y=df_realtime['battery_current'],
                       name="电池电流", line=dict(color='#2ca02c')),
            row=2, col=1
        )

        # SOC
        fig.add_trace(
            go.Scatter(x=df_realtime['timestamp'], y=df_realtime['soc'],
                       name="SOC", line=dict(color='#d62728')),
            row=2, col=2
        )

        fig.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("数据收集中，请稍候...")

    # 自动刷新
    time.sleep(1)
    st.rerun()


def page_warehouse():
    """数仓指标页面"""
    st.markdown('<p class="sub-header">📊 数仓核心指标看板</p>', unsafe_allow_html=True)

    metrics = load_all_metrics()

    if not metrics:
        st.warning("暂无指标数据，请先运行 main.py 生成数据")
        return

    # 指标说明
    st.markdown("### 8个核心指标")

    metric_descriptions = {
        "daily_energy_consumption": ("日均电耗", "kWh/100km", "每日每百公里平均能耗"),
        "rapid_acceleration_count": ("急加速次数", "次", "加速度超过阈值的次数"),
        "high_temp_duration": ("高温累计时长", "小时", "电池温度超过45°C的累计时长"),
        "daily_mileage": ("日均行驶里程", "km", "每日平均行驶里程"),
        "charging_frequency": ("充电频次", "次", "每日充电次数"),
        "average_speed": ("平均车速", "km/h", "统计周期内平均车速"),
        "brake_energy_recovery_rate": ("制动能量回收率", "%", "制动能量回收占总能耗的比例"),
        "battery_health_index": ("电池健康指数", "%", "基于SOH和衰减规律的电池健康度"),
    }

    # 指标卡片
    cols = st.columns(4)
    for idx, (metric_name, (display_name, unit, desc)) in enumerate(metric_descriptions.items()):
        with cols[idx % 4]:
            if metric_name in metrics:
                metric_df = metrics[metric_name]
                if not metric_df.empty:
                    # 获取主要数值列
                    numeric_cols = [c for c in metric_df.columns if c not in ['vehicle_id', 'date', 'update_time']]
                    if numeric_cols:
                        value = metric_df[numeric_cols[0]].mean()
                        st.metric(f"{display_name}", f"{value:.2f} {unit}")

    # 指标详细图表
    st.markdown("### 指标详细分析")

    selected_metric = st.selectbox(
        "选择指标查看详情",
        options=list(metric_descriptions.keys()),
        format_func=lambda x: metric_descriptions[x][0]
    )

    if selected_metric in metrics:
        metric_df = metrics[selected_metric]

        if not metric_df.empty:
            # 时间趋势图
            if 'date' in metric_df.columns:
                st.markdown("#### 时间趋势")

                numeric_cols = [c for c in metric_df.select_dtypes(include=[np.number]).columns
                                if c not in ['vehicle_id']]

                if numeric_cols:
                    col_to_plot = st.selectbox("选择指标列", numeric_cols)

                    # 按日期聚合
                    daily_avg = metric_df.groupby('date')[col_to_plot].mean().reset_index()

                    fig = px.line(
                        daily_avg,
                        x='date',
                        y=col_to_plot,
                        title=f"{metric_descriptions[selected_metric][0]} - 时间趋势",
                        markers=True
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # 车辆分布图
            if 'vehicle_id' in metric_df.columns:
                st.markdown("#### 车辆分布")

                numeric_cols = [c for c in metric_df.select_dtypes(include=[np.number]).columns
                                if c not in ['vehicle_id', 'date']]

                if numeric_cols:
                    col_to_plot = st.selectbox("选择指标列(车辆)", numeric_cols)

                    vehicle_avg = metric_df.groupby('vehicle_id')[col_to_plot].mean().reset_index()

                    fig = px.bar(
                        vehicle_avg,
                        x='vehicle_id',
                        y=col_to_plot,
                        title=f"{metric_descriptions[selected_metric][0]} - 车辆分布",
                        color=col_to_plot,
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # 数据预览
            st.markdown("#### 数据预览")
            st.dataframe(metric_df.head(20), use_container_width=True)


def page_quality():
    """数据质量页面"""
    st.markdown('<p class="sub-header">✅ 数据质量报告</p>', unsafe_allow_html=True)

    quality_report = load_quality_report()

    if not quality_report:
        st.warning("暂无质量报告，请先运行 main.py 生成数据")
        return

    # 质量概览
    st.markdown("### 质量概览")

    col1, col2, col3, col4 = st.columns(4)

    original = quality_report.get('original_quality', {})
    final = quality_report.get('final_quality', {})
    improvement = quality_report.get('improvement', {})

    with col1:
        st.metric("原始记录数", f"{original.get('original_count', 0):,}")

    with col2:
        st.metric("最终记录数", f"{quality_report.get('final_record_count', 0):,}")

    with col3:
        completeness = final.get('completeness', {})
        st.metric("完整率", f"{completeness.get('completeness', 0):.2f}%")

    with col4:
        dup_original = original.get('duplicates', {})
        st.metric("去重数量", f"{improvement.get('duplicates_removed', 0):,}")

    # 完整性分析
    st.markdown("### 数据完整性分析")

    completeness = final.get('completeness', {})
    if completeness:
        col_null_rates = completeness.get('column_null_rates', {})

        if col_null_rates:
            # 转换为DataFrame（空值率存数值，显示时再加%）
            null_df = pd.DataFrame([
                {"字段": k, "空值数": v['null_count'], "空值率": float(v['null_rate'])}
                for k, v in col_null_rates.items()
            ])

            # 空值率柱状图
            null_df_sorted = null_df.sort_values('空值率', ascending=False)
            null_df_sorted = null_df_sorted[null_df_sorted['空值率'] > 0]

            if not null_df_sorted.empty:
                display_df = null_df_sorted.head(10).copy()
                display_df['空值率(%)'] = display_df['空值率']
                fig = px.bar(
                    display_df,
                    x='字段',
                    y='空值率(%)',
                    title="各字段空值率",
                    color='空值率(%)',
                    color_continuous_scale="Reds"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("所有字段均无空值！")

    # 异常值分析
    st.markdown("### 异常值分析")

    outliers = final.get('outliers', {})
    if outliers:
        physical_method = outliers.get('physical_method', {})

        if physical_method:
            outlier_list = []
            for col, info in physical_method.items():
                if info.get('outlier_count', 0) > 0:
                    outlier_list.append({
                        "字段": col,
                        "异常数": info['outlier_count'],
                        "异常率": f"{info['outlier_rate']}%",
                        "有效范围": f"[{info['physical_range'][0]}, {info['physical_range'][1]}]"
                    })

            if outlier_list:
                outlier_df = pd.DataFrame(outlier_list)
                st.dataframe(outlier_df, use_container_width=True)

                # 异常率饼图
                fig = px.pie(
                    outlier_df,
                    values='异常数',
                    names='字段',
                    title="异常值分布"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("无异常值！")

    # 改善效果
    st.markdown("### 改善效果")

    improvement = quality_report.get('improvement', {})

    improv_col1, improv_col2, improv_col3 = st.columns(3)

    with improv_col1:
        completeness_improved = improvement.get('completeness_improved', 0)
        st.metric("完整率提升", f"+{completeness_improved:.2f}%")

    with improv_col2:
        st.metric("去除重复", f"{improvement.get('duplicates_removed', 0)} 条")

    with improv_col3:
        st.metric("处理异常值", f"{improvement.get('outliers_handled', 0)} 个")


def page_architecture():
    """架构说明页面"""
    st.markdown('<p class="sub-header">📚 技术架构说明</p>', unsafe_allow_html=True)

    # 技术栈说明
    st.markdown("""
    ### 技术栈概览

    #### 实时计算层
    | 技术 | 用途 | 特点 |
    |------|------|------|
    | PyFlink | 实时流计算 | 5秒滑动窗口，瞬时能耗、温升速率计算 |
    | Kafka模拟 | 消息队列 | 支持真实/模拟两种模式 |

    #### 离线数仓层
    | 技术 | 用途 | 特点 |
    |------|------|------|
    | PySpark SQL | 数据处理 | 本地模式，ODS→DWD→DWS三层架构 |
    | Pandas | 数据分析 | 降级模式支持 |
    """)

    # 数仓模型说明
    st.markdown("""
    ### 数仓三层模型

    #### ODS层 (Operational Data Store) - 操作数据存储层
    **职责**：接收原始CAN数据，只做格式统一

    **表结构**：
    | 字段 | 类型 | 说明 |
    |------|------|------|
    | timestamp | string | 时间戳 |
    | vehicle_id | string | 车辆ID |
    | speed | float | 车速(km/h) |
    | battery_voltage | float | 电池电压(V) |
    | battery_current | float | 电池电流(A) |
    | battery_temperature | float | 电池温度(°C) |
    | motor_temperature | float | 电机温度(°C) |
    | soc | float | 电池SOC(%) |
    | ... | ... | ... |

    #### DWD层 (Data Warehouse Detail) - 明细数据层
    **职责**：数据清洗、标准化、派生字段

    **处理内容**：
    - 去重：基于(vehicle_id, timestamp)
    - 空值填充：前向填充 + 中位数填充
    - 异常值处理：物理范围约束
    - 派生字段：driving_status, energy_category, temp_status, battery_status

    #### DWS层 (Data Warehouse Service) - 数据服务层
    **职责**：汇总计算，提供8个核心指标

    **8个核心指标**：
    1. 日均电耗 = Σ能耗 / Σ里程 × 100
    2. 急加速次数 = count(加速度 > 5 km/h/s)
    3. 高温累计时长 = sum(电池温度 > 45°C 的时长)
    4. 日均行驶里程 = sum(里程) / 天数
    5. 充电频次 = count(充电状态变化)
    6. 平均车速 = avg(车速)
    7. 制动能量回收率 = 回收能量 / 总能耗 × 100%
    8. 电池健康指数 = 100 - 高温衰减 - 循环衰减
    """)

    # 项目结构
    st.markdown("""
    ### 项目结构

    ```
    Vehicle_Realtime_Warehouse/
    ├── README.md                    # 项目说明文档
    ├── requirements.txt             # 依赖清单
    ├── config.py                    # 项目配置
    ├── main.py                      # 一键运行入口
    ├── app.py                       # Streamlit可视化仪表盘
    ├── src/
    │   ├── __init__.py
    │   ├── scraper.py               # 数据爬虫
    │   ├── data_generator.py       # CAN数据生成器
    │   ├── kafka_sim.py             # Kafka消息队列模拟
    │   ├── flink_stream.py          # PyFlink实时流计算
    │   ├── spark_warehouse.py       # PySpark离线数仓
    │   ├── data_quality.py          # 数据质量保障
    │   └── metrics.py               # 指标计算函数
    ├── data/
    │   ├── raw/                     # 原始数据
    │   ├── ods/                     # ODS层
    │   ├── dwd/                     # DWD层
    │   └── dws/                     # DWS层
    └── output/
        ├── figures/                 # 可视化图表
        └── reports/                 # 数据质量报告
    ```
    """)


# ============== 主程序 ==============
def main():
    """主程序入口"""

    # 侧边栏导航
    st.sidebar.markdown("## 🚗 导航菜单")

    pages = {
        "🏠 首页概览": page_home,
        "📡 实时监控": page_realtime,
        "📊 数仓指标": page_warehouse,
        "✅ 数据质量": page_quality,
        "📚 架构说明": page_architecture,
    }

    selected_page = st.sidebar.radio("选择页面", list(pages.keys()))

    # 执行选中的页面
    pages[selected_page]()

    # 侧边栏信息
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 系统信息")
    st.sidebar.markdown(f"""
    - **项目版本**: 1.0.0
    - **更新时间**: {datetime.now().strftime('%Y-%m-%d')}
    - **数据目录**: `{config.PROJECT_ROOT}`
    """)

    # 快速链接
    st.sidebar.markdown("### 快速链接")

    if st.sidebar.button("🔄 刷新数据", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("📖 查看README", use_container_width=True):
        readme_path = os.path.join(config.PROJECT_ROOT, "README.md")
        if os.path.exists(readme_path):
            with open(readme_path, 'r', encoding='utf-8') as f:
                content = f.read()
            st.sidebar.markdown(content[:1000] + "..." if len(content) > 1000 else content)


if __name__ == "__main__":
    main()
