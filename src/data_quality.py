# -*- coding: utf-8 -*-
"""
数据质量保障模块

功能：
- 基于(vehicle_id, timestamp)去重
- 空值填充：前向填充 + 中位数填充
- 异常值检测：IQR方法 + 物理范围约束
- 输出数据质量报告
- 保障数据完整性≥99%
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import os
import json

import config


class DataQualityChecker:
    """
    数据质量检查器
    
    检查项：
    1. 数据完整性
    2. 重复记录
    3. 空值分布
    4. 异常值检测
    5. 数据类型检查
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.original_count = len(df)
        self.columns = list(df.columns)
        self.quality_report: Dict = {}
        
    def check_completeness(self) -> Dict:
        """检查数据完整性"""
        total_cells = self.original_count * len(self.columns)
        null_cells = self.df.isnull().sum().sum()
        completeness = (1 - null_cells / total_cells) * 100 if total_cells > 0 else 100
        
        # 每列的空值率
        null_rates = {}
        for col in self.columns:
            null_count = self.df[col].isnull().sum()
            null_rates[col] = {
                "null_count": int(null_count),
                "null_rate": round(null_count / self.original_count * 100, 2)
            }
            
        return {
            "total_records": self.original_count,
            "total_cells": total_cells,
            "null_cells": int(null_cells),
            "completeness": round(completeness, 2),
            "column_null_rates": null_rates
        }
    
    def check_duplicates(self, key_columns: List[str]) -> Dict:
        """检查重复记录"""
        if all(col in self.columns for col in key_columns):
            dup_count = self.df.duplicated(subset=key_columns).sum()
            dup_rate = dup_count / self.original_count * 100 if self.original_count > 0 else 0
            
            # 显示重复示例
            dup_examples = None
            if dup_count > 0:
                dup_df = self.df[self.df.duplicated(subset=key_columns, keep=False)]
                dup_examples = dup_df.head(5).to_dict('records')
                
            return {
                "duplicate_count": int(dup_count),
                "duplicate_rate": round(dup_rate, 2),
                "key_columns": key_columns,
                "examples": dup_examples
            }
        return {"duplicate_count": 0, "duplicate_rate": 0, "key_columns": key_columns}
    
    def check_outliers_iqr(self, column: str, factor: float = 1.5) -> Dict:
        """
        使用IQR方法检测异常值
        
        Args:
            column: 列名
            factor: IQR倍数因子，默认1.5
            
        Returns:
            异常值信息
        """
        if column not in self.df.columns:
            return {}
            
        # 只处理数值列
        if not pd.api.types.is_numeric_dtype(self.df[column]):
            return {}
            
        Q1 = self.df[column].quantile(0.25)
        Q3 = self.df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
        
        # 统计异常值
        outlier_mask = (self.df[column] < lower_bound) | (self.df[column] > upper_bound)
        outlier_count = outlier_mask.sum()
        outlier_rate = outlier_count / self.original_count * 100 if self.original_count > 0 else 0
        
        return {
            "column": column,
            "method": "IQR",
            "Q1": round(Q1, 2),
            "Q3": round(Q3, 2),
            "IQR": round(IQR, 2),
            "lower_bound": round(lower_bound, 2),
            "upper_bound": round(upper_bound, 2),
            "outlier_count": int(outlier_count),
            "outlier_rate": round(outlier_rate, 2)
        }
    
    def check_outliers_physical(self, column: str, 
                                 physical_range: Tuple[float, float]) -> Dict:
        """
        使用物理范围检测异常值
        
        Args:
            column: 列名
            physical_range: 物理有效范围(最小值, 最大值)
            
        Returns:
            异常值信息
        """
        if column not in self.df.columns:
            return {}
            
        if not pd.api.types.is_numeric_dtype(self.df[column]):
            return {}
            
        lower, upper = physical_range
        outlier_mask = (self.df[column] < lower) | (self.df[column] > upper)
        outlier_count = outlier_mask.sum()
        outlier_rate = outlier_count / self.original_count * 100 if self.original_count > 0 else 0
        
        return {
            "column": column,
            "method": "physical_range",
            "physical_range": [lower, upper],
            "outlier_count": int(outlier_count),
            "outlier_rate": round(outlier_rate, 2)
        }
    
    def check_all_outliers(self) -> Dict:
        """检查所有数值列的异常值"""
        results = {
            "iqr_method": {},
            "physical_method": {}
        }
        
        for col in self.columns:
            if col in config.PHYSICAL_RANGES:
                # 物理范围检查
                physical_result = self.check_outliers_physical(col, config.PHYSICAL_RANGES[col])
                if physical_result:
                    results["physical_method"][col] = physical_result
                    
            # IQR方法检查
            iqr_result = self.check_outliers_iqr(col)
            if iqr_result and iqr_result.get("outlier_count", 0) > 0:
                results["iqr_method"][col] = iqr_result
                
        # 汇总异常值
        total_outliers = sum(v.get("outlier_count", 0) for v in results["physical_method"].values())
        results["total_physical_outliers"] = total_outliers
        results["total_physical_outlier_rate"] = round(
            total_outliers / self.original_count * 100, 2
        )
        
        return results
    
    def generate_report(self) -> Dict:
        """生成完整的数据质量报告"""
        report = {
            "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "original_count": self.original_count,
            "completeness": self.check_completeness(),
            "duplicates": self.check_duplicates(config.DATA_QUALITY_CONFIG["dedup_key"]),
            "outliers": self.check_all_outliers()
        }
        
        self.quality_report = report
        return report
    
    def print_report(self):
        """打印质量报告"""
        if not self.quality_report:
            self.generate_report()
            
        report = self.quality_report
        
        print("\n" + "="*60)
        print("数据质量报告")
        print("="*60)
        
        print(f"\n报告时间: {report['report_time']}")
        print(f"原始记录数: {report['original_count']}")
        
        print("\n【完整性检查】")
        comp = report["completeness"]
        print(f"  总体完整率: {comp['completeness']}%")
        print(f"  空值单元格: {comp['null_cells']}/{comp['total_cells']}")
        
        # 显示空值率较高的列
        high_null = [(k, v) for k, v in comp["column_null_rates"].items() 
                     if v["null_rate"] > 0]
        if high_null:
            print("  空值率>0的列:")
            for col, info in high_null[:5]:
                print(f"    {col}: {info['null_rate']}%")
        
        print("\n【重复检查】")
        dup = report["duplicates"]
        print(f"  重复记录数: {dup['duplicate_count']}")
        print(f"  重复率: {dup['duplicate_rate']}%")
        print(f"  去重键: {dup['key_columns']}")
        
        print("\n【异常值检查】(物理范围)")
        outliers = report["outliers"]
        print(f"  总异常值数: {outliers['total_physical_outliers']}")
        print(f"  总异常率: {outliers['total_physical_outlier_rate']}%")
        
        for col, info in outliers["physical_method"].items():
            if info["outlier_count"] > 0:
                print(f"  {col}: {info['outlier_count']} ({info['outlier_rate']}%) "
                      f"范围[{info['physical_range'][0]}, {info['physical_range'][1]}]")


class DataQualityProcessor:
    """
    数据质量处理器 - 执行数据清洗和质量保障
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.checker = DataQualityChecker(df)
        self.cleaning_report: Dict = {}
        
    def deduplicate(self, key_columns: List[str]) -> pd.DataFrame:
        """
        去重处理
        
        Args:
            key_columns: 去重键
            
        Returns:
            去重后的DataFrame
        """
        original_count = len(self.df)
        
        if all(col in self.df.columns for col in key_columns):
            self.df = self.df.drop_duplicates(subset=key_columns, keep='first')
            
        removed_count = original_count - len(self.df)
        
        self.cleaning_report["deduplication"] = {
            "original_count": original_count,
            "after_dedup": len(self.df),
            "removed_count": removed_count,
            "key_columns": key_columns
        }
        
        return self.df
    
    def fill_nulls(self, strategy: Dict = None) -> pd.DataFrame:
        """
        空值填充
        
        Args:
            strategy: 填充策略，默认使用配置文件中的设置
            
        Returns:
            填充后的DataFrame
        """
        if strategy is None:
            strategy = config.DATA_QUALITY_CONFIG["null_fill_strategy"]
            
        fill_report = {}
        
        for col in self.df.columns:
            null_count = self.df[col].isnull().sum()
            if null_count == 0:
                continue
                
            col_strategy = strategy.get(col, strategy.get("default", "median"))
            
            if col_strategy == "ffill":
                # 前向填充
                self.df[col] = self.df[col].ffill()
                fill_report[col] = {"method": "forward_fill", "filled_count": null_count}
                
            elif col_strategy == "bfill":
                # 后向填充
                self.df[col] = self.df[col].bfill()
                fill_report[col] = {"method": "backward_fill", "filled_count": null_count}
                
            elif col_strategy == "median":
                # 中位数填充
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    median_val = self.df[col].median()
                    self.df[col] = self.df[col].fillna(median_val)
                    fill_report[col] = {"method": "median_fill", "value": median_val, 
                                        "filled_count": null_count}
                    
            elif col_strategy == "mean":
                # 均值填充
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    mean_val = self.df[col].mean()
                    self.df[col] = self.df[col].fillna(mean_val)
                    fill_report[col] = {"method": "mean_fill", "value": mean_val,
                                        "filled_count": null_count}
                    
            elif col_strategy == "zero":
                # 零值填充
                self.df[col] = self.df[col].fillna(0)
                fill_report[col] = {"method": "zero_fill", "filled_count": null_count}
                
        self.cleaning_report["null_filling"] = fill_report
        return self.df
    
    def handle_outliers(self, method: str = "iqr", 
                        action: str = "clip") -> pd.DataFrame:
        """
        异常值处理
        
        Args:
            method: 检测方法 ("iqr" 或 "physical")
            action: 处理动作 ("clip", "remove", "nan")
            
        Returns:
            处理后的DataFrame
        """
        handle_report = {"method": method, "action": action, "columns": {}}
        
        for col in self.df.columns:
            if col not in config.PHYSICAL_RANGES:
                continue
                
            if not pd.api.types.is_numeric_dtype(self.df[col]):
                continue
                
            physical_range = config.PHYSICAL_RANGES[col]
            lower, upper = physical_range
            
            outlier_count_before = ((self.df[col] < lower) | (self.df[col] > upper)).sum()
            
            if action == "clip":
                # 裁剪到边界值
                self.df[col] = self.df[col].clip(lower=lower, upper=upper)
                
            elif action == "remove":
                # 删除异常记录
                mask = (self.df[col] >= lower) & (self.df[col] <= upper)
                removed = len(self.df) - mask.sum()
                self.df = self.df[mask]
                
            elif action == "nan":
                # 替换为空值（后续会被填充）
                self.df.loc[(self.df[col] < lower) | (self.df[col] > upper), col] = np.nan
            
            outlier_count_after = ((self.df[col] < lower) | (self.df[col] > upper)).sum()
            
            if outlier_count_before > 0:
                handle_report["columns"][col] = {
                    "range": [lower, upper],
                    "outliers_before": int(outlier_count_before),
                    "outliers_after": int(outlier_count_after)
                }
                
        self.cleaning_report["outlier_handling"] = handle_report
        return self.df
    
    def clean(self, remove_duplicates: bool = True,
              fill_nulls: bool = True,
              handle_outliers: bool = True) -> Tuple[pd.DataFrame, Dict]:
        """
        执行完整的数据清洗流程
        
        Args:
            remove_duplicates: 是否去重
            fill_nulls: 是否填充空值
            handle_outliers: 是否处理异常值
            
        Returns:
            (清洗后的DataFrame, 清洗报告)
        """
        print("\n" + "="*60)
        print("数据质量处理")
        print("="*60)
        
        # 1. 生成清洗前报告
        print("\n[1/4] 生成原始数据质量报告...")
        original_report = self.checker.generate_report()
        self.checker.print_report()
        
        # 2. 去重
        if remove_duplicates:
            print("\n[2/4] 执行去重...")
            self.deduplicate(config.DATA_QUALITY_CONFIG["dedup_key"])
            print(f"  去除重复记录: {self.cleaning_report['deduplication']['removed_count']}条")
        
        # 3. 空值填充
        if fill_nulls:
            print("\n[3/4] 填充空值...")
            self.fill_nulls()
            fill_info = self.cleaning_report.get("null_filling", {})
            filled_cols = len(fill_info)
            total_filled = sum(v.get("filled_count", 0) for v in fill_info.values())
            print(f"  填充了 {filled_cols} 列, 共 {total_filled} 个空值")
        
        # 4. 异常值处理
        if handle_outliers:
            print("\n[4/4] 处理异常值...")
            self.handle_outliers(method="physical", action="clip")
            outlier_info = self.cleaning_report.get("outlier_handling", {})
            total_outliers = sum(v.get("outliers_before", 0) for v in outlier_info.get("columns", {}).values())
            print(f"  处理了 {len(outlier_info.get('columns', {}))} 列, 共 {total_outliers} 个异常值")
        
        # 5. 生成清洗后报告
        print("\n[完成] 生成清洗后数据质量报告...")
        self.checker = DataQualityChecker(self.df)
        final_report = self.checker.generate_report()
        
        # 合并报告
        full_report = {
            "original_quality": original_report,
            "cleaning_steps": self.cleaning_report,
            "final_quality": final_report,
            "final_record_count": len(self.df),
            "improvement": {
                "completeness_improved": final_report["completeness"]["completeness"] - 
                                        original_report["completeness"]["completeness"],
                "duplicates_removed": original_report["duplicates"]["duplicate_count"],
                "outliers_handled": original_report["outliers"]["total_physical_outliers"]
            }
        }
        
        print("\n" + "="*60)
        print("清洗完成")
        print("="*60)
        print(f"  原始记录数: {original_report['original_count']}")
        print(f"  最终记录数: {len(self.df)}")
        print(f"  完整率提升: {full_report['improvement']['completeness_improved']:.2f}%")
        
        return self.df, full_report


def save_quality_report(report: Dict, output_path: str):
    """
    保存质量报告到JSON文件
    
    Args:
        report: 质量报告字典
        output_path: 输出路径
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        
    print(f"质量报告已保存到: {output_path}")


def demo_data_quality():
    """演示数据质量模块"""
    from src.data_generator import CANDataGenerator
    
    print("=" * 60)
    print("数据质量模块演示")
    print("=" * 60)
    
    # 生成测试数据
    generator = CANDataGenerator()
    test_data = generator.generate_stream_sample(["EV_001", "EV_002"], num_records=100)
    
    # 模拟添加一些问题数据
    test_data.loc[10, 'speed'] = np.nan
    test_data.loc[20, 'battery_voltage'] = np.nan
    test_data.loc[30:35, 'speed'] = 250  # 异常速度
    test_data = pd.concat([test_data, test_data.iloc[[10, 20]]], ignore_index=True)  # 添加重复
    
    print(f"\n测试数据: {len(test_data)}条记录")
    print("已注入: 空值、异常值、重复记录")
    
    # 执行清洗
    processor = DataQualityProcessor(test_data)
    cleaned_df, report = processor.clean()
    
    # 保存报告
    report_path = os.path.join(config.REPORTS_DIR, "quality_report_demo.json")
    save_quality_report(report, report_path)
    
    return cleaned_df, report


if __name__ == "__main__":
    demo_data_quality()
