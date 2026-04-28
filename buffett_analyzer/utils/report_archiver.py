# -*- coding: utf-8 -*-
"""
报告归档工具：将旧报告从 latest/ 移到 historical/
"""

import os
import re
import shutil
from typing import List, Optional


REPORTS_ROOT = os.path.join(os.getcwd(), "reports")
LATEST_DIR = os.path.join(REPORTS_ROOT, "latest")
HISTORICAL_DIR = os.path.join(REPORTS_ROOT, "historical")


def _ensure_dirs():
    os.makedirs(LATEST_DIR, exist_ok=True)
    os.makedirs(HISTORICAL_DIR, exist_ok=True)


def _list_analysis_reports_in_latest(stock_code: str) -> List[str]:
    """列出 latest/ 中指定股票代码的所有分析报告文件名。"""
    _ensure_dirs()
    pattern = re.compile(rf"^\d{{5}}_{re.escape(stock_code)}_.+\.md$")
    results = []
    for fname in os.listdir(LATEST_DIR):
        if pattern.match(fname):
            results.append(fname)
    return results


def archive_analysis_report(stock_code: str) -> List[str]:
    """
    在生成新的分析报告前调用。
    将 latest/ 中同公司的旧分析报告移到 historical/。
    返回被移动的文件名列表。
    """
    _ensure_dirs()
    old_reports = _list_analysis_reports_in_latest(stock_code)
    moved = []
    for fname in old_reports:
        src = os.path.join(LATEST_DIR, fname)
        dst = os.path.join(HISTORICAL_DIR, fname)
        shutil.move(src, dst)
        moved.append(fname)
    return moved


def archive_momentum_report() -> List[str]:
    """
    在生成新的动量报告前调用。
    将 latest/ 中的旧动量报告移到 historical/。
    返回被移动的文件名列表。
    """
    _ensure_dirs()
    pattern = re.compile(r"^momentum_report_\d{8}\.md$")
    moved = []
    for fname in os.listdir(LATEST_DIR):
        if pattern.match(fname):
            src = os.path.join(LATEST_DIR, fname)
            dst = os.path.join(HISTORICAL_DIR, fname)
            shutil.move(src, dst)
            moved.append(fname)
    return moved


def get_latest_analysis_report(stock_code: str) -> Optional[str]:
    """获取 latest/ 中指定公司的最新分析报告完整路径（编号最大）。"""
    _ensure_dirs()
    reports = _list_analysis_reports_in_latest(stock_code)
    if not reports:
        return None
    # 按文件名排序，编号大的在后
    reports.sort()
    return os.path.join(LATEST_DIR, reports[-1])
