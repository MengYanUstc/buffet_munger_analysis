#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动提交脚本
检测 git diff 修改行数，若大于 100 行则自动提交并推送。
支持自定义 commit message：python auto_commit.py "你的提交信息"
"""

import subprocess
import sys
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
# 便携版 Git 路径（如已加入系统 PATH，可改为 "git"）
GIT_BIN = str(PROJECT_ROOT / "tools" / "git" / "cmd" / "git.exe")

THRESHOLD = 100


def run_git(args):
    return subprocess.run([GIT_BIN] + args, cwd=str(PROJECT_ROOT), capture_output=True, text=True)


def get_diff_stats():
    result = run_git(["diff", "--stat", "HEAD"])
    return result.stdout


def parse_total_lines(stat_text):
    """从 git diff --stat 最后一行提取总修改行数。"""
    lines = stat_text.strip().splitlines()
    if not lines:
        return 0
    last_line = lines[-1]
    insertions = re.search(r'(\d+)\s+insertions?', last_line)
    deletions = re.search(r'(\d+)\s+deletions?', last_line)
    total = 0
    if insertions:
        total += int(insertions.group(1))
    if deletions:
        total += int(deletions.group(1))
    return total


def get_changed_files():
    result = run_git(["diff", "--name-only", "HEAD"])
    return [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]


def generate_commit_message(files, total_lines, custom_msg=None):
    if custom_msg:
        return custom_msg

    # 推断修改类型
    has_py = any(f.endswith('.py') for f in files)
    has_req = any('requirements' in f for f in files)
    has_md = any(f.endswith('.md') for f in files)

    modules = set()
    for f in files:
        if 'data_warehouse' in f:
            modules.add('data_warehouse')
        elif 'scorer' in f:
            modules.add('scorer')
        elif 'quality_analysis' in f:
            modules.add('quality_analysis')
        elif 'data_fetcher' in f:
            modules.add('data_fetcher')
        elif f.startswith('buffett_analyzer/'):
            modules.add('core')

    module_str = '/'.join(sorted(modules)) if modules else 'project'

    # 选择标签
    if has_req:
        tag = "deps"
    elif has_md and not has_py:
        tag = "docs"
    elif any('fetcher' in f or 'collector' in f for f in files):
        tag = "feat"
    elif any('fix' in f.lower() or 'bug' in f.lower() for f in files):
        tag = "fix"
    else:
        tag = "refactor"

    return f"{tag}: update {module_str} ({total_lines} lines changed)"


def main():
    stats = get_diff_stats()
    if not stats.strip():
        print("[auto_commit] 未检测到代码变更，跳过提交。")
        sys.exit(0)

    total_lines = parse_total_lines(stats)
    print(f"[auto_commit] 检测到 {total_lines} 行代码变更。")

    if total_lines <= THRESHOLD:
        print(f"[auto_commit] 变更行数（{total_lines}）<= {THRESHOLD}，未达到自动提交阈值。")
        sys.exit(0)

    files = get_changed_files()
    custom_msg = sys.argv[1] if len(sys.argv) > 1 else None
    msg = generate_commit_message(files, total_lines, custom_msg)

    print(f"[auto_commit] 自动生成的 commit message: {msg}")

    # git add
    r1 = run_git(["add", "."])
    if r1.returncode != 0:
        print(f"[auto_commit] git add 失败: {r1.stderr}")
        sys.exit(1)

    # git commit
    r2 = run_git(["commit", "-m", msg])
    if r2.returncode != 0:
        print(f"[auto_commit] git commit 失败: {r2.stderr}")
        sys.exit(1)

    # git push
    r3 = run_git(["push"])
    if r3.returncode != 0:
        print(f"[auto_commit] git push 失败: {r3.stderr}")
        sys.exit(1)

    print(f"[auto_commit] 已成功提交并推送: {msg}")


if __name__ == "__main__":
    main()
