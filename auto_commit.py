#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动提交 + 语义化版本管理脚本

规则：
- 检测工作区变更，自动 commit（保持原有提交信息生成逻辑）
- 累计变更行数（自上次 tag 以来已 commit + 当前工作区）>= 500 → 自动打 minor tag（v1.1.0）
- 累计变更行数 >= 200 → 自动打 patch tag（v1.0.1）
- 大版本 tag（major bump）由用户手动打

用法：
    python auto_commit.py              # 自动检测、提交、打 tag
    python auto_commit.py "提交说明"    # 使用自定义 commit message
"""

import subprocess
import sys
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
# 便携版 Git 路径（如已加入系统 PATH，可改为 "git"）
GIT_BIN = str(PROJECT_ROOT / "tools" / "git" / "cmd" / "git.exe")

PATCH_THRESHOLD = 200
MINOR_THRESHOLD = 500


def run_git(args):
    return subprocess.run([GIT_BIN] + args, cwd=str(PROJECT_ROOT), capture_output=True, text=True)


def get_latest_tag():
    """获取最近的 tag，如果没有返回 None。"""
    result = run_git(["describe", "--tags", "--abbrev=0"])
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def parse_version(tag):
    """解析语义化版本号 v1.0.0 -> (1, 0, 0)。"""
    if not tag:
        return (0, 0, 0)
    m = re.match(r'v?(\d+)\.(\d+)\.(\d+)', tag)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return (0, 0, 0)


def bump_version(major, minor, patch, bump_type):
    """根据 bump 类型计算新版本号。"""
    if bump_type == "minor":
        return (major, minor + 1, 0)
    elif bump_type == "patch":
        return (major, minor, patch + 1)
    return (major, minor, patch)


def parse_total_lines(stat_text):
    """从 git diff --stat 最后一行提取总修改行数（insertions + deletions）。"""
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


def get_lines_since_tag(tag):
    """计算从 tag 到 HEAD 的累计变更行数。如果没有 tag，返回 0。"""
    if tag is None:
        return 0
    result = run_git(["diff", "--stat", tag, "HEAD"])
    return parse_total_lines(result.stdout)


def get_uncommitted_lines():
    """计算工作区未提交的变更行数。"""
    result = run_git(["diff", "--stat", "HEAD"])
    return parse_total_lines(result.stdout)


def get_changed_files():
    result = run_git(["diff", "--name-only", "HEAD"])
    return [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]


def generate_commit_message(files, total_lines, custom_msg=None):
    if custom_msg:
        return custom_msg

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
    latest_tag = get_latest_tag()
    print(f"[auto_version] 最近 tag: {latest_tag or '无'}")

    major, minor, patch = parse_version(latest_tag)

    committed_lines = get_lines_since_tag(latest_tag)
    uncommitted_lines = get_uncommitted_lines()
    total_lines = committed_lines + uncommitted_lines

    print(f"[auto_version] 自上次 tag 已 commit: {committed_lines} 行")
    print(f"[auto_version] 当前工作区未提交: {uncommitted_lines} 行")
    print(f"[auto_version] 累计: {total_lines} 行")

    # 判断是否需要打 tag
    bump_type = None
    if total_lines >= MINOR_THRESHOLD:
        bump_type = "minor"
    elif total_lines >= PATCH_THRESHOLD:
        bump_type = "patch"

    # 如果有未提交的变更，先 commit
    if uncommitted_lines > 0:
        files = get_changed_files()
        custom_msg = sys.argv[1] if len(sys.argv) > 1 else None
        msg = generate_commit_message(files, uncommitted_lines, custom_msg)

        print(f"[auto_version] 自动 commit: {msg}")

        r1 = run_git(["add", "."])
        if r1.returncode != 0:
            print(f"[auto_version] git add 失败: {r1.stderr}")
            sys.exit(1)

        r2 = run_git(["commit", "-m", msg])
        if r2.returncode != 0:
            print(f"[auto_version] git commit 失败: {r2.stderr}")
            sys.exit(1)

        print("[auto_version] commit 成功")
    else:
        print("[auto_version] 工作区无未提交变更")

    if bump_type is None:
        print(f"[auto_version] 累计 {total_lines} 行，未达到 tag 阈值（{PATCH_THRESHOLD}/{MINOR_THRESHOLD}），跳过打 tag。")
        sys.exit(0)

    # 计算新版本号并打 tag
    new_major, new_minor, new_patch = bump_version(major, minor, patch, bump_type)
    new_tag = f"v{new_major}.{new_minor}.{new_patch}"

    print(f"[auto_version] 累计 {total_lines} 行 >= {MINOR_THRESHOLD if bump_type == 'minor' else PATCH_THRESHOLD}，自动打 {bump_type} tag: {new_tag}")

    r3 = run_git(["tag", new_tag])
    if r3.returncode != 0:
        print(f"[auto_version] git tag 失败: {r3.stderr}")
        sys.exit(1)

    print(f"[auto_version] 已成功打 tag: {new_tag}")


if __name__ == "__main__":
    main()
