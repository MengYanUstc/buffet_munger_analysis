# -*- coding: utf-8 -*-
import re
import os

reports_dir = 'reports/latest'
high_score_stocks = []

for fname in os.listdir(reports_dir):
    if not fname.endswith('.md'):
        continue
    fpath = os.path.join(reports_dir, fname)
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        continue
    
    score_match = re.search(r'(\d+\.\d)/100', content)
    if score_match:
        score = float(score_match.group(1))
        if score >= 70:
            code_match = re.search(r'(\d{6})', fname)
            name_match = re.search(r'_\d{6}_(.+?)_\d{8}\.md', fname)
            code = code_match.group(1) if code_match else ''
            name = name_match.group(1) if name_match else ''
            high_score_stocks.append({
                'file': fname,
                'code': code,
                'name': name,
                'score': score
            })

print(f'总分>=70分的报告: {len(high_score_stocks)}份')
for s in high_score_stocks:
    print(f"  {s['code']} {s['name']}: {s['score']}分")
