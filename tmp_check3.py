import sys
sys.path.insert(0, '.')
from buffett_analyzer.data_warehouse.collector import DataCollector

c = DataCollector()
for module in ['moat', 'business_model', 'management']:
    data = c.get_qualitative_result('000858', module)
    print(f'=== {module} ===')
    print('key_facts:', data.get('key_facts', 'NOT FOUND'))
    print('risk_warnings:', data.get('risk_warnings', 'NOT FOUND'))
    print()
