# str_dashboard/utils/queries/stage_4/special_range_rules.py
"""
특별 기간 적용이 필요한 Rule ID 관리
"""

# 365일 기간을 적용해야 하는 Rule ID 목록
EXTENDED_RANGE_RULES = [
    'IO9999',
    'IO3000',
    # 필요시 추가
]

def requires_extended_range(rule_ids):
    """
    Rule ID 리스트에 확장 기간 적용 대상이 포함되어 있는지 확인
    
    Args:
        rule_ids: Rule ID 리스트
        
    Returns:
        bool: 확장 기간(365일) 적용 여부
    """
    if not rule_ids:
        return False
    
    for rule_id in rule_ids:
        if rule_id in EXTENDED_RANGE_RULES:
            return True
    
    return False