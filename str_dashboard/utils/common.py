# utils/common.py
from decimal import Decimal
from typing import List, Any

def convert_row_types(row: tuple) -> List[Any]:
    """행 데이터 타입 변환 (Decimal -> float)"""
    return [
        float(value) if isinstance(value, Decimal) else value 
        for value in row
    ]

def format_timestamp(date_str: str) -> str:
    """날짜 문자열을 타임스탬프 형식으로 변환"""
    if not date_str:
        return date_str
    if ' ' in date_str and ':' in date_str:
        return date_str
    if '-' in date_str and ' ' not in date_str:
        return f"{date_str} 00:00:00"
    return date_str