# str_dashboard/toml_config.py
"""
TOML Export 설정 관리
각 섹션별 마스킹 규칙과 필드 설정을 정의
"""

from typing import Dict, Any, List, Optional


class TomlFieldConfig:
    """TOML 필드 설정 관리 클래스"""
    
    # ==================== 고객 정보 설정 ====================
    CUSTOMER_INFO = {
        # 고정 마스킹 값 (실제 값과 무관하게 항상 이 값으로 치환)
        'fixed_masking': {
            '고객ID': '{cust_id}',  # 동적 치환
            'MID': '{mid}',  # 동적 치환
            '휴대폰 번호': '01077778888'
        },
        
        # 제외할 필드 (TOML에 포함하지 않음)
        'exclude_fields': [
            '실명번호구분',
            '실명번호',
            '거주지상세주소',
            '거주지우편번호',
            '직장우편번호',
            '직장상세주소',
            '사업장우편번호',
            '사업장상세주소'
        ],
        
        # 부분 마스킹 규칙
        'partial_masking': {
            '거주지주소': 'district_only',      # 구 단위까지만
            '직장주소': 'district_only',        # 구 단위까지만
            '생년월일': 'year_only',            # 연도만 (1984년생)
            '설립일': 'year_only',              # 연도만 (법인용)
            '성명': 'surname_only',             # 성만 표시 (김**)
            '이메일': 'domain_only'             # 도메인만 표시 (gmail.com)
        }
    }
    
    # ==================== 동일 차명의심 설정 ====================
    DUPLICATE_PERSON = {
        # 매칭된 필드를 설명으로 변환
        'match_descriptions': {
            'ADDRESS': '거주주소가 동일합니다',
            'EMAIL': '이메일이 동일합니다',
            'WORKPLACE_NAME': '직장명이 동일합니다',
            'WORKPLACE_ADDRESS': '직장주소가 동일합니다',
            'PHONE': '휴대폰번호(뒷자리)가 동일합니다'
        },
        
        # 출력 형식
        'output_format': 'text_description'  # 실제값 대신 설명 문자열
    }


# 싱글톤 인스턴스
toml_config = TomlFieldConfig()