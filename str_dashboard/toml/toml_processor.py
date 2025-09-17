# str_dashboard/toml_processor.py
"""
TOML 데이터 처리 로직
마스킹, 변환, 포맷팅 등의 실제 처리 담당
"""

import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

from .toml_config import toml_config

logger = logging.getLogger(__name__)


class TomlDataProcessor:
    """TOML 데이터 처리 클래스"""
    
    def __init__(self):
        self.config = toml_config
    
    @staticmethod
    def mask_customer_field(field_name: str, value: Any, cust_id: str = None, mid: str = None) -> Any:
        """고객 정보 필드 마스킹"""
        config = toml_config.CUSTOMER_INFO
        
        # 제외 필드
        if field_name in config['exclude_fields']:
            return None
        
        # 고정값 마스킹
        if field_name in config['fixed_masking']:
            if field_name == '고객ID' and cust_id:
                return f'{{{cust_id}}}'
            elif field_name == 'MID' and mid:
                return f'{{{mid}}}'
            else:
                return config['fixed_masking'][field_name]
        
        # 부분 마스킹
        if field_name in config['partial_masking']:
            rule = config['partial_masking'][field_name]
            return TomlDataProcessor.mask_partial_value(field_name, value, rule)
        
        # 마스킹 대상이 아닌 경우 원본 반환
        return value
    
    @staticmethod
    def mask_partial_value(field_name: str, value: str, rule: str) -> str:
        """부분 마스킹"""
        if not value or not isinstance(value, str):
            return value
        
        if rule == 'district_only':
            # 주소를 구 단위까지만 표시
            return TomlDataProcessor._extract_district(value)
        
        elif rule == 'year_only':
            # 생년월일/설립일을 연도만 표시 (YYYY년생)
            return TomlDataProcessor._format_year_only(value)
        elif rule == 'surname_only':
            # 성만 표시하고 나머지는 ** 처리
            return TomlDataProcessor._mask_name_surname_only(value)
            
        elif rule == 'domain_only':
            # 이메일에서 도메인만 표시
            return TomlDataProcessor._extract_email_domain(value)
        
        return value
    
    @staticmethod
    def _extract_district(address: str) -> str:
        """주소에서 구 단위까지 추출"""
        if not address:
            return ''
        
        # 다양한 주소 형식 처리
        patterns = [
            r'(.*?[시도])\s+(.*?[구군])',  # 시/도 + 구/군
            r'(서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시)\s+(.*?[구군])',
            r'(.*?특별시|.*?광역시|.*?시)\s+(.*?[구군])',
            r'(서울|부산|대구|인천|광주|대전|울산|세종)\s+(.*?[구군])',
            r'(.*?도)\s+(.*?[시군])\s+(.*?[구읍면])',  # 도 + 시/군 + 구/읍/면
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address)
            if match:
                if len(match.groups()) >= 3:  # 도 + 시/군 + 구/읍/면
                    return f"{match.group(1)} {match.group(2)} {match.group(3)}"
                elif len(match.groups()) >= 2:  # 시 + 구
                    return f"{match.group(1)} {match.group(2)}"
        
        # 패턴 매칭 실패 시 처음 두 단어만 반환
        parts = address.split()
        if len(parts) >= 2:
            return f"{parts[0]} {parts[1]}"
        
        return address.split()[0] if address.split() else address
    
    @staticmethod
    def _format_year_only(birthdate: str) -> str:
        """생년월일/설립일에서 연도만 추출하여 'YYYY년생' 형식으로 변환"""
        if not birthdate:
            return birthdate
            
        # 다양한 형식 처리
        date_clean = re.sub(r'[^0-9]', '', str(birthdate))
        
        # 연도 추출 (최소 4자리)
        if len(date_clean) >= 4:
            year = date_clean[:4]
            return f"{year}년생"
        
        return birthdate
        
    @staticmethod
    def _mask_name_surname_only(value: str) -> str:
        """성명에서 성(surname)만 남기고 나머지는 **로 처리"""
        if not value or len(value) < 2:
            return value
            
        # 한글 이름 처리 (첫 글자는 성으로 간주)
        if re.match(r'[\uAC00-\uD7A3]', value[0]):  # 한글 유니코드 범위
            return f"{value[0]}**"
            
        # 영문 이름 처리
        match = re.match(r'([A-Za-z]+)\s+([A-Za-z\s]+)', value)
        if match:
            # 영문 이름은 성이 뒤에 오는 경우가 많음
            first_name, last_name = match.groups()
            return f"{last_name[0]}**"
            
        # 기타 케이스
        return f"{value[0]}**"
    
    @staticmethod
    def _extract_email_domain(value: str) -> str:
        """이메일에서 도메인 부분만 추출"""
        if not value or '@' not in value:
            return value
            
        # 이메일 형식에서 @ 이후 부분만 추출
        domain = value.split('@')[-1]
        return domain
    @staticmethod
    def process_duplicate_matches(match_types: str) -> str:
        """중복 매칭 타입을 설명 문자열로 변환"""
        if not match_types:
            return "일치하는 정보가 없습니다"
        
        config = toml_config.DUPLICATE_PERSON
        matches = match_types.split(',') if isinstance(match_types, str) else []
        descriptions = []
        
        for match in matches:
            match = match.strip()
            if match in config['match_descriptions']:
                descriptions.append(config['match_descriptions'][match])
            # phone_suffix 처리
            elif 'phone' in match.lower():
                descriptions.append(config['match_descriptions'].get('PHONE', '휴대폰번호가 일치합니다'))
        
        return ', '.join(descriptions) if descriptions else "일치 정보 확인 필요"
    


# 싱글톤 인스턴스
toml_processor = TomlDataProcessor()