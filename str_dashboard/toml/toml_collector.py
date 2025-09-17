# str_dashboard/toml_collector.py
"""
TOML 데이터 수집 및 조합 로직
세션 데이터를 수집하여 TOML 형식으로 변환
"""

import logging
from typing import Dict, Any, List, Optional

from .toml_config import toml_config
from .toml_processor import toml_processor

logger = logging.getLogger(__name__)


class TomlDataCollector:
    """
    렌더링된 데이터를 수집하고 TOML 형식으로 변환하는 클래스
    """
    
    def __init__(self):
        self.config = toml_config
        self.processor = toml_processor

    def collect_all_data(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """세션 데이터를 수집하여 TOML 형식으로 변환"""
        
        logger.info("Starting TOML data collection...")
        collected_data = {}
        
        # 고객 정보만 처리
        customer_data = session_data.get('current_customer_data', {})
        
        if customer_data and customer_data.get('rows'):
            logger.info("Processing customer data...")
            collected_data = self._process_customer_data(customer_data)
            
            # 중복 회원 처리 (고객 정보에 추가)
            dup_data = session_data.get('duplicate_persons_data', {})
            if dup_data and dup_data.get('rows'):
                logger.info("Processing duplicate persons...")
                dup_info = self._process_duplicate_persons(dup_data)
                # 동일_차명_의심회원 필드 추가
                if '혐의대상자_고객_정보' in collected_data:
                    if dup_info.startswith('동일_차명_의심회원 = '):
                        # = 기호로 분리하여 값만 추출
                        parts = dup_info.split('=', 1)
                        if len(parts) > 1:
                            collected_data['혐의대상자_고객_정보']['동일_차명_의심회원'] = parts[1].strip().strip('"')
                        else:
                            collected_data['혐의대상자_고객_정보']['동일_차명_의심회원'] = "없음"
                    else:
                        collected_data['혐의대상자_고객_정보']['동일_차명_의심회원'] = "없음"
        
        logger.info(f"Data collection completed. Sections: {list(collected_data.keys())}")
        
        return collected_data


    def _process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리 - 새로운 형식으로 변환"""
        # 새로운 형식으로 시작
        result = ["[혐의대상자_고객_정보]"]
        
        if not customer_data.get('rows'):
            return {"혐의대상자_고객_정보": {}}
        
        # 기본 데이터 준비
        columns = customer_data.get('columns', [])
        row = customer_data['rows'][0] if customer_data['rows'] else []
        
        # 매핑 테이블: 기존 필드명 -> 새 필드명
        field_mapping = {
            '성명': '성명',
            '고객구분': '고객구분',
            'RA등급': 'RA등급',
            'KYC위험등급': '위험등급',
            '고액자산가여부': '고액자산가',
            '생년월일': '출생/설립',
            '설립일': '출생/설립',
            '성별': '성별',
            '국적': '국적',
            '이메일': '이메일도메인',
            'E-mail': '이메일도메인',
            '거주지국가': '거주지국가',
            '거주지주소': '거주지주소',
            '직업': '직업/업종',
            '직업대분류': '직업대분류',
            '직업소분류': '직업소분류',
            '직위': '직위',
            '직장국가': '직장국가',
            '직장주소': '직장주소',
            '거래목적': '거래목적',
            '월평균소득': '월평균소득/매출',
            '매출액': '월평균소득/매출',
            '매매거래자금원천': '매매거래자금원천',
            'STR보고건수': 'STR보고건수',
            '최종STR보고일': '최종STR보고일',
            'Alert건수': 'Alert건수',
            'KYC완료일시': 'KYC완료일시'
        }
        
        # 결과 딕셔너리
        processed = {}
        
        # cust_id와 mid 추출 (필요시)
        cust_id = customer_data.get('cust_id', '')
        mid = None
        
        # MID 찾기
        mid_idx = columns.index('MID') if 'MID' in columns else -1
        if mid_idx >= 0 and mid_idx < len(row):
            mid = row[mid_idx]
        
        # 고객ID 찾기 (cust_id가 없는 경우)
        if not cust_id:
            cust_id_idx = columns.index('고객ID') if '고객ID' in columns else -1
            if cust_id_idx >= 0 and cust_id_idx < len(row):
                cust_id = row[cust_id_idx]
        
        for idx, col in enumerate(columns):
            if idx < len(row):
                value = row[idx]
                
                # NULL 값 스킵
                if value is None or value == '':
                    continue
                
                # 해당 필드가 매핑 테이블에 있는지 확인
                if col in field_mapping:
                    new_field_name = field_mapping[col]
                    
                    # 마스킹 처리
                    masked_value = self.processor.mask_customer_field(col, value, cust_id, mid)
                    
                    # None이면 제외 필드이므로 스킵
                    if masked_value is not None:
                        processed[new_field_name] = masked_value
        
        return {"혐의대상자_고객_정보": processed}

    def _process_duplicate_persons(self, duplicate_data: Dict) -> str:
        """중복 회원 데이터를 설명 문자열로 처리"""
        if not duplicate_data or not duplicate_data.get('rows'):
            return "동일_차명_의심회원 = \"없음\""
        
        columns = duplicate_data.get('columns', [])
        rows = duplicate_data.get('rows', [])
        
        match_descriptions = []
        count = 0
        
        for row in rows:
            count += 1
            # 필요한 인덱스 찾기
            match_types_idx = columns.index('MATCH_TYPES') if 'MATCH_TYPES' in columns else -1
            
            if match_types_idx >= 0 and match_types_idx < len(row):
                match_types = row[match_types_idx] 
                # 매칭 타입을 설명 문자열로 변환
                description = self.processor.process_duplicate_matches(match_types)
                if description and description != "일치하는 정보가 없습니다" and description != "일치 정보 확인 필요":
                    match_descriptions.append(description)
        
        if match_descriptions:
            # 중복 제거 및 쉼표로 구분된 목록 생성
            unique_descriptions = list(set(match_descriptions))
            formatted_desc = ", ".join(unique_descriptions)
            
            return f"동일_차명_의심회원 = \"{formatted_desc}이 동일한 회원이 {count}명 존재\""
        
        return "동일_차명_의심회원 = \"없음\""




# 싱글톤 인스턴스
toml_collector = TomlDataCollector()