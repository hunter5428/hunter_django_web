"""
Data processing utilities
"""
import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataProcessor:
    """데이터 처리 유틸리티 클래스"""
    
    @staticmethod
    def process_alert_data(cols: List[str], rows: List[List], alert_id: str) -> Dict[str, Any]:
        """
        Alert 데이터 처리
        
        Args:
            cols: 컬럼 리스트
            rows: 데이터 행 리스트
            alert_id: Alert ID
            
        Returns:
            처리된 Alert 데이터 딕셔너리
        """
        # 인덱스 찾기
        idx_alert = cols.index('STR_ALERT_ID') if 'STR_ALERT_ID' in cols else -1
        idx_rule = cols.index('STR_RULE_ID') if 'STR_RULE_ID' in cols else -1
        idx_cust = cols.index('CUST_ID') if 'CUST_ID' in cols else -1
        
        rep_rule_id = None
        cust_id_for_person = None
        canonical_ids = []
        
        # 대표 Rule ID 찾기
        if idx_alert >= 0 and idx_rule >= 0:
            rep_row = next((r for r in rows if str(r[idx_alert]) == alert_id), None)
            if rep_row:
                rep_rule_id = str(rep_row[idx_rule]) if idx_rule < len(rep_row) else None
                if idx_cust >= 0 and idx_cust < len(rep_row):
                    cust_id_for_person = rep_row[idx_cust]
        
        # 첫 번째 행에서 cust_id 가져오기 (백업)
        if not cust_id_for_person and rows and idx_cust >= 0:
            cust_id_for_person = rows[0][idx_cust] if idx_cust < len(rows[0]) else None
        
        # Canonical Rule IDs 수집
        if idx_rule >= 0:
            seen = set()
            for row in rows:
                if idx_rule < len(row):
                    rule_id = row[idx_rule]
                    if rule_id is not None:
                        str_id = str(rule_id).strip()
                        if str_id and str_id not in seen:
                            seen.add(str_id)
                            canonical_ids.append(str_id)
        
        return {
            'rep_rule_id': rep_rule_id,
            'cust_id_for_person': cust_id_for_person,
            'canonical_ids': canonical_ids
        }
    
    @staticmethod
    def extract_transaction_period(cols: List[str], rows: List[List]) -> Dict[str, Any]:
        """
        거래 기간 추출 및 조회 기간 계산
        
        Args:
            cols: 컬럼 리스트
            rows: 데이터 행 리스트
            
        Returns:
            기간 정보 딕셔너리
        """
        idx_tran_start = cols.index('TRAN_STRT') if 'TRAN_STRT' in cols else -1
        idx_tran_end = cols.index('TRAN_END') if 'TRAN_END' in cols else -1
        idx_rule_id = cols.index('STR_RULE_ID') if 'STR_RULE_ID' in cols else -1
        
        if idx_tran_start < 0 or idx_tran_end < 0:
            return {
                'start': None,
                'end': None,
                'months_back': 3,
                'has_special_rule': False
            }
        
        min_start = None
        max_end = None
        has_special_rule = False
        
        # 특정 RULE ID 체크 (IO000, IO111은 12개월)
        special_rules = {'IO000', 'IO111'}
        if idx_rule_id >= 0:
            for row in rows:
                if idx_rule_id < len(row):
                    rule_id = str(row[idx_rule_id])
                    if rule_id in special_rules:
                        has_special_rule = True
                        break
        
        # 최소/최대 날짜 찾기
        for row in rows:
            if idx_tran_start < len(row):
                start_date = row[idx_tran_start]
                if start_date and isinstance(start_date, str):
                    if re.match(r'^\d{4}-\d{2}-\d{2}', start_date):
                        if not min_start or start_date < min_start:
                            min_start = start_date
            
            if idx_tran_end < len(row):
                end_date = row[idx_tran_end]
                if end_date and isinstance(end_date, str):
                    if re.match(r'^\d{4}-\d{2}-\d{2}', end_date):
                        if not max_end or end_date > max_end:
                            max_end = end_date
        
        # 조회 기간 계산 (3개월 또는 12개월 이전)
        months_back = 12 if has_special_rule else 3
        
        if min_start:
            try:
                start_date_obj = datetime.strptime(min_start.split()[0], '%Y-%m-%d')
                # 월 단위로 정확히 빼기
                year = start_date_obj.year
                month = start_date_obj.month - months_back
                
                while month <= 0:
                    month += 12
                    year -= 1
                
                start_date_obj = start_date_obj.replace(year=year, month=month)
                min_start = start_date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            except Exception as e:
                logger.error(f"Error calculating start date: {e}")
        
        if max_end and ' ' not in max_end:
            max_end = f"{max_end} 23:59:59.999999999"
        
        return {
            'start': min_start,
            'end': max_end,
            'months_back': months_back,
            'has_special_rule': has_special_rule
        }
    
    @staticmethod
    def extract_column_value(columns: List[str], row: List, column_name: str) -> Any:
        """
        행에서 특정 컬럼 값 추출
        
        Args:
            columns: 컬럼 리스트
            row: 데이터 행
            column_name: 찾을 컬럼명
            
        Returns:
            컬럼 값 또는 None
        """
        try:
            idx = columns.index(column_name)
            if idx >= 0 and idx < len(row):
                return row[idx]
        except (ValueError, IndexError):
            pass
        return None
    
    @staticmethod
    def build_duplicate_params(columns: List[str], row: List) -> Dict[str, str]:
        """
        중복 회원 조회용 파라미터 생성
        
        Args:
            columns: 컬럼 리스트
            row: 데이터 행
            
        Returns:
            중복 조회 파라미터 딕셔너리
        """
        get_value = lambda col: DataProcessor.extract_column_value(columns, row, col) or ''
        
        phone = get_value('연락처')
        phone_suffix = phone[-4:] if len(phone) >= 4 else ''
        
        return {
            'full_email': get_value('이메일'),
            'phone_suffix': phone_suffix,
            'address': get_value('거주지주소'),
            'detail_address': get_value('거주지상세주소'),
            'workplace_name': get_value('직장명'),
            'workplace_address': get_value('직장주소'),
            'workplace_detail_address': get_value('직장상세주소')
        }
    
    @staticmethod
    def format_currency(amount: float, unit: str = '원') -> str:
        """
        금액 포맷팅
        
        Args:
            amount: 금액
            unit: 단위
            
        Returns:
            포맷된 금액 문자열
        """
        if amount == 0:
            return f'0{unit}'
        
        abs_amount = abs(amount)
        
        if abs_amount >= 100000000:  # 1억 이상
            eok = int(abs_amount // 100000000)
            man = int((abs_amount % 100000000) // 10000)
            if man > 0:
                return f"{eok:,}억 {man:,}만{unit}"
            return f"{eok:,}억{unit}"
        elif abs_amount >= 10000:  # 1만 이상
            man = int(abs_amount // 10000)
            return f"{man:,}만{unit}"
        else:
            return f"{int(abs_amount):,}{unit}"
    
    @staticmethod
    def clean_sql_value(value: Any) -> Any:
        """
        SQL 결과 값 정리
        
        Args:
            value: 원본 값
            
        Returns:
            정리된 값
        """
        if value is None:
            return None
        
        if isinstance(value, str):
            # 앞뒤 공백 제거
            value = value.strip()
            # 빈 문자열은 None으로
            if value == '':
                return None
        
        return value
    
    @staticmethod
    def calculate_age(birth_date: str) -> Optional[int]:
        """
        생년월일로부터 만 나이 계산
        
        Args:
            birth_date: 생년월일 문자열 (YYYY-MM-DD 또는 YYYYMMDD)
            
        Returns:
            만 나이 또는 None
        """
        if not birth_date:
            return None
        
        try:
            # 숫자만 추출
            birth_clean = re.sub(r'[^0-9]', '', str(birth_date))
            
            if len(birth_clean) >= 8:
                year = int(birth_clean[:4])
                month = int(birth_clean[4:6])
                day = int(birth_clean[6:8])
                
                birth = datetime(year, month, day)
                today = datetime.now()
                
                age = today.year - birth.year
                if (today.month, today.day) < (birth.month, birth.day):
                    age -= 1
                
                return age
        except Exception as e:
            logger.error(f"Error calculating age from {birth_date}: {e}")
        
        return None