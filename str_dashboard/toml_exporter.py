# str_dashboard/toml_exporter.py
"""
화면에 렌더링된 데이터를 TOML 형식으로 수집하고 저장하는 모듈
사용자가 직접 수정 가능한 데이터 선택 로직 포함
"""

import toml
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class TomlDataCollector:
    """
    렌더링된 데이터를 수집하고 TOML 형식으로 변환하는 클래스
    """
    
    def __init__(self):
        """
        데이터 선택 설정을 초기화
        이 부분을 직접 수정하여 TOML에 포함할 데이터를 선택할 수 있습니다.
        """
        # ==================== 데이터 선택 설정 ====================
        # True로 설정된 항목만 TOML 파일에 포함됩니다.
        # 필요에 따라 직접 수정하세요.
        
        self.data_selection = {
            # 기본 정보
            'alert_info': True,           # ALERT ID 정보
            'query_period': True,         # 조회 기간 정보
            
            # 고객 정보
            'customer_basic': True,       # 고객 기본 정보
            'customer_detail': True,      # 고객 상세 정보
            'customer_str_info': True,    # STR 관련 정보
            
            # 관련인 정보
            'corp_related': True,         # 법인 관련인
            'person_related': True,       # 개인 관련인
            'duplicate_persons': True,    # 중복 회원
            
            # Rule 정보
            'rule_history': True,         # Rule 히스토리
            'rule_objectives': True,      # 의심거래 객관식
            'rule_description': True,     # Rule 설명
            
            # 거래 정보
            'alert_history': True,        # Alert 발생 내역
            'ip_history': False,          # IP 접속 이력 (기본 제외)
            
            # Orderbook 분석
            'orderbook_patterns': True,   # 거래 패턴 요약
            'orderbook_daily': True,      # 일자별 요약
            'orderbook_alert_detail': True,  # ALERT별 상세
            'stds_dtm_summary': True,     # STDS_DTM 날짜 요약 (새로 추가)
        }
        
        # 민감 정보 마스킹 설정
        self.mask_sensitive_data = True  # True: 민감 정보 마스킹
        self.sensitive_fields = [
            '실명번호', '사업자등록번호', 'E-mail', '이메일',
            '휴대폰 번호', '연락처', 'IP주소'
        ]
        
        # 데이터 크기 제한 설정
        self.max_rows_per_table = 100    # 테이블당 최대 행 수
        self.truncate_long_text = True   # 긴 텍스트 자르기
        self.max_text_length = 500       # 텍스트 최대 길이

    def collect_all_data(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        세션에서 모든 렌더링된 데이터 수집
        
        Args:
            session_data: 세션에 저장된 데이터
        
        Returns:
            TOML 형식으로 변환 가능한 딕셔너리
        """
        collected_data = {
            'metadata': self._create_metadata(),
            'data': {}
        }
        
        # Alert 정보
        if self.data_selection.get('alert_info') and 'alert_data' in session_data:
            collected_data['data']['alert_info'] = self._process_alert_info(
                session_data['alert_data']
            )
        
        # 고객 정보
        if self.data_selection.get('customer_basic') and 'customer_data' in session_data:
            collected_data['data']['customer'] = self._process_customer_data(
                session_data['customer_data']
            )
        
        # 관련인 정보
        if self.data_selection.get('corp_related') and 'corp_related_data' in session_data:
            collected_data['data']['corp_related'] = self._process_table_data(
                session_data['corp_related_data'],
                'corp_related'
            )
        
        if self.data_selection.get('person_related') and 'person_related_data' in session_data:
            collected_data['data']['person_related'] = self._process_person_related(
                session_data['person_related_data']
            )
        
        # Rule 정보
        if self.data_selection.get('rule_history') and 'rule_history_data' in session_data:
            collected_data['data']['rule_history'] = self._process_table_data(
                session_data['rule_history_data'],
                'rule_history'
            )
        
        # Orderbook 분석
        if self.data_selection.get('orderbook_patterns') and 'orderbook_analysis' in session_data:
            collected_data['data']['orderbook'] = self._process_orderbook_data(
                session_data['orderbook_analysis']
            )
        
        # STDS_DTM 날짜별 요약 (새로 추가)
        if self.data_selection.get('stds_dtm_summary') and 'stds_dtm_summary' in session_data:
            collected_data['data']['stds_dtm_summary'] = self._process_stds_dtm_summary(
                session_data['stds_dtm_summary']
            )
        
        return collected_data

    def _create_metadata(self) -> Dict[str, Any]:
        """메타데이터 생성"""
        return {
            'export_time': datetime.now().isoformat(),
            'version': '1.0',
            'data_selection': self.data_selection,
            'mask_sensitive': self.mask_sensitive_data
        }

    def _process_alert_info(self, alert_data: Dict) -> Dict[str, Any]:
        """Alert 정보 처리"""
        return {
            'alert_id': alert_data.get('alert_id'),
            'rep_rule_id': alert_data.get('rep_rule_id'),
            'cust_id': alert_data.get('cust_id'),
            'rule_ids': alert_data.get('canonical_ids', []),
            'period': alert_data.get('period', {})
        }

    def _process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리"""
        processed = {}
        
        if not customer_data.get('rows'):
            return processed
        
        columns = customer_data.get('columns', [])
        row = customer_data['rows'][0] if customer_data['rows'] else []
        
        for idx, col in enumerate(columns):
            if idx < len(row):
                value = row[idx]
                
                # 민감 정보 마스킹
                if self.mask_sensitive_data and col in self.sensitive_fields:
                    value = self._mask_value(value)
                
                # NULL 값 처리
                if value is None or value == '':
                    continue
                    
                processed[col] = value
        
        return processed

    def _process_table_data(self, table_data: Dict, table_name: str) -> List[Dict]:
        """테이블 데이터 처리"""
        if not table_data or not table_data.get('rows'):
            return []
        
        columns = table_data.get('columns', [])
        rows = table_data.get('rows', [])
        
        # 행 수 제한
        if len(rows) > self.max_rows_per_table:
            rows = rows[:self.max_rows_per_table]
            logger.info(f"Table {table_name} truncated to {self.max_rows_per_table} rows")
        
        processed_rows = []
        for row in rows:
            row_dict = {}
            for idx, col in enumerate(columns):
                if idx < len(row):
                    value = row[idx]
                    
                    # 민감 정보 마스킹
                    if self.mask_sensitive_data and col in self.sensitive_fields:
                        value = self._mask_value(value)
                    
                    # 긴 텍스트 자르기
                    if self.truncate_long_text and isinstance(value, str):
                        if len(value) > self.max_text_length:
                            value = value[:self.max_text_length] + '...'
                    
                    row_dict[col] = value
            
            processed_rows.append(row_dict)
        
        return processed_rows

    def _process_person_related(self, person_data: Dict) -> Dict[str, Any]:
        """개인 관련인 데이터 처리"""
        processed = {}
        
        for cust_id, data in person_data.items():
            person_info = data.get('info', {})
            
            # 민감 정보 마스킹
            if self.mask_sensitive_data:
                if 'id_number' in person_info:
                    person_info['id_number'] = self._mask_value(person_info['id_number'])
            
            processed[cust_id] = {
                'info': person_info,
                'transactions': data.get('transactions', [])[:20]  # 거래 내역 제한
            }
        
        return processed

    def _process_orderbook_data(self, orderbook_data: Dict) -> Dict[str, Any]:
        """Orderbook 분석 데이터 처리"""
        return {
            'patterns': orderbook_data.get('patterns', {}),
            'period_info': orderbook_data.get('period_info', {}),
            'summary_text': orderbook_data.get('text_summary', '')[:1000] if self.truncate_long_text else orderbook_data.get('text_summary', '')
        }

    def _process_stds_dtm_summary(self, stds_summary: Dict) -> Dict[str, Any]:
        """STDS_DTM 날짜별 요약 처리"""
        return {
            'date': stds_summary.get('date'),
            'summary': {
                'buy': {
                    'amount': stds_summary.get('buy_amount', 0),
                    'count': stds_summary.get('buy_count', 0),
                    'details': stds_summary.get('buy_details', [])[:10]
                },
                'sell': {
                    'amount': stds_summary.get('sell_amount', 0),
                    'count': stds_summary.get('sell_count', 0),
                    'details': stds_summary.get('sell_details', [])[:10]
                },
                'deposit_krw': {
                    'amount': stds_summary.get('deposit_krw_amount', 0),
                    'count': stds_summary.get('deposit_krw_count', 0)
                },
                'withdraw_krw': {
                    'amount': stds_summary.get('withdraw_krw_amount', 0),
                    'count': stds_summary.get('withdraw_krw_count', 0)
                },
                'deposit_crypto': {
                    'amount': stds_summary.get('deposit_crypto_amount', 0),
                    'count': stds_summary.get('deposit_crypto_count', 0),
                    'details': stds_summary.get('deposit_crypto_details', [])[:10]
                },
                'withdraw_crypto': {
                    'amount': stds_summary.get('withdraw_crypto_amount', 0),
                    'count': stds_summary.get('withdraw_crypto_count', 0),
                    'details': stds_summary.get('withdraw_crypto_details', [])[:10]
                }
            }
        }

    def _mask_value(self, value: str) -> str:
        """민감 정보 마스킹"""
        if not value or not isinstance(value, str):
            return value
        
        if len(value) <= 4:
            return '*' * len(value)
        
        # 앞 2자리와 뒤 2자리만 표시
        visible_start = 2
        visible_end = 2
        masked_length = len(value) - visible_start - visible_end
        
        return value[:visible_start] + '*' * masked_length + value[-visible_end:]

    def save_to_toml(self, data: Dict[str, Any], filepath: str) -> bool:
        """
        데이터를 TOML 파일로 저장
        
        Args:
            data: 저장할 데이터
            filepath: 저장 경로
        
        Returns:
            성공 여부
        """
        try:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                toml.dump(data, f)
            
            logger.info(f"TOML file saved successfully: {filepath}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to save TOML file: {e}")
            return False


# 싱글톤 인스턴스
toml_collector = TomlDataCollector()