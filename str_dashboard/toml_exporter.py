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
from .toml_config import toml_config, toml_processor  # 새로 추가

logger = logging.getLogger(__name__)


class TomlDataCollector:
    """
    렌더링된 데이터를 수집하고 TOML 형식으로 변환하는 클래스
    """
    
    def __init__(self):
        # 기존 코드를 toml_config.py의 설정으로 대체
        self.config = toml_config  # 설정 객체
        self.processor = toml_processor  # 처리 객체
        
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

    # toml_exporter.py의 collect_all_data 메서드 수정
    def collect_all_data(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """세션 데이터를 수집하여 TOML 형식으로 변환"""
        collected_data = {
            'metadata': self._create_metadata(),
            'data': {}
        }
        
        # Alert 정보
        if self.data_selection.get('alert_info') and 'current_alert_data' in session_data:
            collected_data['data']['alert_info'] = self._process_alert_info(
                session_data['current_alert_data']
            )
        
        # 고객 정보
        if self.data_selection.get('customer_basic') and 'current_customer_data' in session_data:
            collected_data['data']['customer'] = self._process_customer_data(
                session_data['current_customer_data']
            )
        
        # 중복 회원
        if self.data_selection.get('duplicate_persons') and 'duplicate_persons_data' in session_data:
            collected_data['data']['duplicate_persons'] = self._process_duplicate_persons(
                session_data['duplicate_persons_data']
            )
        
        # 개인 관련인
        if self.data_selection.get('person_related') and 'current_person_related_data' in session_data:
            collected_data['data']['person_related'] = self._process_person_related(
                session_data['current_person_related_data']
            )
        
        # 법인 관련인
        if self.data_selection.get('corp_related') and 'current_corp_related_data' in session_data:
            collected_data['data']['corp_related'] = self._process_table_data(
                session_data['current_corp_related_data'], 'corp_related'
            )
        
        # IP 이력
        if self.data_selection.get('ip_history') and 'ip_history_data' in session_data:
            collected_data['data']['ip_history'] = self._process_ip_history(
                session_data['ip_history_data']
            )
        
        # Rule 히스토리
        if self.data_selection.get('rule_history') and 'current_rule_history_data' in session_data:
            collected_data['data']['rule_history'] = self._process_rule_history_data(
                session_data['current_rule_history_data']
            )
        
        # Orderbook 분석
        if self.data_selection.get('orderbook_patterns') and 'current_orderbook_analysis' in session_data:
            collected_data['data']['orderbook'] = self._process_orderbook_data(
                session_data['current_orderbook_analysis']
            )
        
        # STDS_DTM 요약
        if self.data_selection.get('stds_dtm_summary') and 'current_stds_dtm_summary' in session_data:
            collected_data['data']['stds_dtm_summary'] = self._process_stds_dtm_summary(
                session_data['current_stds_dtm_summary']
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

    # toml_exporter.py의 _process_customer_data 메서드를 완전히 교체

    def _process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리 - toml_config 규칙 적용"""
        processed = {}
        
        if not customer_data.get('rows'):
            return processed
        
        columns = customer_data.get('columns', [])
        row = customer_data['rows'][0] if customer_data['rows'] else []
        
        for idx, col in enumerate(columns):
            if idx < len(row):
                value = row[idx]
                
                # 1. 제외 필드 체크
                if col in self.config.CUSTOMER_INFO['exclude_fields']:
                    continue
                
                # 2. NULL 값 처리
                if value is None or value == '':
                    continue
                
                # 3. 고정값 마스킹 적용
                if col in self.config.CUSTOMER_INFO['fixed_masking']:
                    value = self.config.CUSTOMER_INFO['fixed_masking'][col]
                
                # 4. 부분 마스킹 적용
                elif col in self.config.CUSTOMER_INFO['partial_masking']:
                    rule = self.config.CUSTOMER_INFO['partial_masking'][col]
                    value = self.processor.mask_partial_value(col, str(value), rule)
                
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


    def _process_orderbook_data(self, orderbook_data: Dict) -> Dict[str, Any]:
        """Orderbook 분석 데이터 처리 - 텍스트 요약으로 변환"""
        if not orderbook_data:
            return {}
        
        # 패턴과 기간 정보 추출
        patterns = orderbook_data.get('patterns', {})
        period_info = orderbook_data.get('period_info', {})
        
        # toml_processor를 사용하여 텍스트 요약 생성
        text_summary = self.processor.format_orderbook_summary(patterns, period_info)
        
        return {
            'summary_text': text_summary,
            'period': period_info,
            'total_buy': patterns.get('total_buy_amount', 0),
            'total_sell': patterns.get('total_sell_amount', 0),
            'total_deposit_krw': patterns.get('total_deposit_krw', 0),
            'total_withdraw_krw': patterns.get('total_withdraw_krw', 0)
        }

    def _process_stds_dtm_summary(self, stds_summary: Dict) -> Dict[str, Any]:
        """STDS_DTM 날짜별 요약을 텍스트로 처리"""
        if not stds_summary:
            return {}
        
        # toml_processor를 사용하여 텍스트 요약 생성
        text_summary = self.processor.format_stds_dtm_summary(stds_summary)
        
        return {
            'date': stds_summary.get('date'),
            'summary_text': text_summary
        }

    def _process_alert_detail(self, alert_data: Dict, rule_data: Dict) -> Dict[str, Any]:
        """ALERT 상세 정보 처리 - RULE ID 치환 및 병합"""
        if not alert_data or not rule_data:
            return {}
        
        # RULE ID 목록과 대표 RULE ID 추출
        rule_ids = alert_data.get('canonical_ids', [])
        rep_rule_id = alert_data.get('rep_rule_id')
        
        # RULE ID를 X1, X2... 형식으로 치환
        rule_substitution = self.processor.substitute_rule_ids(rule_ids, rep_rule_id)
        
        # 치환된 데이터 구성
        processed = {
            'rule_mapping': rule_substitution,
            'rep_rule': f"X1 ({rep_rule_id})" if rep_rule_id else None,
            'alert_id': alert_data.get('alert_id'),
            'period': f"{alert_data.get('period', {}).get('start', '')} ~ {alert_data.get('period', {}).get('end', '')}"
        }
        
        return processed
    
    # toml_exporter.py에 다음 메서드들을 추가 (기존 _process_alert_detail 메서드 아래에)

    def _process_duplicate_persons(self, duplicate_data: Dict) -> Dict[str, Any]:
        """중복 회원 데이터를 설명 문자열로 처리"""
        if not duplicate_data or not duplicate_data.get('rows'):
            return {}
        
        processed = {}
        columns = duplicate_data.get('columns', [])
        rows = duplicate_data.get('rows', [])
        
        for idx, row in enumerate(rows):
            # MATCH_TYPES 컬럼 찾기
            match_types_idx = columns.index('MATCH_TYPES') if 'MATCH_TYPES' in columns else -1
            cust_id_idx = columns.index('고객ID') if '고객ID' in columns else -1
            
            if match_types_idx >= 0 and cust_id_idx >= 0:
                match_types = row[match_types_idx]
                cust_id = row[cust_id_idx]
                
                # 매칭 타입을 설명 문자열로 변환
                description = self.processor.process_duplicate_matches(match_types)
                processed[f'duplicate_{idx+1}'] = {
                    'cust_id': cust_id,
                    'match_description': description
                }
        
        return processed

    def _process_ip_history(self, ip_data: Dict) -> Dict[str, Any]:
        """IP 접속 이력을 텍스트 요약으로 처리"""
        if not ip_data or not ip_data.get('rows'):
            return {'summary': 'IP 접속 이력이 없습니다'}
        
        # 컬럼과 행 데이터를 딕셔너리 리스트로 변환
        columns = ip_data.get('columns', [])
        rows = ip_data.get('rows', [])
        
        data_list = []
        for row in rows:
            row_dict = {}
            for idx, col in enumerate(columns):
                if idx < len(row):
                    row_dict[col] = row[idx]
            data_list.append(row_dict)
        
        # processor를 사용하여 텍스트 요약 생성
        summary_text = self.processor.format_ip_access_summary(data_list)
        
        return {
            'summary': summary_text,
            'total_records': len(rows)
        }

    def _process_rule_history_data(self, rule_data: Dict) -> Dict[str, Any]:
        """RULE 히스토리 데이터 처리 - SSPC 텍스트 추출"""
        if not rule_data or not rule_data.get('rows'):
            return {}
        
        columns = rule_data.get('columns', [])
        rows = rule_data.get('rows', [])
        
        processed = {
            '혐의관련_대주제_히스토리': [],
            '혐의관련_소주제_히스토리': []
        }
        
        # SSPC 컬럼 인덱스 찾기
        uper_idx = columns.index('STR_SSPC_UPER') if 'STR_SSPC_UPER' in columns else -1
        lwer_idx = columns.index('STR_SSPC_LWER') if 'STR_SSPC_LWER' in columns else -1
        
        for row in rows:
            # 대주제 처리
            if uper_idx >= 0 and row[uper_idx]:
                text = self.processor.extract_sspc_text(str(row[uper_idx]))
                if text and text not in processed['혐의관련_대주제_히스토리']:
                    processed['혐의관련_대주제_히스토리'].append(text)
            
            # 소주제 처리
            if lwer_idx >= 0 and row[lwer_idx]:
                text = self.processor.extract_sspc_text(str(row[lwer_idx]))
                if text and text not in processed['혐의관련_소주제_히스토리']:
                    processed['혐의관련_소주제_히스토리'].append(text)
        
        # 리스트를 쉼표로 구분된 문자열로 변환
        processed['혐의관련_대주제_히스토리'] = ', '.join(processed['혐의관련_대주제_히스토리'])
        processed['혐의관련_소주제_히스토리'] = ', '.join(processed['혐의관련_소주제_히스토리'])
        
        return processed


# 싱글톤 인스턴스
toml_collector = TomlDataCollector()