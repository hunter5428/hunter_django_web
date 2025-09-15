# str_dashboard/toml_exporter.py
"""
화면에 렌더링된 데이터를 TOML 형식으로 수집하고 저장하는 모듈
"""

import toml
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from .toml_config import toml_config, toml_processor

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
        
        # metadata 제거 - 바로 data만 수집
        collected_data = {}
        
        # 1. Alert 정보 처리
        alert_data = session_data.get('current_alert_data', {})
        alert_id = session_data.get('current_alert_id', '')
        
        if alert_data or alert_id:
            logger.info(f"Processing alert data: {alert_id}")
            collected_data['alert_info'] = self._process_alert_info(alert_data, alert_id)
        
        # 2. 고객 정보 처리
        customer_data = session_data.get('current_customer_data', {})
        
        if customer_data and customer_data.get('rows'):
            logger.info("Processing customer data...")
            collected_data['customer'] = self._process_customer_data(customer_data)
        
        # 3. 중복 회원 처리
        dup_data = session_data.get('duplicate_persons_data', {})
        
        if dup_data and dup_data.get('rows'):
            logger.info("Processing duplicate persons...")
            collected_data['duplicate_persons'] = self._process_duplicate_persons(dup_data)
        
        # 4. 개인 관련인 처리
        person_data = session_data.get('current_person_related_data', {})
        
        if person_data:
            logger.info("Processing person related data...")
            collected_data['person_related'] = self._process_person_related(person_data)
        
        # 5. 법인 관련인 처리
        corp_data = session_data.get('current_corp_related_data', {})
        customer_type = customer_data.get('customer_type', '')
        
        if corp_data and corp_data.get('rows') and customer_type == '법인':
            logger.info("Processing corp related data...")
            collected_data['corp_related'] = self._process_corp_related(corp_data, customer_data)
        
        # 6. Rule 히스토리 처리
        rule_data = session_data.get('current_rule_history_data', {})
        
        if rule_data and rule_data.get('rows'):
            logger.info("Processing rule history...")
            collected_data['rule_history'] = self._process_rule_history_data(rule_data)
        
        # 7. Alert 상세 정보 (의심거래 객관식 + ALERT/RULE 발생 내역 등 병합)
        if alert_data:
            logger.info("Processing alert details...")
            alert_details = self._process_alert_details(alert_data, session_data)
            if alert_details:
                collected_data['alert_details'] = alert_details
        
        # 8. Orderbook 분석 처리
        orderbook_data = session_data.get('current_orderbook_analysis', {})
        
        if orderbook_data and orderbook_data.get('patterns'):
            logger.info("Processing orderbook analysis...")
            collected_data['orderbook_summary'] = self._process_orderbook_data(orderbook_data)
        
        # 9. STDS_DTM 요약 처리
        stds_data = session_data.get('current_stds_dtm_summary', {})
        
        if stds_data and stds_data.get('date'):
            logger.info("Processing STDS_DTM summary...")
            collected_data['stds_dtm_summary'] = self._process_stds_dtm_summary(stds_data)
        
        # 10. IP 접속 이력 처리
        ip_data = session_data.get('ip_history_data', {})
        
        if ip_data and ip_data.get('rows'):
            logger.info("Processing IP history...")
            collected_data['ip_history'] = self._process_ip_history(ip_data)
        
        logger.info(f"Data collection completed. Sections: {list(collected_data.keys())}")
        
        return collected_data

    def _process_alert_info(self, alert_data: Dict, alert_id: str) -> Dict[str, Any]:
        """Alert 정보 처리"""
        
        # alert_id 우선 사용
        final_alert_id = alert_id or alert_data.get('alert_id') or alert_data.get('currentAlertId', '')
        
        # canonical_ids와 rep_rule_id 추출
        canonical_ids = alert_data.get('canonical_ids', [])
        rep_rule_id = alert_data.get('rep_rule_id', '')
        cust_id = alert_data.get('custIdForPerson') or alert_data.get('cust_id', '')
        
        # 기간 정보 추출
        period = {}
        if alert_data.get('rows'):
            cols = alert_data.get('cols', [])
            rows = alert_data.get('rows', [])
            
            if cols and rows:
                tran_start_idx = cols.index('TRAN_STRT') if 'TRAN_STRT' in cols else -1
                tran_end_idx = cols.index('TRAN_END') if 'TRAN_END' in cols else -1
                
                if tran_start_idx >= 0 and tran_end_idx >= 0 and rows:
                    period = {
                        'start': rows[0][tran_start_idx] if tran_start_idx < len(rows[0]) else None,
                        'end': rows[0][tran_end_idx] if tran_end_idx < len(rows[0]) else None
                    }
        
        return {
            'alert_id': final_alert_id,
            'rep_rule_id': rep_rule_id,
            'cust_id': cust_id,
            'rule_ids': canonical_ids,
            'period': f"{period.get('start', '')} ~ {period.get('end', '')}" if period else ''
        }

    def _process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리 - toml_config 규칙 적용"""
        processed = {}
        
        if not customer_data.get('rows'):
            return processed
        
        columns = customer_data.get('columns', [])
        row = customer_data['rows'][0] if customer_data['rows'] else []
        
        # cust_id와 mid 먼저 추출
        cust_id = customer_data.get('cust_id', '')
        mid = None
        
        # MID 찾기
        mid_idx = columns.index('MID') if 'MID' in columns else -1
        if mid_idx >= 0 and mid_idx < len(row):
            mid = row[mid_idx]
        
        for idx, col in enumerate(columns):
            if idx < len(row):
                value = row[idx]
                
                # NULL 값 스킵
                if value is None or value == '':
                    continue
                
                # 마스킹 처리
                masked_value = self.processor.mask_customer_field(col, value, cust_id, mid)
                
                # None이면 제외 필드이므로 스킵
                if masked_value is not None:
                    processed[col] = masked_value
        
        return processed

    def _process_duplicate_persons(self, duplicate_data: Dict) -> List[Dict[str, Any]]:
        """중복 회원 데이터를 설명 문자열로 처리"""
        if not duplicate_data or not duplicate_data.get('rows'):
            return []
        
        processed = []
        columns = duplicate_data.get('columns', [])
        rows = duplicate_data.get('rows', [])
        
        for row in rows:
            # 필요한 인덱스 찾기
            match_types_idx = columns.index('MATCH_TYPES') if 'MATCH_TYPES' in columns else -1
            cust_id_idx = columns.index('고객ID') if '고객ID' in columns else -1
            name_idx = columns.index('성명') if '성명' in columns else -1
            
            if cust_id_idx >= 0 and cust_id_idx < len(row):
                cust_id = row[cust_id_idx]
                match_types = row[match_types_idx] if match_types_idx >= 0 and match_types_idx < len(row) else ''
                name = row[name_idx] if name_idx >= 0 and name_idx < len(row) else ''
                
                # 매칭 타입을 설명 문자열로 변환
                description = self.processor.process_duplicate_matches(match_types)
                
                processed.append({
                    'cust_id': cust_id,
                    'name': name,
                    'match_description': description
                })
        
        return processed

    def _process_person_related(self, person_data: Dict) -> Dict[str, Any]:
        """개인 관련인 데이터 처리"""
        if not person_data:
            return {}
        
        processed = {}
        
        for cust_id, data in person_data.items():
            person_info = data.get('info', {})
            transactions = data.get('transactions', [])
            
            if not person_info:
                continue
            
            # 필요 필드만 추출 (요청사항대로)
            filtered_info = {
                '만나이': person_info.get('age', ''),
                '성별': person_info.get('gender', ''),
                '직업': person_info.get('job', ''),
                '직장명': person_info.get('workplace', ''),
                '위험등급': person_info.get('risk_grade', ''),
                '총거래횟수': person_info.get('total_tran_count', 0)
            }
            
            # 거래 내역 텍스트 변환
            transaction_text = self.processor.process_related_person_transactions(transactions)
            
            processed[f'관련인_{cust_id}'] = {
                '정보': filtered_info,
                '거래내역': transaction_text
            }
        
        return processed

    def _process_corp_related(self, corp_data: Dict, customer_data: Dict) -> List[Dict[str, Any]]:
        """법인 관련인 데이터 처리"""
        if not corp_data or not corp_data.get('rows'):
            return []
        
        columns = corp_data.get('columns', [])
        rows = corp_data.get('rows', [])
        
        # 대표자명 찾기 (customer_data에서)
        ceo_name = None
        if customer_data and customer_data.get('rows'):
            cust_cols = customer_data.get('columns', [])
            cust_row = customer_data['rows'][0] if customer_data['rows'] else []
            ceo_idx = cust_cols.index('대표자명') if '대표자명' in cust_cols else -1
            if ceo_idx >= 0 and ceo_idx < len(cust_row):
                ceo_name = cust_row[ceo_idx]
        
        processed = []
        
        for row in rows[:20]:  # 최대 20명까지
            row_dict = {}
            
            # 필요한 컬럼 인덱스
            rel_idx = columns.index('관계') if '관계' in columns else -1
            name_idx = columns.index('관계인명') if '관계인명' in columns else -1
            birth_idx = columns.index('생년월일') if '생년월일' in columns else -1
            
            # 관계
            if rel_idx >= 0 and rel_idx < len(row):
                row_dict['관계'] = row[rel_idx]
            
            # 생년월일 (만 나이 포함)
            if birth_idx >= 0 and birth_idx < len(row) and row[birth_idx]:
                row_dict['생년월일'] = self.processor._format_birthdate_with_age(str(row[birth_idx]))
            
            # 관계인명과 대표자 비교
            if name_idx >= 0 and name_idx < len(row):
                related_name = row[name_idx]
                row_dict['관계인명'] = related_name
                
                if ceo_name and related_name:
                    if related_name == ceo_name:
                        row_dict['대표자_확인'] = "대표자와 실소유자가 동일"
                    else:
                        row_dict['대표자_확인'] = "대표자가 다른 사람임"
            
            if row_dict:  # 데이터가 있는 경우만 추가
                processed.append(row_dict)
        
        return processed

    def _process_rule_history_data(self, rule_data: Dict) -> Dict[str, Any]:
        """RULE 히스토리 데이터 처리 - SSPC 텍스트 추출"""
        if not rule_data or not rule_data.get('rows'):
            return {}
        
        columns = rule_data.get('columns', [])
        rows = rule_data.get('rows', [])
        
        uper_texts = []
        lwer_texts = []
        
        # SSPC 컬럼 인덱스 찾기
        uper_idx = columns.index('STR_SSPC_UPER') if 'STR_SSPC_UPER' in columns else -1
        lwer_idx = columns.index('STR_SSPC_LWER') if 'STR_SSPC_LWER' in columns else -1
        
        for row in rows:
            # 대주제 처리
            if uper_idx >= 0 and uper_idx < len(row) and row[uper_idx]:
                text = self.processor.extract_sspc_text(str(row[uper_idx]))
                if text and text not in uper_texts:
                    uper_texts.append(text)
            
            # 소주제 처리
            if lwer_idx >= 0 and lwer_idx < len(row) and row[lwer_idx]:
                text = self.processor.extract_sspc_text(str(row[lwer_idx]))
                if text and text not in lwer_texts:
                    lwer_texts.append(text)
        
        return {
            '혐의관련_대주제_히스토리': ', '.join(uper_texts),
            '혐의관련_소주제_히스토리': ', '.join(lwer_texts)
        }

    def _process_alert_details(self, alert_data: Dict, session_data: Dict) -> Dict[str, Any]:
        """Alert 상세 정보 처리 (의심거래 객관식 + ALERT/RULE 발생 내역 등)"""
        
        if not alert_data or not alert_data.get('rows'):
            return {}
        
        cols = alert_data.get('cols', [])
        rows = alert_data.get('rows', [])
        
        # Rule ID 치환 맵 생성
        canonical_ids = alert_data.get('canonical_ids', [])
        rep_rule_id = alert_data.get('rep_rule_id', '')
        rule_substitution = self.processor.substitute_rule_ids(canonical_ids, rep_rule_id)
        
        # 대표 RULE (X1)에 대한 정보만 추출
        processed = {
            'rule_mapping': rule_substitution,
            'X1_details': {}
        }
        
        # 대표 RULE ID가 있는 행 찾기
        if rep_rule_id and cols and rows:
            rule_idx = cols.index('STR_RULE_ID') if 'STR_RULE_ID' in cols else -1
            
            for row in rows:
                if rule_idx >= 0 and rule_idx < len(row) and row[rule_idx] == rep_rule_id:
                    # 필요한 컬럼 추출
                    details = {}
                    
                    # STR_RULE_NM
                    nm_idx = cols.index('STR_RULE_NM') if 'STR_RULE_NM' in cols else -1
                    if nm_idx >= 0 and nm_idx < len(row):
                        details['STR_RULE_NM'] = row[nm_idx]
                    
                    # TRAN_STRT, TRAN_END
                    start_idx = cols.index('TRAN_STRT') if 'TRAN_STRT' in cols else -1
                    end_idx = cols.index('TRAN_END') if 'TRAN_END' in cols else -1
                    if start_idx >= 0 and end_idx >= 0:
                        details['거래기간'] = f"{row[start_idx]} ~ {row[end_idx]}"
                    
                    # STR_RULE_EXTR_COND_CTNT
                    cond_idx = cols.index('STR_RULE_EXTR_COND_CTNT') if 'STR_RULE_EXTR_COND_CTNT' in cols else -1
                    if cond_idx >= 0 and cond_idx < len(row):
                        details['추출조건'] = row[cond_idx]
                    
                    # AML_BSS_CTNT
                    aml_idx = cols.index('AML_BSS_CTNT') if 'AML_BSS_CTNT' in cols else -1
                    if aml_idx >= 0 and aml_idx < len(row):
                        details['AML근거'] = row[aml_idx]
                    
                    processed['X1_details'] = details
                    break
        
        return processed

    def _process_orderbook_data(self, orderbook_data: Dict) -> str:
        """Orderbook 분석 데이터 처리 - 텍스트 요약으로 변환"""
        if not orderbook_data:
            return ""
        
        patterns = orderbook_data.get('patterns', {})
        period_info = orderbook_data.get('period_info', {})
        
        # toml_processor를 사용하여 텍스트 요약 생성
        return self.processor.format_orderbook_summary(patterns, period_info)

    def _process_stds_dtm_summary(self, stds_summary: Dict) -> str:
        """STDS_DTM 날짜별 요약을 텍스트로 처리"""
        if not stds_summary:
            return ""
        
        # toml_processor를 사용하여 텍스트 요약 생성
        return self.processor.format_stds_dtm_summary(stds_summary)

    def _process_ip_history(self, ip_data: Dict) -> str:
        """IP 접속 이력을 텍스트 요약으로 처리"""
        if not ip_data or not ip_data.get('rows'):
            return "IP 접속 이력이 없습니다"
        
        columns = ip_data.get('columns', [])
        rows = ip_data.get('rows', [])
        
        # 데이터를 딕셔너리 리스트로 변환
        data_list = []
        for row in rows:
            row_dict = {}
            for idx, col in enumerate(columns):
                if idx < len(row):
                    row_dict[col] = row[idx]
            data_list.append(row_dict)
        
        # processor를 사용하여 텍스트 요약 생성
        return self.processor.format_ip_access_summary(data_list)

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