"""
TOML Export 상세 설정 파일
각 섹션별로 포함/제외할 필드와 마스킹 규칙을 정의
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class TomlFieldConfig:
    """TOML 필드 설정 관리 클래스"""
    
    # ==================== 고객 정보 설정 ====================
    CUSTOMER_INFO = {
        # 고정 마스킹 값 (실제 값과 무관하게 항상 이 값으로 치환)
        'fixed_masking': {
            '고객ID': 'CUST_ID_PLACEHOLDER',  # 나중에 동적으로 처리
            'MID': 'MID_PLACEHOLDER',  # 나중에 동적으로 처리
            '성명': '김빛썸',
            '연락처': '01077778888',
            '이메일': 'kimbithumb@gmail.com',
            'E-mail': 'kimbithumb@gmail.com',
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
            '생년월일': 'year_with_age',        # 연도 + 만 나이
            '설립일': 'year_with_age'           # 연도 + 만 나이 (법인용)
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
    
    # ==================== 관련인 정보 설정 ====================
    RELATED_PERSON = {
        'individual': {
            # 개인 관련인 포함 필드
            'include_fields': [
                '만나이', '성별', '직업', '직장명', 
                '위험등급', '총거래횟수'
            ],
            
            # 거래 내역 포맷
            'transaction_format': '내부{type}: {details}',
            'detail_format': '{ticker}({quantity:.4f}개, {amount:,}원, {count}건)'
        },
        
        'corporation': {
            # 법인 관련인 포함 필드
            'include_fields': [
                '관계', '생년월일', '관계인명', '대표자명'
            ],
            
            # 대표자 비교 규칙
            'representative_check': {
                'same': '대표자와 실소유자가 동일',
                'different': '대표자가 다른 사람임'
            }
        }
    }
    
    # ==================== RULE 히스토리 설정 ====================
    RULE_HISTORY = {
        # 컬럼명 매핑
        'column_mapping': {
            'STR_SSPC_UPER': '혐의관련_대주제_히스토리',
            'STR_SSPC_LWER': '혐의관련_소주제_히스토리'
        },
        
        # 숫자 코드 제거
        'remove_codes': True,
        
        # 출력 형식
        'delimiter': ', '
    }
    
    # ==================== ALERT 상세 설정 ====================
    ALERT_DETAIL = {
        # RULE ID 치환 규칙
        'rule_id_substitution': {
            'prefix': 'X',
            'start_number': 1
        },
        
        # 병합할 섹션들
        'merge_sections': [
            '의심거래 객관식 정보',
            'ALERT / RULE 발생 내역',
            '발생한 RULE의 정보(DISTINCT)',
            'ALERT_ID별 매매/입출고 현황'
        ],
        
        # 포함할 컬럼
        'include_columns': [
            'STR_RULE_ID', 'STR_RULE_NM', '객관식정보',
            'TRAN_STRT', 'TRAN_END',
            'STR_RULE_EXTR_COND_CTNT', 'AML_BSS_CTNT'
        ]
    }
    
    # ==================== Orderbook 개요 설정 ====================
    ORDERBOOK_SUMMARY = {
        # 텍스트 템플릿
        'template': {
            'period': '대표 혐의 기간 {start_date} ~ {end_date} 동안',
            'buy': '{amount}을 매수 하였으며 {details}',
            'sell': '{amount}을 매도 하였으며 {details}',
            'deposit_krw': '{amount}을 원화 입금 하였으며',
            'withdraw_krw': '{amount}을 원화 출금 하였으며',
            'deposit_crypto': '{amount} 상당의 가상자산을 입금 하였으며 {details}',
            'withdraw_crypto': '{amount} 상당의 가상자산을 출금 하였으며 {details}',
            'detail': '{ticker}종목을 {amount}, {count}회, {quantity:.4f}수량 만큼 진행'
        },
        
        # 액션 매핑
        'action_mapping': {
            'buy': '매수',
            'sell': '매도',
            'deposit_krw': '원화 입금',
            'withdraw_krw': '원화 출금',
            'deposit_crypto': '가상자산 입금',
            'withdraw_crypto': '가상자산 출금'
        },
        
        # 상위 N개 종목만 포함
        'top_n_tickers': 5
    }
    
    # ==================== STDS_DTM 요약 설정 ====================
    STDS_DTM_SUMMARY = {
        # 텍스트 템플릿
        'template': {
            'header': '대표 ALERT가 발생한 당일({date}),',
            'body': '매매 및 입출고 상세내역: '
        },
        
        # 포맷 규칙
        'use_same_format_as_orderbook': True
    }
    
    # ==================== IP 접속 이력 설정 ====================
    IP_ACCESS_HISTORY = {
        # 텍스트 템플릿
        'template': {
            'period': '{start_date} ~ {end_date} 기간동안',
            'countries': '{countries}에서 접속하였으며',
            'channels': '사용 채널은 {channels}이고',
            'ip_count': '총 {count}개의 IP 주소를 사용하였습니다'
        },
        
        # 집계 필드
        'aggregate_fields': {
            'period': ['접속일시'],
            'countries': ['국가한글명'],
            'channels': ['채널'],
            'ips': ['IP주소']
        },
        
        # 출력 형식
        'delimiter': ', ',
        'max_items_display': 10  # 최대 표시 항목 수
    }


class TomlConfig:
    """TOML 설정 관리 클래스"""
    
    def __init__(self):
        self.field_config = TomlFieldConfig()
    
    def get_customer_config(self) -> Dict[str, Any]:
        """고객 정보 설정 반환"""
        return TomlFieldConfig.CUSTOMER_INFO
    
    def get_duplicate_config(self) -> Dict[str, Any]:
        """중복 회원 설정 반환"""
        return TomlFieldConfig.DUPLICATE_PERSON
    
    def get_related_person_config(self) -> Dict[str, Any]:
        """관련인 설정 반환"""
        return TomlFieldConfig.RELATED_PERSON
    
    def get_rule_history_config(self) -> Dict[str, Any]:
        """Rule 히스토리 설정 반환"""
        return TomlFieldConfig.RULE_HISTORY
    
    def get_alert_detail_config(self) -> Dict[str, Any]:
        """Alert 상세 설정 반환"""
        return TomlFieldConfig.ALERT_DETAIL
    
    def get_orderbook_config(self) -> Dict[str, Any]:
        """Orderbook 설정 반환"""
        return TomlFieldConfig.ORDERBOOK_SUMMARY
    
    def get_stds_dtm_config(self) -> Dict[str, Any]:
        """STDS_DTM 설정 반환"""
        return TomlFieldConfig.STDS_DTM_SUMMARY
    
    def get_ip_history_config(self) -> Dict[str, Any]:
        """IP 이력 설정 반환"""
        return TomlFieldConfig.IP_ACCESS_HISTORY