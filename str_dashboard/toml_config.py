# str_dashboard/toml_config.py
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
        # 고정 마스킹 값
        'fixed_masking': {
            '고객ID': '{cust_id}',
            'MID': '{mid}',
            '성명': '{김빛썸}',
            '연락처': '01077778888',
            '이메일': 'kimbithumb@gmail.com',
            'E-mail': 'kimbithumb@gmail.com'
        },
        
        # 제외할 필드
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
            '거주지주소': 'district_only',  # 구 단위까지만
            '직장주소': 'district_only',    # 구 단위까지만
            '생년월일': 'year_with_age',    # 연도 + 만 나이
            '설립일': 'year_only'           # 연도만
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
            'phone_suffix': '휴대폰번호 뒷자리가 동일합니다'
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
            'transaction_format': '{type}: {ticker}({quantity:.4f}개, {amount:,}원, {count}건)'
        },
        
        'corporation': {
            # 법인 관련인 포함 필드
            'include_fields': [
                '관계', '생년월일'
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
            'header': '대표 혐의 기간 {start_date} ~ {end_date} 동안',
            'action_template': '{amount:,}원을 {action} 하였으며',
            'detail_template': '{ticker}종목을 {amount:,}원, {count}회, {quantity:.4f}수량 만큼 진행'
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
            'channels': '사용 채널: {channels}',
            'ip_count': '총 {count}개의 IP 주소 사용'
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


class TomlDataProcessor:
    """TOML 데이터 처리 클래스"""
    
    @staticmethod
    def mask_fixed_value(field_name: str, value: Any) -> Any:
        """고정값 마스킹"""
        if field_name in TomlFieldConfig.CUSTOMER_INFO['fixed_masking']:
            return TomlFieldConfig.CUSTOMER_INFO['fixed_masking'][field_name]
        return value
    
    @staticmethod
    def mask_partial_value(field_name: str, value: str, rule: str) -> str:
        """부분 마스킹"""
        if not value or not isinstance(value, str):
            return value
        
        if rule == 'district_only':
            # 주소를 구 단위까지만 표시
            return TomlDataProcessor._extract_district(value)
        
        elif rule == 'year_with_age':
            # 생년월일을 연도와 만 나이로 변환
            return TomlDataProcessor._format_birthdate_with_age(value)
        
        elif rule == 'year_only':
            # 연도만 추출
            if len(value) >= 4:
                return f"{value[:4]}년"
        
        return value
    
    @staticmethod
    def _extract_district(address: str) -> str:
        """주소에서 구 단위까지 추출"""
        if not address:
            return ''
        
        # 패턴: 시/도 + 구/군/시
        patterns = [
            r'(.*?[시도])\s+(.*?[구군시])',
            r'(.*?특별시)\s+(.*?구)',
            r'(.*?광역시)\s+(.*?[구군])',
            r'(.*?도)\s+(.*?[시군])'
        ]
        
        for pattern in patterns:
            match = re.match(pattern, address)
            if match:
                return f"{match.group(1)} {match.group(2)}"
        
        # 패턴 매칭 실패 시 첫 두 단어만 반환
        parts = address.split()
        if len(parts) >= 2:
            return f"{parts[0]} {parts[1]}"
        
        return address.split()[0] if address.split() else address
    
    @staticmethod
    def _format_birthdate_with_age(birthdate: str) -> str:
        """생년월일을 연도와 만 나이로 변환"""
        if not birthdate or len(birthdate) < 8:
            return birthdate
        
        try:
            # YYYYMMDD 형식 가정
            year = int(birthdate[:4])
            month = int(birthdate[4:6])
            day = int(birthdate[6:8])
            
            birth_date = datetime(year, month, day)
            today = datetime.now()
            
            # 만 나이 계산
            age = today.year - birth_date.year
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            return f"{year}년생 (만 {age}세)"
            
        except (ValueError, IndexError):
            # 파싱 실패 시 연도만 반환
            if len(birthdate) >= 4:
                return f"{birthdate[:4]}년생"
            return birthdate
    
    @staticmethod
    def process_duplicate_matches(match_types: str) -> str:
        """중복 매칭 타입을 설명 문자열로 변환"""
        if not match_types:
            return "일치하는 정보가 없습니다"
        
        matches = match_types.split(',')
        descriptions = []
        
        for match in matches:
            match = match.strip()
            if match in TomlFieldConfig.DUPLICATE_PERSON['match_descriptions']:
                descriptions.append(
                    TomlFieldConfig.DUPLICATE_PERSON['match_descriptions'][match]
                )
        
        return ', '.join(descriptions) if descriptions else "일치 정보 확인 필요"
    
    @staticmethod
    def extract_sspc_text(sspc_value: str) -> str:
        """SSPC 값에서 숫자 코드를 제거하고 텍스트만 추출"""
        if not sspc_value:
            return ''
        
        # (숫자, 텍스트) 패턴에서 텍스트만 추출
        pattern = r'\(\d+,\s*([^)]+)\)'
        matches = re.findall(pattern, sspc_value)
        
        if matches:
            return ', '.join(matches)
        
        return sspc_value
    
    @staticmethod
    def substitute_rule_ids(rule_ids: List[str], rep_rule_id: str) -> Dict[str, str]:
        """RULE ID를 X1, X2, X3... 형식으로 치환"""
        substitution = {}
        counter = 1
        
        # 대표 RULE ID를 X1로 설정
        if rep_rule_id:
            substitution[rep_rule_id] = f'X{counter}'
            counter += 1
        
        # 나머지 RULE ID 치환
        for rule_id in rule_ids:
            if rule_id != rep_rule_id and rule_id not in substitution:
                substitution[rule_id] = f'X{counter}'
                counter += 1
        
        return substitution
    
    # str_dashboard/toml_config.py - format_orderbook_summary 메서드 수정
    @staticmethod
    def format_orderbook_summary(patterns: Dict, period: Dict) -> str:
        """Orderbook 패턴을 텍스트 요약으로 변환"""
        config = TomlFieldConfig.ORDERBOOK_SUMMARY
        lines = []
        
        # 헤더
        header = config['template']['header'].format(
            start_date=period.get('start_date', ''),
            end_date=period.get('end_date', '')
        )
        lines.append(header)
        
        # 각 액션별 요약
        for action_key, action_name in config['action_mapping'].items():
            amount_key = f'total_{action_key}_amount'
            details_key = f'{action_key}_details'
            
            if amount_key in patterns and patterns[amount_key] > 0:
                # 액션 요약
                action_line = config['template']['action_template'].format(
                    amount=patterns[amount_key],
                    action=action_name
                )
                
                # 상세 내역 (상위 N개)
                details = patterns.get(details_key, [])[:config['top_n_tickers']]
                if details:
                    detail_parts = []
                    for item in details:
                        # details가 튜플 리스트인지 딕셔너리 리스트인지 확인
                        if isinstance(item, tuple) and len(item) == 2:
                            # 튜플인 경우: (ticker, data)
                            ticker, data = item
                            if isinstance(data, dict):
                                detail_part = config['template']['detail_template'].format(
                                    ticker=ticker,
                                    amount=data.get('amount_krw', 0),
                                    count=data.get('count', 0),
                                    quantity=data.get('quantity', 0)
                                )
                            else:
                                # data가 딕셔너리가 아닌 경우
                                detail_part = f"{ticker}: 데이터 형식 오류"
                        elif isinstance(item, dict):
                            # 딕셔너리인 경우
                            detail_part = config['template']['detail_template'].format(
                                ticker=item.get('ticker', 'Unknown'),
                                amount=item.get('amount', item.get('amount_krw', 0)),
                                count=item.get('count', 0),
                                quantity=item.get('quantity', 0)
                            )
                        else:
                            # 예상치 못한 형식
                            logger.warning(f"Unexpected detail format: {type(item)}")
                            continue
                        
                        detail_parts.append(detail_part)
                    
                    if detail_parts:
                        action_line += ' ' + ', '.join(detail_parts)
                
                lines.append(action_line)
        
        return ' '.join(lines)


    @staticmethod
    def format_ip_access_summary(ip_data: List[Dict]) -> str:
        """IP 접속 이력을 텍스트 요약으로 변환"""
        if not ip_data:
            return "IP 접속 이력이 없습니다"
        
        config = TomlFieldConfig.IP_ACCESS_HISTORY
        
        # 데이터 집계
        dates = []
        countries = set()
        channels = set()
        ips = set()
        
        for row in ip_data:
            if '접속일시' in row:
                dates.append(row['접속일시'])
            if '국가한글명' in row:
                countries.add(row['국가한글명'])
            if '채널' in row:
                channels.add(row['채널'])
            if 'IP주소' in row:
                ips.add(row['IP주소'])
        
        # 기간 계산
        if dates:
            dates.sort()
            period_text = config['template']['period'].format(
                start_date=dates[0].split()[0] if dates else '',
                end_date=dates[-1].split()[0] if dates else ''
            )
        else:
            period_text = "기간 정보 없음"
        
        # 국가 정보
        countries_text = config['template']['countries'].format(
            countries=', '.join(sorted(countries)[:config['max_items_display']])
        )
        
        # 채널 정보
        channels_text = config['template']['channels'].format(
            channels=', '.join(sorted(channels))
        )
        
        # IP 개수
        ip_count_text = config['template']['ip_count'].format(
            count=len(ips)
        )
        
        # 최종 조합
        return f"{period_text} {countries_text} {channels_text}, {ip_count_text}"
    
    @staticmethod
    def format_stds_dtm_summary(stds_data: Dict) -> str:
        """STDS_DTM 요약을 텍스트로 변환"""
        config = TomlFieldConfig.STDS_DTM_SUMMARY
        
        # 헤더
        header = config['template']['header'].format(
            date=stds_data.get('date', '')
        )
        
        # Orderbook과 동일한 형식 사용
        if config['use_same_format_as_orderbook']:
            # 패턴 데이터 구성 - details 형식을 통일
            patterns = {
                'total_buy_amount': stds_data.get('buy_amount', 0),
                'buy_details': [],
                'total_sell_amount': stds_data.get('sell_amount', 0),
                'sell_details': [],
                'total_deposit_krw': stds_data.get('deposit_krw_amount', 0),
                'total_withdraw_krw': stds_data.get('withdraw_krw_amount', 0),
                'total_deposit_crypto': stds_data.get('deposit_crypto_amount', 0),
                'deposit_crypto_details': [],
                'total_withdraw_crypto': stds_data.get('withdraw_crypto_amount', 0),
                'withdraw_crypto_details': []
            }
            
            # buy_details 처리
            if stds_data.get('buy_details'):
                for item in stds_data['buy_details']:
                    if isinstance(item, dict):
                        # 이미 딕셔너리 형식
                        patterns['buy_details'].append(item)
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        # 리스트/튜플 형식을 딕셔너리로 변환
                        patterns['buy_details'].append({
                            'ticker': item[0],
                            'amount_krw': item[1] if isinstance(item[1], (int, float)) else item[1].get('amount', 0),
                            'count': item[1].get('count', 1) if isinstance(item[1], dict) else 1,
                            'quantity': item[1].get('quantity', 0) if isinstance(item[1], dict) else 0
                        })
            
            # sell_details 처리
            if stds_data.get('sell_details'):
                for item in stds_data['sell_details']:
                    if isinstance(item, dict):
                        patterns['sell_details'].append(item)
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        patterns['sell_details'].append({
                            'ticker': item[0],
                            'amount_krw': item[1] if isinstance(item[1], (int, float)) else item[1].get('amount', 0),
                            'count': item[1].get('count', 1) if isinstance(item[1], dict) else 1,
                            'quantity': item[1].get('quantity', 0) if isinstance(item[1], dict) else 0
                        })
            
            # deposit_crypto_details 처리
            if stds_data.get('deposit_crypto_details'):
                for item in stds_data['deposit_crypto_details']:
                    if isinstance(item, dict):
                        patterns['deposit_crypto_details'].append(item)
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        patterns['deposit_crypto_details'].append({
                            'ticker': item[0] if len(item) > 0 else 'Unknown',
                            'amount_krw': item[1] if isinstance(item[1], (int, float)) else item[1].get('amount', 0),
                            'count': item[1].get('count', 1) if isinstance(item[1], dict) else 1,
                            'quantity': item[1].get('quantity', 0) if isinstance(item[1], dict) else 0
                        })
            
            # withdraw_crypto_details 처리
            if stds_data.get('withdraw_crypto_details'):
                for item in stds_data['withdraw_crypto_details']:
                    if isinstance(item, dict):
                        patterns['withdraw_crypto_details'].append(item)
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        patterns['withdraw_crypto_details'].append({
                            'ticker': item[0] if len(item) > 0 else 'Unknown',
                            'amount_krw': item[1] if isinstance(item[1], (int, float)) else item[1].get('amount', 0),
                            'count': item[1].get('count', 1) if isinstance(item[1], dict) else 1,
                            'quantity': item[1].get('quantity', 0) if isinstance(item[1], dict) else 0
                        })
            
            # 더미 기간 정보 (날짜만 사용)
            period = {
                'start_date': stds_data.get('date', ''),
                'end_date': stds_data.get('date', '')
            }
            
            # Orderbook 포맷 함수 재사용
            body = TomlDataProcessor.format_orderbook_summary(patterns, period)
            
            # 헤더와 본문 결합
            return f"{header} {config['template']['body']} {body}"
        
        return header


    @staticmethod
    def process_related_person_transactions(transactions: List[Dict]) -> str:
        """관련인 거래 내역을 텍스트로 변환"""
        config = TomlFieldConfig.RELATED_PERSON['individual']
        
        # 내부입고/출고 분리
        deposits = []
        withdrawals = []
        
        for t in transactions:
            formatted = config['transaction_format'].format(
                type=t.get('tran_type', ''),
                ticker=t.get('coin_symbol', ''),
                quantity=float(t.get('tran_qty', 0)),
                amount=int(float(t.get('tran_amt', 0))),
                count=int(t.get('tran_cnt', 0))
            )
            
            if '입고' in t.get('tran_type', ''):
                deposits.append(formatted)
            else:
                withdrawals.append(formatted)
        
        result = []
        if deposits:
            result.append(f"내부입고: {', '.join(deposits)}")
        if withdrawals:
            result.append(f"내부출고: {', '.join(withdrawals)}")
        
        return ' | '.join(result) if result else "거래 내역 없음"


# 싱글톤 인스턴스
toml_config = TomlFieldConfig()
toml_processor = TomlDataProcessor()