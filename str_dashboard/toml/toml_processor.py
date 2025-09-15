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
        
        elif rule == 'year_with_age':
            # 생년월일을 연도와 만 나이로 변환
            return TomlDataProcessor._format_birthdate_with_age(value)
        
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
    def _format_birthdate_with_age(birthdate: str) -> str:
        """생년월일을 연도와 만 나이로 변환"""
        if not birthdate:
            return birthdate
        
        # 다양한 형식 처리
        birthdate_clean = re.sub(r'[^0-9]', '', str(birthdate))
        
        if len(birthdate_clean) < 8:
            # 연도만 있는 경우
            if len(birthdate_clean) >= 4:
                year = int(birthdate_clean[:4])
                age = datetime.now().year - year
                return f"{year}년생 (만 {age}세)"
            return birthdate
        
        try:
            # YYYYMMDD 형식
            year = int(birthdate_clean[:4])
            month = int(birthdate_clean[4:6])
            day = int(birthdate_clean[6:8])
            
            birth_date = datetime(year, month, day)
            today = datetime.now()
            
            # 만 나이 계산
            age = today.year - birth_date.year
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            return f"{year}년생 (만 {age}세)"
            
        except (ValueError, IndexError):
            # 파싱 실패 시 연도만 반환
            if len(birthdate_clean) >= 4:
                year = birthdate_clean[:4]
                return f"{year}년생"
            return birthdate
    
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
    
    @staticmethod
    def extract_sspc_text(sspc_value: str) -> str:
        """SSPC 값에서 숫자 코드를 제거하고 텍스트만 추출"""
        if not sspc_value:
            return ''
        
        # (숫자, 텍스트) 패턴에서 텍스트만 추출
        pattern = r'\(\d+,\s*([^)]+)\)'
        matches = re.findall(pattern, str(sspc_value))
        
        if matches:
            return ', '.join(matches)
        
        # 패턴이 없으면 원본 반환
        return str(sspc_value)
    
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
    
    @staticmethod
    def format_amount(amount: float) -> str:
        """금액을 읽기 쉬운 형식으로 변환 (억원, 만원 단위)"""
        if amount == 0:
            return '0원'
        
        abs_amount = abs(amount)
        
        if abs_amount >= 100000000:  # 1억 이상
            eok = int(abs_amount // 100000000)
            man = int((abs_amount % 100000000) // 10000)
            if man > 0:
                return f"{eok:,}억 {man:,}만원"
            return f"{eok:,}억원"
        elif abs_amount >= 10000:  # 1만 이상
            man = int(abs_amount // 10000)
            return f"{man:,}만원"
        else:
            return f"{int(abs_amount):,}원"
    
    @staticmethod
    def format_orderbook_summary(patterns: Dict, period: Dict) -> str:
        """Orderbook 패턴을 텍스트 요약으로 변환"""
        config = toml_config.ORDERBOOK_SUMMARY
        sentences = []
        
        # 헤더 (기간 정보)
        if period.get('start_date') and period.get('end_date'):
            header = config['template']['period'].format(
                start_date=period['start_date'],
                end_date=period['end_date']
            )
            sentences.append(header)
        
        # 각 액션별 요약
        actions = [
            ('buy', 'total_buy_amount', 'buy_details'),
            ('sell', 'total_sell_amount', 'sell_details'),
            ('deposit_krw', 'total_deposit_krw', None),
            ('withdraw_krw', 'total_withdraw_krw', None),
            ('deposit_crypto', 'total_deposit_crypto', 'deposit_crypto_details'),
            ('withdraw_crypto', 'total_withdraw_crypto', 'withdraw_crypto_details')
        ]
        
        for action_key, amount_key, details_key in actions:
            amount = patterns.get(amount_key, 0)
            
            if amount > 0:
                amount_str = TomlDataProcessor.format_amount(amount)
                
                # 상세 내역이 있는 경우
                if details_key and patterns.get(details_key):
                    details = patterns[details_key][:config['top_n_tickers']]
                    detail_parts = []
                    
                    for item in details:
                        if isinstance(item, tuple) and len(item) == 2:
                            ticker, data = item
                            if isinstance(data, dict):
                                detail_str = config['template']['detail'].format(
                                    ticker=ticker,
                                    amount=TomlDataProcessor.format_amount(data.get('amount_krw', 0)),
                                    count=data.get('count', 0),
                                    quantity=data.get('quantity', 0)
                                )
                                detail_parts.append(detail_str)
                    
                    if detail_parts:
                        details_str = ', '.join(detail_parts)
                        sentence = config['template'][action_key].format(
                            amount=amount_str,
                            details=details_str
                        )
                    else:
                        sentence = config['template'][action_key].format(
                            amount=amount_str,
                            details=''
                        ).rstrip()
                else:
                    # 상세 내역이 없는 경우 (원화 입출금)
                    if action_key in ['deposit_krw', 'withdraw_krw']:
                        sentence = config['template'][action_key].format(amount=amount_str)
                    else:
                        sentence = f"{amount_str}을 {config['action_mapping'][action_key]} 하였으며"
                
                sentences.append(sentence)
        
        return ' '.join(sentences)
    
    @staticmethod
    def format_stds_dtm_summary(stds_data: Dict) -> str:
        """STDS_DTM 요약을 텍스트로 변환"""
        config = toml_config.STDS_DTM_SUMMARY
        
        # 헤더
        header = config['template']['header'].format(
            date=stds_data.get('date', '')
        )
        
        # Orderbook과 동일한 형식으로 패턴 구성
        patterns = {
            'total_buy_amount': stds_data.get('buy_amount', 0),
            'buy_details': stds_data.get('buy_details', []),
            'total_sell_amount': stds_data.get('sell_amount', 0),
            'sell_details': stds_data.get('sell_details', []),
            'total_deposit_krw': stds_data.get('deposit_krw_amount', 0),
            'total_withdraw_krw': stds_data.get('withdraw_krw_amount', 0),
            'total_deposit_crypto': stds_data.get('deposit_crypto_amount', 0),
            'deposit_crypto_details': stds_data.get('deposit_crypto_details', []),
            'total_withdraw_crypto': stds_data.get('withdraw_crypto_amount', 0),
            'withdraw_crypto_details': stds_data.get('withdraw_crypto_details', [])
        }
        
        # 기간 정보 (당일만)
        period = {
            'start_date': stds_data.get('date', ''),
            'end_date': stds_data.get('date', '')
        }
        
        # Orderbook 포맷 함수 재사용
        body = TomlDataProcessor.format_orderbook_summary(patterns, period)
        
        # 기간 정보 부분 제거 (당일이므로)
        body = body.replace(f"대표 혐의 기간 {period['start_date']} ~ {period['end_date']} 동안 ", "")
        
        return f"{header} {config['template']['body']} {body}"
    
    @staticmethod
    def format_ip_access_summary(ip_data: List[Dict]) -> str:
        """IP 접속 이력을 텍스트 요약으로 변환"""
        if not ip_data:
            return "IP 접속 이력이 없습니다"
        
        config = toml_config.IP_ACCESS_HISTORY
        
        # 데이터 집계
        dates = []
        countries = set()
        channels = set()
        ips = set()
        
        for row in ip_data:
            if '접속일시' in row and row['접속일시']:
                dates.append(row['접속일시'])
            if '국가한글명' in row and row['국가한글명']:
                countries.add(row['국가한글명'])
            if '채널' in row and row['채널']:
                channels.add(row['채널'])
            if 'IP주소' in row and row['IP주소']:
                ips.add(row['IP주소'])
        
        # 기간 계산
        if dates:
            dates.sort()
            # 날짜 부분만 추출
            start_date = dates[0].split()[0] if ' ' in dates[0] else dates[0]
            end_date = dates[-1].split()[0] if ' ' in dates[-1] else dates[-1]
            period_text = config['template']['period'].format(
                start_date=start_date,
                end_date=end_date
            )
        else:
            period_text = "기간 정보 없음"
        
        # 국가 정보
        if countries:
            countries_list = sorted(list(countries))[:config['max_items_display']]
            countries_text = config['template']['countries'].format(
                countries=', '.join(countries_list)
            )
        else:
            countries_text = "접속 국가 정보 없음"
        
        # 채널 정보
        if channels:
            channels_text = config['template']['channels'].format(
                channels=', '.join(sorted(channels))
            )
        else:
            channels_text = ""
        
        # IP 개수
        ip_count_text = config['template']['ip_count'].format(
            count=len(ips)
        )
        
        # 최종 조합
        parts = [period_text, countries_text]
        if channels_text:
            parts.append(channels_text)
        parts.append(ip_count_text)
        
        return ' '.join(parts)
    
    @staticmethod
    def process_related_person_transactions(transactions: List[Dict]) -> str:
        """관련인 거래 내역을 텍스트로 변환"""
        config = toml_config.RELATED_PERSON['individual']
        
        if not transactions:
            return "거래 내역 없음"
        
        # 내부입고/출고 분리
        deposits = []
        withdrawals = []
        
        for t in transactions:
            tran_type = t.get('tran_type', '')
            detail = config['detail_format'].format(
                ticker=t.get('coin_symbol', ''),
                quantity=float(t.get('tran_qty', 0)),
                amount=int(float(t.get('tran_amt', 0))),
                count=int(t.get('tran_cnt', 0))
            )
            
            if '입고' in tran_type:
                deposits.append(detail)
            elif '출고' in tran_type:
                withdrawals.append(detail)
        
        result = []
        if deposits:
            result.append(config['transaction_format'].format(
                type='입고',
                details=', '.join(deposits)
            ))
        if withdrawals:
            result.append(config['transaction_format'].format(
                type='출고',
                details=', '.join(withdrawals)
            ))
        
        return ' / '.join(result) if result else "거래 내역 없음"


# 싱글톤 인스턴스
toml_processor = TomlDataProcessor()