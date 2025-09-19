# str_dashboard/utils/queries/stage_3/ip_access_executor.py
"""
IP 접속 이력 조회 실행 모듈
Stage 2 데이터를 활용한 통합 조회
"""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal

from .sql_templates import IP_ACCESS_HISTORY_QUERY

logger = logging.getLogger(__name__)


class IPAccessExecutor:
    """
    Stage 3: IP 접속 이력 조회 실행 클래스
    """
    
    def __init__(self, db_connection):
        self.db_conn = db_connection
        
    def execute(self, stage_1_metadata: Dict[str, Any], 
                stage_2_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        주 고객 및 관련인의 IP 접속 이력 통합 조회
        
        Args:
            stage_1_metadata: Stage 1 메타데이터 (거래 기간 등)
            stage_2_data: Stage 2 결과 (고객 정보, 관련인 정보)
            
        Returns:
            통합 IP 접속 결과
        """
        try:
            # 필요 정보 추출
            start_date = stage_1_metadata.get('tran_start')
            end_date = stage_1_metadata.get('tran_end')
            
            if not start_date or not end_date:
                return {
                    'success': False,
                    'message': 'No transaction period available'
                }
            
            # 날짜 정리
            clean_start = self._extract_date(start_date)
            clean_end = self._extract_date(end_date)
            
            # Stage 2 데이터에서 정보 추출
            customer_info = self._extract_customer_info(stage_2_data)
            related_persons = self._extract_related_persons_with_mid(stage_2_data)
            
            logger.info(f"[Stage 3] Processing IP access for main customer and "
                       f"{len(related_persons)} related persons")
            
            # 통합 IP 접속 데이터 수집
            all_ip_data = []
            
            # 1. 주 고객 IP 접속 이력
            if customer_info.get('mid'):
                main_result = self._query_ip_for_person(
                    customer_info['mid'],
                    customer_info['cust_id'],
                    customer_info['name'],
                    clean_start,
                    clean_end,
                    is_primary=True
                )
                if main_result['success']:
                    all_ip_data.extend(main_result['rows'])
            
            # 2. 관련인 IP 접속 이력 (개인인 경우만)
            if customer_info.get('customer_type') == 'PERSON':
                for person in related_persons:
                    if person.get('mid'):
                        related_result = self._query_ip_for_person(
                            person['mid'],
                            person['cust_id'],
                            person['name'],
                            clean_start,
                            clean_end,
                            is_primary=False
                        )
                        if related_result['success']:
                            all_ip_data.extend(related_result['rows'])
            
            # 통합 DataFrame 구조 생성
            unified_result = self._create_unified_structure(all_ip_data)
            
            return {
                'success': True,
                'unified_ip_data': unified_result,  # 키 이름 수정
                'ip_access_data': unified_result,   # 호환성을 위해 양쪽 다 제공
                'summary': {
                    'period': f'{clean_start} ~ {clean_end}',
                    'total_persons': len(set([row[0] for row in all_ip_data])) if all_ip_data else 0,
                    'total_records': len(all_ip_data),
                    'main_customer': customer_info
                }
            }
            
        except Exception as e:
            logger.exception(f"[Stage 3] Error in execute: {e}")
            return {
                'success': False,
                'message': f"IP 접속 이력 조회 실패: {str(e)}"
            }
    
    def _extract_customer_info(self, stage_2_data: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2에서 주 고객 정보 추출"""
        customer_df = stage_2_data.get('dataframes', {}).get('customer')
        
        if customer_df is None:
            return {}
        
        # DataFrame 데이터 구조에서 추출
        cols = customer_df.get('columns', [])
        rows = customer_df.get('rows', [])
        
        if not rows:
            return {}
        
        row = rows[0]  # 첫 번째 행 (주 고객)
        
        info = {}
        if '고객ID' in cols:
            info['cust_id'] = row[cols.index('고객ID')]
        if 'MID' in cols:
            info['mid'] = row[cols.index('MID')]
        if '성명' in cols:
            info['name'] = row[cols.index('성명')]
        if '고객구분' in cols:
            cust_type = row[cols.index('고객구분')]
            info['customer_type'] = 'PERSON' if '개인' in str(cust_type) else 'CORP'
        
        return info
    
    def _extract_related_persons_with_mid(self, stage_2_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """MID가 있는 관련인만 추출"""
        related_df = stage_2_data.get('dataframes', {}).get('related_persons')
        if not related_df:
            return []

        # DataFrame dict 구조 확인
        if isinstance(related_df, dict):
            cols = related_df.get('columns', [])
            rows = related_df.get('rows', [])
        else:
            # pandas DataFrame인 경우
            cols = related_df.columns.tolist()
            rows = related_df.values.tolist()
        
        related_persons = []
        
        # Stage 2의 통합 DataFrame 구조에 따라 데이터 추출
        for row in rows:
            person = {}
            
            # 기본 정보
            if '관련인고객ID' in cols:
                person['cust_id'] = row[cols.index('관련인고객ID')]
            if '관련인성명' in cols:
                person['name'] = row[cols.index('관련인성명')]
            
            # MID 정보 - Stage 2에서 이미 조회됨
            if '관련인MID' in cols:
                person['mid'] = row[cols.index('관련인MID')]
            elif 'MID' in cols:  # 컬럼명이 다를 수 있음
                person['mid'] = row[cols.index('MID')]
            
            # MID가 있는 경우만 추가
            if person.get('mid'):
                related_persons.append(person)
                logger.info(f"[Stage 3] Found related person: {person['name']}({person['mid']})")
        
        return related_persons
    

    def _query_ip_for_person(self, mem_id: str, cust_id: str, name: str,
                            start_date: str, end_date: str, 
                            is_primary: bool = False) -> Dict[str, Any]:
        """개인별 IP 접속 이력 조회"""
        try:
            with self.db_conn.cursor() as cursor:
                # Oracle 스타일 딕셔너리 바인딩
                params = {
                    'mem_id': mem_id,
                    'start_date': start_date,
                    'end_date': end_date
                }
                cursor.execute(IP_ACCESS_HISTORY_QUERY, params)
                rows = cursor.fetchall()
                # cols = [desc[0] for desc in cursor.description]  # 제거
                
                # 각 행에 고객 정보 추가
                enhanced_rows = []
                for row in rows:
                    # 기존 row + 추가 정보
                    enhanced_row = [
                        cust_id,                    # 고객ID
                        name,                       # 고객명
                        'PRIMARY' if is_primary else 'RELATED',  # 구분
                        mem_id                      # MID
                    ] + list(row)
                    
                    # Decimal 타입 변환
                    converted_row = []
                    for value in enhanced_row:
                        if isinstance(value, Decimal):
                            converted_row.append(float(value))
                        else:
                            converted_row.append(value)
                    
                    enhanced_rows.append(converted_row)
                
                logger.info(f"[Stage 3] IP query for {name}({mem_id}): {len(rows)} records")
                
                return {
                    'success': True,
                    'rows': enhanced_rows
                }
                
        except Exception as e:
            logger.error(f"[Stage 3] Error querying IP for {mem_id}: {e}")
            return {'success': False, 'rows': []}



    def _create_unified_structure(self, all_rows: List[List]) -> Dict[str, Any]:
        """통합 DataFrame 구조 생성"""
        # 통합 컬럼 정의
        unified_columns = [
            '고객ID',
            '고객명',
            '구분',  # PRIMARY/RELATED
            'MID',
            '국가한글명',
            '채널',
            '채널코드',
            '접속위치',
            'OS정보',
            '브라우저정보',
            'IP주소',
            '접속결과코드',
            '접속일시',
            '모바일접속코드',
            '접속유형코드',
            '헤더브라우저값'
        ]
        
        return {
            'columns': unified_columns,
            'rows': all_rows
        }
    
    def _extract_date(self, datetime_str: str) -> Optional[str]:
        """날짜 추출 (YYYY-MM-DD)"""
        try:
            if not datetime_str:
                return None
            date_part = str(datetime_str).split()[0]
            if len(date_part) == 10 and date_part[4] == '-' and date_part[7] == '-':
                return date_part
            return None
        except:
            return None