# -*- coding: utf-8 -*-
"""
RULE_ID_히스토리 탐색 - 개선된 버전
더 나은 에러 처리와 성능 최적화 포함
중첩 괄호 처리 개선
유사 RULE 조합 검색 기능 추가 (동일 유사도 모두 표시)
"""

import re
import logging
from typing import List, Tuple, Optional, Dict, Any
from contextlib import closing

import pandas as pd
import jaydebeapi

logger = logging.getLogger(__name__)

# SQL 쿼리를 상수로 정의
SQL_RULE_HISTORY = r"""
WITH R_SRC AS (
  SELECT DISTINCT STR_RPT_MNGT_NO, STR_RULE_ID
  FROM STR_ALERT_INFO_BASE
  WHERE STR_RULE_ID IS NOT NULL
),
R AS (
  SELECT
    STR_RPT_MNGT_NO,
    LISTAGG(STR_RULE_ID, ',') WITHIN GROUP (ORDER BY STR_RULE_ID) AS STR_RULE_ID_LIST
  FROM R_SRC
  GROUP BY STR_RPT_MNGT_NO
),
UPER_SRC AS (
  SELECT DISTINCT
    L.STR_RPT_MNGT_NO,
    L.UPER_STR_SSPC_PTTN_CD AS CODE,
    B.UPER_STR_SSPC_PTTN_CTNT AS TEXT
  FROM STR_RPT_SSPC_LIST L
  JOIN STR_SSPC_PTTN_BASE B
    ON B.UPER_STR_SSPC_PTTN_CD = L.UPER_STR_SSPC_PTTN_CD
  WHERE L.UPER_STR_SSPC_PTTN_CD IS NOT NULL
),
UPER AS (
  SELECT
    STR_RPT_MNGT_NO,
    LISTAGG('(' || TO_CHAR(CODE) || ', ' || TEXT || ')', ', ')
      WITHIN GROUP (ORDER BY CODE) AS STR_SSPC_UPER
  FROM UPER_SRC
  GROUP BY STR_RPT_MNGT_NO
),
LWER_SRC AS (
  SELECT DISTINCT
    L.STR_RPT_MNGT_NO,
    L.LWER_STR_SSPC_PTTN_CD AS CODE,
    B.STR_SSPC_PTTN_CTNT AS TEXT
  FROM STR_RPT_SSPC_LIST L
  JOIN STR_SSPC_PTTN_BASE B
    ON B.STR_SSPC_PTTN_CD = L.LWER_STR_SSPC_PTTN_CD
  WHERE L.LWER_STR_SSPC_PTTN_CD IS NOT NULL
),
LWER AS (
  SELECT
    STR_RPT_MNGT_NO,
    LISTAGG('(' || TO_CHAR(CODE) || ', ' || TEXT || ')', ', ')
      WITHIN GROUP (ORDER BY CODE) AS STR_SSPC_LWER
  FROM LWER_SRC
  GROUP BY STR_RPT_MNGT_NO
)
SELECT
  R.STR_RPT_MNGT_NO,
  R.STR_RULE_ID_LIST,
  U.STR_SSPC_UPER,
  W.STR_SSPC_LWER
FROM R
LEFT JOIN UPER U ON U.STR_RPT_MNGT_NO = R.STR_RPT_MNGT_NO
LEFT JOIN LWER W ON W.STR_RPT_MNGT_NO = R.STR_RPT_MNGT_NO
ORDER BY R.STR_RPT_MNGT_NO
"""


class RuleHistorySearchError(Exception):
    """Rule 히스토리 검색 관련 예외"""
    pass


def parse_pairs(cell: Any) -> List[Tuple[int, str]]:
    """
    '(num, text), (num2, text2)...' 형식의 문자열을 파싱
    중첩된 괄호를 올바르게 처리
    
    Args:
        cell: 파싱할 셀 값
    
    Returns:
        [(num, text), ...] 형태의 리스트
    """
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    
    cell_str = str(cell).strip()
    if not cell_str:
        return []
    
    pairs = []
    i = 0
    
    while i < len(cell_str):
        # '(' 찾기
        if cell_str[i] == '(':
            # 괄호 카운터
            paren_count = 1
            start_idx = i + 1
            j = i + 1
            
            # 매칭되는 ')' 찾기
            while j < len(cell_str) and paren_count > 0:
                if cell_str[j] == '(':
                    paren_count += 1
                elif cell_str[j] == ')':
                    paren_count -= 1
                j += 1
            
            # 괄호 내용 추출
            if paren_count == 0:
                content = cell_str[start_idx:j-1].strip()
                
                # 첫 번째 콤마를 기준으로 숫자와 텍스트 분리
                first_comma_idx = content.find(',')
                if first_comma_idx > 0:
                    num_str = content[:first_comma_idx].strip()
                    text_str = content[first_comma_idx+1:].strip()
                    
                    try:
                        num = int(num_str)
                        pairs.append((num, text_str))
                    except ValueError:
                        logger.warning(f"Failed to parse number from: {num_str}")
                
                i = j
            else:
                # 매칭되는 괄호를 찾지 못한 경우
                i += 1
        else:
            i += 1
    
    return pairs


def format_pairs_sorted_unique(pairs: List[Tuple[int, str]]) -> str:
    """
    중복 제거 후 코드 순으로 정렬하여 문자열로 포맷팅
    
    Args:
        pairs: (코드, 텍스트) 튜플 리스트
    
    Returns:
        포맷팅된 문자열
    """
    if not pairs:
        return ""
    
    # 중복 제거 및 정렬
    unique_pairs = {(int(n), str(t)) for n, t in pairs}
    sorted_pairs = sorted(unique_pairs, key=lambda x: (x[0], x[1]))
    
    # 문자열로 변환
    return ", ".join(f"({n}, {t})" for n, t in sorted_pairs)


def calculate_rule_similarity(rule_set1: set, rule_set2: set) -> float:
    """
    두 RULE 집합 간의 Jaccard 유사도 계산
    
    Args:
        rule_set1: 첫 번째 RULE ID 집합
        rule_set2: 두 번째 RULE ID 집합
    
    Returns:
        유사도 점수 (0.0 ~ 1.0)
    """
    if not rule_set1 or not rule_set2:
        return 0.0
    
    intersection = len(rule_set1 & rule_set2)
    union = len(rule_set1 | rule_set2)
    
    if union == 0:
        return 0.0
    
    return intersection / union


def find_most_similar_rule_combinations(target_rule_key: str, df: pd.DataFrame, max_results: int = 5) -> List[Dict[str, Any]]:
    """
    가장 유사한 RULE 조합들 찾기 (동일 유사도 모두 포함)
    
    Args:
        target_rule_key: 검색 대상 RULE_ID_LIST (콤마로 구분)
        df: 전체 집계 데이터 DataFrame
        max_results: 최대 반환 개수
    
    Returns:
        유사한 조합들의 리스트
    """
    if df.empty:
        return []
    
    # 타겟 RULE 집합 생성
    target_rules = set(target_rule_key.split(','))
    
    # 단일 RULE인 경우 유사도 검색 생략
    if len(target_rules) < 2:
        return []
    
    # 모든 후보에 대한 유사도 계산
    candidates = []
    
    for _, row in df.iterrows():
        rule_list = row['STR_RULE_ID_LIST']
        if pd.isna(rule_list) or rule_list == target_rule_key:
            continue
        
        candidate_rules = set(rule_list.split(','))
        
        # 유사도 계산
        similarity = calculate_rule_similarity(target_rules, candidate_rules)
        
        if similarity > 0:  # 유사도가 0보다 큰 경우만
            candidates.append({
                'rule_list': rule_list,
                'similarity': similarity,
                'count': row['STR_RULE_ID_NO_COUNT'],
                'uper': row.get('STR_SSPC_UPER'),
                'lwer': row.get('STR_SSPC_LWER')
            })
    
    # 유사도로 정렬
    candidates.sort(key=lambda x: (-x['similarity'], -x['count'], x['rule_list']))
    
    # 최소 유사도 임계값 (예: 0.3 = 30% 이상 일치)
    min_similarity = 0.3
    
    # 최고 유사도 찾기
    if candidates and candidates[0]['similarity'] >= min_similarity:
        max_similarity = candidates[0]['similarity']
        
        # 최고 유사도와 동일한 모든 조합 반환 (최대 max_results개)
        similar_combinations = []
        for candidate in candidates:
            if abs(candidate['similarity'] - max_similarity) < 0.001:  # 부동소수점 비교
                similar_combinations.append(candidate)
                if len(similar_combinations) >= max_results:
                    break
            elif candidate['similarity'] < max_similarity:
                break  # 정렬되어 있으므로 더 낮은 유사도면 중단
        
        return similar_combinations
    
    return []


def fetch_df_result_0(
    jdbc_url: str,
    driver_class: str,
    driver_path: str,
    username: str,
    password: str,
    prefetch: int = 1000
) -> pd.DataFrame:
    """
    오라클에서 기본 집계 데이터 조회
    
    Args:
        jdbc_url: JDBC 연결 URL
        driver_class: JDBC 드라이버 클래스
        driver_path: JDBC 드라이버 JAR 파일 경로
        username: 데이터베이스 사용자명
        password: 데이터베이스 비밀번호
        prefetch: 행 프리페치 크기 (성능 최적화)
    
    Returns:
        조회 결과 DataFrame
    
    Raises:
        RuleHistorySearchError: 조회 실패 시
    """
    conn = None
    try:
        # 연결 생성
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            [username, password],
            driver_path
        )
        
        # 성능 최적화: row prefetch 설정
        try:
            conn.jconn.setDefaultRowPrefetch(prefetch)
            logger.debug(f"Set row prefetch to {prefetch}")
        except Exception as e:
            logger.debug(f"Could not set row prefetch: {e}")
        
        # 쿼리 실행
        with closing(conn.cursor()) as cursor:
            logger.info("Executing rule history base query...")
            cursor.execute(SQL_RULE_HISTORY)
            
            # 결과 가져오기
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            
            logger.info(f"Fetched {len(rows)} rows from database")
            
            # DataFrame 생성
            df = pd.DataFrame(rows, columns=cols)
            return df
            
    except Exception as e:
        logger.exception(f"Failed to fetch rule history data: {e}")
        raise RuleHistorySearchError(f"데이터 조회 실패: {e}")
    
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")


def aggregate_by_rule_id_list(df_result_0: pd.DataFrame) -> pd.DataFrame:
    """
    STR_RULE_ID_LIST 기준으로 데이터 집계
    
    Args:
        df_result_0: 기본 조회 결과 DataFrame
    
    Returns:
        집계된 DataFrame (columns: STR_RULE_ID_LIST, STR_RULE_ID_NO_COUNT, 
                                  STR_SSPC_UPER, STR_SSPC_LWER)
    """
    # 빈 DataFrame 처리
    if df_result_0.empty:
        logger.info("Empty input DataFrame, returning empty result")
        return pd.DataFrame(columns=[
            "STR_RULE_ID_LIST",
            "STR_RULE_ID_NO_COUNT",
            "STR_SSPC_UPER",
            "STR_SSPC_LWER"
        ])
    
    # 작업용 복사본 생성
    df = df_result_0.copy()
    
    # 패턴 파싱
    logger.debug("Parsing UPER/LWER patterns...")
    df["__UPER_PAIRS__"] = df["STR_SSPC_UPER"].apply(parse_pairs)
    df["__LWER_PAIRS__"] = df["STR_SSPC_LWER"].apply(parse_pairs)
    
    # 그룹별 집계
    logger.debug("Aggregating by STR_RULE_ID_LIST...")
    aggregated_rows = []
    
    for rule_id_list, group_df in df.groupby("STR_RULE_ID_LIST", dropna=False):
        # 보고 건수 계산
        report_count = group_df["STR_RPT_MNGT_NO"].nunique()
        
        # 상위/하위 패턴 수집
        uper_pairs = []
        lwer_pairs = []
        
        for pairs_list in group_df["__UPER_PAIRS__"]:
            uper_pairs.extend(pairs_list)
        
        for pairs_list in group_df["__LWER_PAIRS__"]:
            lwer_pairs.extend(pairs_list)
        
        # 집계 결과 생성
        aggregated_rows.append({
            "STR_RULE_ID_LIST": rule_id_list,
            "STR_RULE_ID_NO_COUNT": report_count,
            "STR_SSPC_UPER": format_pairs_sorted_unique(uper_pairs) if uper_pairs else None,
            "STR_SSPC_LWER": format_pairs_sorted_unique(lwer_pairs) if lwer_pairs else None,
        })
    
    # DataFrame 생성
    result_df = pd.DataFrame(aggregated_rows, columns=[
        "STR_RULE_ID_LIST",
        "STR_RULE_ID_NO_COUNT",
        "STR_SSPC_UPER",
        "STR_SSPC_LWER"
    ])
    
    # 필터링: 1건 이상만 표시
    result_df = result_df[result_df["STR_RULE_ID_NO_COUNT"] >= 1].reset_index(drop=True)
    
    # 정렬
    result_df = result_df.sort_values(
        by=["STR_RULE_ID_LIST"],
        ascending=True,
        ignore_index=True
    )
    
    logger.info(f"Aggregation complete. Result has {len(result_df)} rows")
    
    return result_df


def search_rule_history(
    rule_key: str,
    jdbc_url: str,
    driver_class: str,
    driver_path: str,
    username: str,
    password: str
) -> Dict[str, Any]:
    """
    특정 RULE_ID_LIST 키로 히스토리 검색 (헬퍼 함수)
    
    Args:
        rule_key: 검색할 RULE_ID_LIST (예: "ID1,ID2,ID3")
        jdbc_url: JDBC 연결 URL
        driver_class: JDBC 드라이버 클래스
        driver_path: JDBC 드라이버 JAR 파일 경로
        username: 데이터베이스 사용자명
        password: 데이터베이스 비밀번호
    
    Returns:
        {'success': True/False, 'columns': [...], 'rows': [...], 
         'message': '...', 'similar_list': [...]}
    """
    try:
        # 전체 데이터 조회
        df0 = fetch_df_result_0(
            jdbc_url=jdbc_url,
            driver_class=driver_class,
            driver_path=driver_path,
            username=username,
            password=password
        )
        
        # 집계 처리
        df1 = aggregate_by_rule_id_list(df0)
        
        # 일치하는 행 필터링
        matching_df = df1[df1["STR_RULE_ID_LIST"] == rule_key]
        
        # 결과 변환
        columns = list(matching_df.columns) if not matching_df.empty else []
        rows = matching_df.values.tolist()
        
        # 결과가 없는 경우 유사 조합 검색
        if len(rows) == 0:
            similar_list = find_most_similar_rule_combinations(rule_key, df1)
            
            return {
                'success': True,
                'columns': columns,
                'rows': rows,
                'searched_rule': rule_key,
                'similar_list': similar_list  # 리스트로 변경
            }
        
        return {
            'success': True,
            'columns': columns,
            'rows': rows,
            'searched_rule': rule_key
        }
        
    except RuleHistorySearchError as e:
        return {
            'success': False,
            'message': str(e)
        }
    except Exception as e:
        logger.exception(f"Unexpected error in search_rule_history: {e}")
        return {
            'success': False,
            'message': f'예상치 못한 오류: {e}'
        }