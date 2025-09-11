# -*- coding: utf-8 -*-
"""
RULE_ID_히스토리 탐색:
- df_result_0: 보고번호별 STR_RULE_ID_LIST, 상/하위 패턴 집계
- df_result_final: STR_RULE_ID_LIST 단위로 병합 집계
- 오라클 구버전에서도 동작하도록 LISTAGG(DISTINCT ...) 사용 금지
"""

from contextlib import closing
import re
import pandas as pd
import jaydebeapi

# SQL 주석을 Python 문자열 밖으로 이동
SQL_BASE = r"""
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

_PAIR_PATTERN = re.compile(r"\(\s*(\d+)\s*,\s*(.*?)\s*\)")

def parse_pairs(cell):
    """'(num, text), (num2, text2)...' 문자열 → [(num, text), ...]"""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    if not s:
        return []
    out = []
    for m in _PAIR_PATTERN.finditer(s):
        out.append((int(m.group(1)), m.group(2)))
    return out

def format_pairs_sorted_unique(pairs):
    """중복 제거 → 코드 오름차순 → 문자열"""
    uniq = {(int(n), str(t)) for n, t in pairs}
    pairs_sorted = sorted(uniq, key=lambda x: (x[0], x[1]))
    return ", ".join(f"({n}, {t})" for n, t in pairs_sorted)

def fetch_df_result_0(jdbc_url, driver_class, driver_path, username, password) -> pd.DataFrame:
    """오라클에서 df_result_0 조회"""
    with closing(jaydebeapi.connect(driver_class, jdbc_url, [username, password], driver_path)) as conn:
        try:
            conn.jconn.setDefaultRowPrefetch(1000)
        except Exception:
            pass
        with closing(conn.cursor()) as cur:
            cur.execute(SQL_BASE)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)

def aggregate_by_rule_id_list(df_result_0: pd.DataFrame) -> pd.DataFrame:
    """STR_RULE_ID_LIST 기준 병합 집계 (보고건수 + 상/하위 패턴 합집합)"""
    if df_result_0.empty:
        return pd.DataFrame(columns=[
            "STR_RULE_ID_LIST", "STR_RULE_ID_NO_COUNT", "STR_SSPC_UPER", "STR_SSPC_LWER"
        ])

    df = df_result_0.copy()
    df["__UPER_PAIRS__"] = df["STR_SSPC_UPER"].apply(parse_pairs)
    df["__LWER_PAIRS__"] = df["STR_SSPC_LWER"].apply(parse_pairs)

    groups = []
    for rule_id_list, sub in df.groupby("STR_RULE_ID_LIST", dropna=False):
        rpt_cnt = sub["STR_RPT_MNGT_NO"].nunique()

        uper, lwer = [], []
        for lst in sub["__UPER_PAIRS__"]:
            uper.extend(lst)
        for lst in sub["__LWER_PAIRS__"]:
            lwer.extend(lst)

        groups.append({
            "STR_RULE_ID_LIST": rule_id_list,
            "STR_RULE_ID_NO_COUNT": rpt_cnt,
            "STR_SSPC_UPER": format_pairs_sorted_unique(uper) if uper else None,
            "STR_SSPC_LWER": format_pairs_sorted_unique(lwer) if lwer else None,
        })

    df_final = pd.DataFrame(groups, columns=[
        "STR_RULE_ID_LIST", "STR_RULE_ID_NO_COUNT", "STR_SSPC_UPER", "STR_SSPC_LWER"
    ])

    # 규칙: 1건 이상만 표시
    df_final = df_final[df_final["STR_RULE_ID_NO_COUNT"] >= 1].reset_index(drop=True)

    # 정렬(키 자체가 오름차순 리스트 문자열)
    df_final = df_final.sort_values(by=["STR_RULE_ID_LIST"], ascending=True, ignore_index=True)
    return df_final