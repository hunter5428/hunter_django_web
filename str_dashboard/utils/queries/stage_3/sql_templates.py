# str_dashboard/utils/queries/stage_3/sql_templates.py
"""
Stage 3 SQL 쿼리 템플릿
IP 접속 이력 조회
"""

IP_ACCESS_HISTORY_QUERY = """
SELECT 
    B.NAT_KO_NM AS "국가한글명",
    (SELECT AML_DTL_CD_NM 
     FROM SM_CD_DTL 
     WHERE AML_COMN_CD = 'SVC_USE_TYPE_CD' 
       AND AML_DTL_CD = A.SVC_USE_TYPE_CD) AS "채널",
    A.SVC_USE_TYPE_CD AS "채널코드",
    A.CONN_PST_NM AS "접속위치",
    A.OS_INFO_NM AS "OS정보",
    A.BRWR_INFO_NM AS "브라우저정보",
    A.IP AS "IP주소",
    A.MEM_CONN_RSLT_CD AS "접속결과코드",
    TO_CHAR(A.REG_DTM, 'YYYY-MM-DD HH24:MI:SS') AS "접속일시",
    A.MBIL_CONN_CD AS "모바일접속코드",
    A.CONN_TYPE_CD AS "접속유형코드",
    A.HEDR_BRWR_VAL AS "헤더브라우저값"
FROM DM_MEM_CONN_LIST A
INNER JOIN DM_SYS_NAT_BASE B
    ON A.NAT_CD = B.NAT_CD
WHERE A.MEM_ID = ?
  AND A.REG_DTM >= TO_DATE(?, 'YYYY-MM-DD')
  AND A.REG_DTM < TO_DATE(?, 'YYYY-MM-DD') + 1
ORDER BY A.REG_DTM DESC
"""