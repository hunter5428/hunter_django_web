# str_dashboard/utils/queries/stage_2/sql_templates.py
"""
Stage 2 SQL 쿼리 템플릿
고객 정보 및 관련인 정보 조회
"""

# ==================== 고객 기본 정보 (기존 유지) ====================
CUSTOMER_UNIFIED_INFO_QUERY = """
SELECT 
    -- 기본 식별 정보
    c.CUST_ID AS "고객ID",
    COALESCE(m.MEM_ID, c.KYC_EXE_MEM_ID) AS "MID",
    COALESCE(ce.CUST_KO_NM, c.CUST_KO_NM) AS "성명",
    COALESCE(ce.CUST_EN_NM, c.CUST_EN_NM) AS "영문명",
    
    -- 고객 구분 및 상태
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'CUST_TYPE_CD' AND AML_DTL_CD = c.CUST_TYPE_CD) AS "고객구분",
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'CUST_RKGD_CD' AND AML_DTL_CD = ra.cust_rkgd_cd) AS "RA등급",
    CASE
        WHEN ce.AML_RSLT_CD = '200' THEN 'WLF 검토대기'
        WHEN ce.AML_RSLT_CD = '400' THEN '거래거절'
        WHEN ce.AML_RSLT_CD = '600' THEN 'CDD'
        WHEN ce.AML_RSLT_CD = '700' THEN 'EDD'
        WHEN ce.AML_RSLT_CD = '800' THEN 'EEDD'
        WHEN c.AML_RSLT_CD = '200' THEN 'WLF 검토대기'
        WHEN c.AML_RSLT_CD = '400' THEN '거래거절'
        WHEN c.AML_RSLT_CD = '600' THEN 'CDD'
        WHEN c.AML_RSLT_CD = '700' THEN 'EDD'
        WHEN c.AML_RSLT_CD = '800' THEN 'EEDD'
        ELSE NULL
    END AS "위험등급",
    c.CLB_YN AS "고액자산가",
    
    -- 신원 정보
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'RLNM_CERT_CD' AND AML_DTL_CD = c.RLNM_CERT_CD) AS "실명번호구분",
    AES_DECRYPT(c.RLNM_CERT_VAL) AS "실명번호",
    COALESCE(c.CUST_BDAY, ce.CORP_ESTB_DT, c.CORP_ESTB_DT) AS "생년월일",
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'CUST_GNDR_CD' AND AML_DTL_CD = c.CUST_GNDR_CD) AS "성별",
    
    -- 연락처 정보
    COALESCE(AES_DECRYPT(ce.CUST_TEL_NO), AES_DECRYPT(c.CUST_TEL_NO)) AS "연락처",
    COALESCE(AES_DECRYPT(ce.CUST_EMAIL), AES_DECRYPT(c.CUST_EMAIL)) AS "이메일",
    
    -- 국적 정보
    n1.NAT_KO_NM AS "국적",
    n1.LEN2_ABBR_NAT_CD AS "국적코드",
    
    -- 거주지/본점 정보
    n2.NAT_KO_NM AS "거주지국가",
    COALESCE(ce.CUST_ZIPCD, c.CUST_ZIPCD) AS "거주지우편번호",
    COALESCE(ce.CUST_ADDR, c.CUST_ADDR) AS "거주지주소",
    COALESCE(ce.CUST_DTL_ADDR, c.CUST_DTL_ADDR) AS "거주지상세주소",
    
    -- 직업/직장/업종 정보
    CASE
        WHEN c.CUST_TYPE_CD = '01' THEN
            CASE
                WHEN j.JOB_LV_2_NM IS NULL THEN j.JOB_LV_1_NM
                ELSE j.JOB_LV_1_NM || ' (' || j.JOB_LV_2_NM || ')'
            END
        ELSE
            COALESCE(bz.AML_DTL_CD_NM, b.AML_BZTYP_TYPE_CD_CTNT, c.BZTYP_DTL_CTNT)
    END AS "직업",
    c.WPLC_NM AS "직장명",
    c.WPLC_ADDR AS "직장주소",
    c.WPLC_DTL_ADDR AS "직장상세주소",
    
    -- 거래 정보
    CASE
        WHEN c.CUST_TYPE_CD = '01' THEN
            (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'PERS_TRAN_PUPS_CD' AND AML_DTL_CD = COALESCE(ce.TRAN_PUPS_CD, c.TRAN_PUPS_CD))
        ELSE
            (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'CORP_TRAN_PUPS_CD' AND AML_DTL_CD = COALESCE(ce.TRAN_PUPS_CD, c.TRAN_PUPS_CD))
    END AS "거래목적",
    
    -- STR 관련 정보
    (SELECT COUNT(r.STR_RPT_MNGT_NO) 
     FROM BTCAMLDB_OWN.STR_RPT_BASE r 
     WHERE r.CUST_ID = c.CUST_ID 
       AND r.XML_CRET_FILE_NM IS NOT NULL) AS "STR보고건수",
    
    (SELECT TO_CHAR(MAX(r.STR_RPT_DTM), 'YYYY-MM-DD') 
     FROM BTCAMLDB_OWN.STR_RPT_BASE r 
     WHERE r.CUST_ID = c.CUST_ID 
       AND r.XML_CRET_FILE_NM IS NOT NULL) AS "최종STR보고일",
    
    (SELECT COUNT(*) 
     FROM BTCAMLDB_OWN.STR_ALERT_LIST a 
     WHERE a.CUST_ID = c.CUST_ID) AS "Alert건수",
    
    -- KYC 심사 정보
    TO_CHAR(c.KYC_EXE_FNS_DTM, 'YYYY-MM-DD HH24:MI:SS') AS "KYC완료일시",
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'EXAM_RVIW_STAT_CD' AND AML_DTL_CD = c.EXAM_RVIW_STAT_CD) AS "심사상태",
    
    -- 고객 타입 (내부 사용)
    c.CUST_TYPE_CD AS "CUST_TYPE_CD"
    
FROM BTCAMLDB_OWN.KYC_CUST_BASE c
LEFT JOIN BTCAMLDB_OWN.KYC_MEM_BASE m ON c.CUST_ID = m.CUST_ID
LEFT JOIN BTCAMLDB_OWN.RA_CUST_RKAT_GRD_LIST ra ON ra.CUST_ID = c.CUST_ID
    AND ra.RKAT_DTM = (SELECT MAX(sub_ra.RKAT_DTM) FROM BTCAMLDB_OWN.RA_CUST_RKAT_GRD_LIST sub_ra WHERE sub_ra.CUST_ID = c.CUST_ID)
LEFT JOIN BTCAMLDB_OWN.KYC_JOB_BASE j ON j.AML_JOB_CD = c.JOB_CD
LEFT JOIN BTCAMLDB_OWN.KYC_BZTYP_BASE b ON b.AML_BZTYP_TYPE_CD = c.BZTYP_TYPE_CD
LEFT JOIN BTCAMLDB_OWN.DM_SYS_NAT_BASE n1 ON n1.LEN3_ABBR_NAT_CD = c.CUST_NTNLT_CD
LEFT JOIN BTCAMLDB_OWN.DM_SYS_NAT_BASE n2 ON n2.LEN3_ABBR_NAT_CD = c.CUST_LIVE_NAT_CD
LEFT JOIN (
    SELECT ce_inner.*, ROW_NUMBER() OVER (PARTITION BY ce_inner.CUST_ID ORDER BY ce_inner.KYC_EXAM_STRT_DTM DESC) AS rn
    FROM BTCAMLDB_OWN.KYC_CORP_EXAM_LIST ce_inner
) ce ON ce.CUST_ID = c.CUST_ID AND ce.rn = 1
LEFT JOIN BTCAMLDB_OWN.SM_CD_DTL bz ON bz.AML_DTL_CD = ce.BZTYP_TYPE_CD AND bz.AML_COMN_CD = 'BZTYP_TYPE_CD'
WHERE c.CUST_ID = ?
"""

# ==================== 법인 관련인 (기존 유지) ====================
CORP_RELATED_PERSONS_QUERY = """
WITH LATEST_KYC AS (
    SELECT 
        CUST_ID,
        MAX(KYC_EXAM_STRT_DTM) AS LATEST_KYC_DTM
    FROM BTCAMLDB_OWN.KYC_CORP_EXAM_LIST
    WHERE CUST_ID = ?
    GROUP BY CUST_ID
),
RELATED_PERSONS AS (
    SELECT DISTINCT
        c1_0.cust_id,
        c1_0.relpr_type_cd,
        c1_0.relpr_seq,
        c1_0.real_ownr_stke_rate,
        n1_0.relpr_ko_nm,
        n1_0.relpr_en_nm,
        n1_0.relpr_addr,
        n1_0.relpr_dtl_addr,
        n1_0.relpr_job_cd,
        n1_0.relpr_bday,
        n1_0.relpr_gndr_cd,
        n1_0.RLNM_CERT_VAL,
        c1_0.relpr_id
    FROM BTCAMLDB_OWN.KYC_CORP_RELPR_BASE c1_0
    JOIN LATEST_KYC lk 
        ON c1_0.cust_id = lk.CUST_ID 
        AND c1_0.KYC_EXAM_STRT_DTM = lk.LATEST_KYC_DTM
    JOIN BTCAMLDB_OWN.KYC_NCUS_RELPR_BASE n1_0
        ON c1_0.cust_id = n1_0.cust_id
        AND c1_0.KYC_EXAM_STRT_DTM = n1_0.KYC_EXAM_STRT_DTM
        AND c1_0.relpr_seq = n1_0.relpr_seq
    WHERE c1_0.DEL_YN = 'N'
)
SELECT 
    rp.relpr_id AS "관련인고객ID",
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'AML_RELPR_TYPE_CD' AND AML_DTL_CD = rp.relpr_type_cd) AS "관계유형",
    rp.relpr_ko_nm AS "관련인성명",
    rp.relpr_en_nm AS "관련인영문명",
    rp.relpr_bday AS "관련인생년월일",
    CASE 
        WHEN rp.relpr_gndr_cd = 'M' THEN '남'
        WHEN rp.relpr_gndr_cd = 'F' THEN '여'
        ELSE rp.relpr_gndr_cd
    END AS "관련인성별",
    AES_DECRYPT(rp.RLNM_CERT_VAL) AS "관련인실명번호",
    rp.real_ownr_stke_rate AS "지분율",
    rp.relpr_type_cd AS "관계유형코드"
FROM RELATED_PERSONS rp
LEFT JOIN BTCAMLDB_OWN.KYC_JOB_BASE j1_0 ON rp.relpr_job_cd = j1_0.aml_job_cd
ORDER BY 
    CASE 
        WHEN rp.relpr_type_cd = '001' THEN 1
        WHEN rp.relpr_type_cd = '002' THEN 2
        WHEN rp.relpr_type_cd = '003' THEN 3
        ELSE 4
    END,
    rp.real_ownr_stke_rate DESC NULLS LAST
"""

# ==================== 개인 내부거래 상대방 조회 (개선) ====================
PERSON_INTERNAL_TRANSACTION_QUERY = """
WITH TRANSACTION_SUMMARY AS (
    SELECT 
        c1_0.cntp_cust_id AS related_cust_id,
        SUM(CASE WHEN c1_0.strls_type_cd = '01' THEN c1_0.coin_tran_amt * c1_0.coin_tran_qty ELSE 0 END) AS total_deposit_amount,
        SUM(CASE WHEN c1_0.strls_type_cd = '02' THEN c1_0.coin_tran_amt * c1_0.coin_tran_qty ELSE 0 END) AS total_withdraw_amount,
        COUNT(*) AS transaction_count
    FROM btcamldb_own.dm_coin_tran_list c1_0
    WHERE c1_0.coin_tran_dtm BETWEEN TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9') 
                                  AND TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9')
      AND c1_0.cust_id = ?
      AND c1_0.coin_ist_rels_type_cd = 'IN'
    GROUP BY c1_0.cntp_cust_id
    ORDER BY (total_deposit_amount + total_withdraw_amount) DESC
    FETCH FIRST 20 ROWS ONLY
)
SELECT 
    ts.related_cust_id AS "관련인고객ID",
    c.CUST_KO_NM AS "관련인성명",
    ts.total_deposit_amount AS "내부입고금액",
    ts.total_withdraw_amount AS "내부출고금액",
    ts.transaction_count AS "거래횟수"
FROM TRANSACTION_SUMMARY ts
LEFT JOIN btcamldb_own.kyc_cust_base c ON ts.related_cust_id = c.CUST_ID
ORDER BY (ts.total_deposit_amount + ts.total_withdraw_amount) DESC
"""

# 종목별 거래 상세 조회 (새로 추가)
PERSON_TRANSACTION_DETAIL_QUERY = """
SELECT 
    c1_0.cntp_cust_id AS "관련인고객ID",
    c4_0.coin_symbol_nm AS "종목",
    CASE 
        WHEN c1_0.strls_type_cd = '01' THEN '내부입고'
        WHEN c1_0.strls_type_cd = '02' THEN '내부출고'
        ELSE '기타'
    END AS "거래구분",
    SUM(c1_0.coin_tran_qty) AS "거래수량",
    SUM(COALESCE(c1_0.coin_tran_amt, 0) * COALESCE(c1_0.coin_tran_qty, 0)) AS "거래금액",
    COUNT(*) AS "거래건수"
FROM btcamldb_own.dm_coin_tran_list c1_0
LEFT JOIN btcamldb_own.dm_coin_base c4_0 
    ON c1_0.coin_type_cd = c4_0.coin_type_cd
WHERE c1_0.cust_id = ?
  AND c1_0.cntp_cust_id = ?
  AND c1_0.coin_tran_dtm BETWEEN TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9') 
                              AND TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9')
  AND c1_0.coin_ist_rels_type_cd = 'IN'
GROUP BY 
    c1_0.cntp_cust_id,
    c4_0.coin_symbol_nm,
    c1_0.strls_type_cd
ORDER BY "거래금액" DESC
"""

DUPLICATE_PERSONS_QUERY = """

WITH DUPLICATE_CANDIDATES AS (
    SELECT CUST_ID, 'ADDRESS' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id
      AND :address IS NOT NULL
      AND CUST_ADDR = :address
      AND CUST_DTL_ADDR = :detail_address
    
    UNION ALL
    
    SELECT CUST_ID, 'WORKPLACE_NAME' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id_wpn
      AND :workplace_name IS NOT NULL
      AND WPLC_NM IS NOT NULL
      AND WPLC_NM = :workplace_name
    
    UNION ALL
    
    SELECT CUST_ID, 'WORKPLACE_ADDRESS' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id_wpa
      AND :workplace_address IS NOT NULL
      AND WPLC_ADDR IS NOT NULL
      AND WPLC_ADDR = :workplace_address
      AND WPLC_DTL_ADDR = :workplace_detail_address
),
PHONE_MATCHED_CUST AS (
    SELECT CUST_ID
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE :phone_suffix IS NOT NULL
      AND SUBSTR(AES_DECRYPT(CUST_TEL_NO), -4) = :phone_suffix
),
UNIQUE_CANDIDATES AS (
    SELECT 
        CUST_ID,
        LISTAGG(MATCH_TYPE, ',') WITHIN GROUP (ORDER BY MATCH_TYPE) AS MATCH_TYPES
    FROM DUPLICATE_CANDIDATES
    GROUP BY CUST_ID
)
SELECT 
    UC.MATCH_TYPES "MATCH_TYPES",
    KB.CUST_ID "고객ID",
    KB.KYC_EXE_MEM_ID "MID",
    KB.CUST_KO_NM "성명",
    AES_DECRYPT(KB.RLNM_CERT_VAL) "실명번호",
    KB.CUST_BDAY "생년월일",
    AES_DECRYPT(KB.CUST_EMAIL) "E-mail",
    N1.NAT_KO_NM "국적",
    AES_DECRYPT(KB.CUST_TEL_NO) "휴대폰 번호",
    KB.CUST_ADDR || ' ' || KB.CUST_DTL_ADDR AS "거주주소",
    KB.WPLC_NM "직장명",
    KB.WPLC_ADDR || ' ' || KB.WPLC_DTL_ADDR AS "직장주소"
    
FROM UNIQUE_CANDIDATES UC
INNER JOIN BTCAMLDB_OWN.KYC_CUST_BASE KB ON UC.CUST_ID = KB.CUST_ID
LEFT JOIN BTCAMLDB_OWN.DM_SYS_NAT_BASE N1 ON KB.CUST_NTNLT_CD = N1.LEN3_ABBR_NAT_CD
WHERE UC.CUST_ID IN (SELECT CUST_ID FROM PHONE_MATCHED_CUST) -- 전화번호 매칭 결과 필터
  AND UC.CUST_ID != :current_cust_id_final -- 최종적으로 자기 자신은 제외
ORDER BY KB.CUST_ID
FETCH FIRST 50 ROWS ONLY

"""