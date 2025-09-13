-- person_related_summary.sql
-- 개인 고객의 관련인(내부입출금 거래 상대방) 조회
-- 바인드 변수: :cust_id, :start_date, :end_date

-- 1. 관련인 최대 10인 조회 (거래 횟수 기준)
WITH RELATED_PERSONS AS (
    SELECT 
        c1_0.cntp_cust_id AS related_cust_id,
        COUNT(c1_0.cntp_cust_id) AS tran_count
    FROM btcamldb_own.dm_coin_tran_list c1_0
    WHERE c1_0.coin_tran_dtm BETWEEN TO_TIMESTAMP(:start_date, 'YYYY-MM-DD HH24:MI:SS.FF9') 
                                  AND TO_TIMESTAMP(:end_date, 'YYYY-MM-DD HH24:MI:SS.FF9')
      AND c1_0.cust_id = :cust_id
      AND c1_0.coin_ist_rels_type_cd = 'IN'
    GROUP BY c1_0.cntp_cust_id
    ORDER BY COUNT(c1_0.cntp_cust_id) DESC
    FETCH FIRST 10 ROWS ONLY
),
-- 2. 관련인 기본 정보 조회
RELATED_PERSON_INFO AS (
    SELECT 
        c1_0.cust_id,
        c1_0.cust_ko_nm AS "성명",
        AES_DECRYPT(c1_0.RLNM_CERT_VAL) AS "실명번호",
        c1_0.cust_bday AS "생년월일",
        CASE 
            WHEN c1_0.cust_bday IS NOT NULL THEN 
                FLOOR(MONTHS_BETWEEN(SYSDATE, TO_DATE(c1_0.cust_bday, 'YYYYMMDD')) / 12)
            ELSE NULL
        END AS "만나이",
        (SELECT AML_DTL_CD_NM 
         FROM SM_CD_DTL 
         WHERE AML_COMN_CD = 'CUST_GNDR_CD' 
           AND AML_DTL_CD = c1_0.cust_gndr_cd) AS "성별",
        c1_0.cust_addr || ' ' || c1_0.cust_dtl_addr AS "거주지 정보",
        CASE
            WHEN j1_0.job_lv_2_nm IS NOT NULL 
            THEN j1_0.job_lv_1_nm || '(' || j1_0.job_lv_2_nm || ')'
            ELSE j1_0.job_lv_1_nm 
        END AS "직업",
        c1_0.wplc_nm AS "직장명",
        c1_0.wplc_addr || ' ' || c1_0.wplc_dtl_addr AS "직장주소",
        (SELECT AML_DTL_CD_NM
         FROM SM_CD_DTL
         WHERE AML_COMN_CD = 'PERS_PRMY_INCM_SURC_CD'
           AND AML_DTL_CD = c1_0.prmy_incm_surc_cd) AS "자금의 원천",
        (SELECT AML_DTL_CD_NM
         FROM SM_CD_DTL
         WHERE AML_COMN_CD = 'PERS_TRAN_PUPS_CD'
           AND AML_DTL_CD = c1_0.tran_pups_cd) AS "거래목적",
        CASE
            WHEN c1_0.AML_RSLT_CD = '200' THEN 'WLF 검토대기'
            WHEN c1_0.AML_RSLT_CD = '400' THEN '거래거절'
            WHEN c1_0.AML_RSLT_CD = '600' THEN 'CDD'
            WHEN c1_0.AML_RSLT_CD = '700' THEN 'EDD'
            WHEN c1_0.AML_RSLT_CD = '800' THEN 'EEDD'
        END AS "위험등급",
        rp.tran_count AS "거래횟수"
    FROM RELATED_PERSONS rp
    INNER JOIN btcamldb_own.kyc_cust_base c1_0 
        ON rp.related_cust_id = c1_0.cust_id
    LEFT JOIN btcamldb_own.kyc_job_base j1_0 
        ON c1_0.job_cd = j1_0.aml_job_cd
    WHERE c1_0.cust_type_cd = '01'  -- 개인 고객만
),
-- 3. 관련인들과의 내부거래 규모 (종목별 집계)
TRANSACTION_SUMMARY AS (
    SELECT 
        c1_0.cntp_cust_id AS related_cust_id,
        c3_0.cust_ko_nm AS "관련인명",
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
    INNER JOIN RELATED_PERSONS rp 
        ON c1_0.cntp_cust_id = rp.related_cust_id
    LEFT JOIN btcamldb_own.dm_cust_base c3_0 
        ON c1_0.cntp_cust_id = c3_0.cust_id
    LEFT JOIN btcamldb_own.dm_coin_base c4_0 
        ON c1_0.coin_type_cd = c4_0.coin_type_cd
    WHERE c1_0.cust_id = :cust_id
      AND c1_0.coin_tran_dtm BETWEEN TO_TIMESTAMP(:start_date, 'YYYY-MM-DD HH24:MI:SS.FF9') 
                                  AND TO_TIMESTAMP(:end_date, 'YYYY-MM-DD HH24:MI:SS.FF9')
      AND c1_0.coin_ist_rels_type_cd = 'IN'
    GROUP BY 
        c1_0.cntp_cust_id,
        c3_0.cust_ko_nm,
        c4_0.coin_symbol_nm,
        c1_0.strls_type_cd
)
-- 최종 결과 조합
SELECT 
    'PERSON_INFO' AS RECORD_TYPE,
    rpi.cust_id AS CUST_ID,
    rpi."성명" AS NAME,
    rpi."실명번호" AS ID_NUMBER,
    rpi."생년월일" AS BIRTH_DATE,
    rpi."만나이" AS AGE,
    rpi."성별" AS GENDER,
    rpi."거주지 정보" AS ADDRESS,
    rpi."직업" AS JOB,
    rpi."직장명" AS WORKPLACE,
    rpi."직장주소" AS WORKPLACE_ADDR,
    rpi."자금의 원천" AS INCOME_SOURCE,
    rpi."거래목적" AS TRAN_PURPOSE,
    rpi."위험등급" AS RISK_GRADE,
    rpi."거래횟수" AS TOTAL_TRAN_COUNT,
    NULL AS COIN_SYMBOL,
    NULL AS TRAN_TYPE,
    NULL AS TRAN_QTY,
    NULL AS TRAN_AMT,
    NULL AS TRAN_CNT
FROM RELATED_PERSON_INFO rpi

UNION ALL

SELECT 
    'TRAN_SUMMARY' AS RECORD_TYPE,
    ts.related_cust_id AS CUST_ID,
    ts."관련인명" AS NAME,
    NULL AS ID_NUMBER,
    NULL AS BIRTH_DATE,
    NULL AS AGE,
    NULL AS GENDER,
    NULL AS ADDRESS,
    NULL AS JOB,
    NULL AS WORKPLACE,
    NULL AS WORKPLACE_ADDR,
    NULL AS INCOME_SOURCE,
    NULL AS TRAN_PURPOSE,
    NULL AS RISK_GRADE,
    NULL AS TOTAL_TRAN_COUNT,
    ts."종목" AS COIN_SYMBOL,
    ts."거래구분" AS TRAN_TYPE,
    TO_CHAR(ts."거래수량") AS TRAN_QTY,
    TO_CHAR(ts."거래금액") AS TRAN_AMT,
    TO_CHAR(ts."거래건수") AS TRAN_CNT
FROM TRANSACTION_SUMMARY ts

ORDER BY 
    CUST_ID,
    RECORD_TYPE DESC,  -- PERSON_INFO가 먼저 오도록
    TRAN_AMT DESC NULLS LAST