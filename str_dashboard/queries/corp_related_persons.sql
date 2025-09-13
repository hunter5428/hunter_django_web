-- 법인의 관련인 정보 조회 (중복 제거)
-- 바인드 변수: :cust_id
WITH LATEST_KYC AS (
    -- 최신 KYC 심사 시점 찾기
    SELECT 
        CUST_ID,
        MAX(KYC_EXAM_STRT_DTM) AS LATEST_KYC_DTM
    FROM BTCAMLDB_OWN.KYC_CORP_EXAM_LIST
    WHERE CUST_ID = :cust_id
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
        n1_0.relpr_wplc_addr,
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
    WHERE c1_0.DEL_YN = 'N'  -- 삭제되지 않은 데이터만
)
SELECT 
    (SELECT AML_DTL_CD_NM
     FROM SM_CD_DTL
     WHERE AML_COMN_CD = 'AML_RELPR_TYPE_CD'
       AND AML_DTL_CD = rp.relpr_type_cd) "관계",
    rp.relpr_ko_nm "관계인명",
    rp.relpr_en_nm "관계인 영문명",
    CASE 
        WHEN rp.relpr_addr IS NOT NULL AND rp.relpr_dtl_addr IS NOT NULL 
        THEN rp.relpr_addr || ' ' || rp.relpr_dtl_addr
        WHEN rp.relpr_addr IS NOT NULL 
        THEN rp.relpr_addr
        ELSE ''
    END AS "관계인 거주지정보",
    CASE
        WHEN j1_0.job_lv_2_nm IS NOT NULL 
        THEN j1_0.job_lv_1_nm || '(' || j1_0.job_lv_2_nm || ')'
        ELSE j1_0.job_lv_1_nm 
    END AS "관계인 직업",
    CASE 
        WHEN rp.relpr_wplc_addr IS NOT NULL AND rp.relpr_dtl_addr IS NOT NULL 
        THEN rp.relpr_wplc_addr || ' ' || rp.relpr_dtl_addr
        WHEN rp.relpr_wplc_addr IS NOT NULL 
        THEN rp.relpr_wplc_addr
        ELSE ''
    END AS "관계인 직장주소",
    rp.relpr_bday "관계인 생년월일",
    CASE 
        WHEN rp.relpr_gndr_cd = 'M' THEN '남'
        WHEN rp.relpr_gndr_cd = 'F' THEN '여'
        ELSE rp.relpr_gndr_cd
    END AS "관계인 성별",
    AES_DECRYPT(rp.RLNM_CERT_VAL) "관계인 실명번호",
    rp.real_ownr_stke_rate "관계인지분율",
    rp.relpr_id "관계인고객ID"
FROM RELATED_PERSONS rp
LEFT JOIN BTCAMLDB_OWN.KYC_JOB_BASE j1_0 
    ON rp.relpr_job_cd = j1_0.aml_job_cd
ORDER BY 
    CASE 
        WHEN rp.relpr_type_cd = '001' THEN 1  -- 실제소유자
        WHEN rp.relpr_type_cd = '002' THEN 2  -- 대표자
        WHEN rp.relpr_type_cd = '003' THEN 3  -- 대리인
        ELSE 4
    END,
    rp.real_ownr_stke_rate DESC NULLS LAST,  -- 지분율 높은 순
    rp.relpr_seq;