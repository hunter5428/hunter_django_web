-- duplicate_unified.sql
-- 모든 중복 조건을 UNION ALL로 한 번에 처리
-- 이메일 매칭은 암호화 함수 문제로 제외
WITH DUPLICATE_CANDIDATES AS (
    -- 이메일 매칭 (암호화 함수 문제로 주석 처리)
    -- SELECT CUST_ID, 'EMAIL' AS MATCH_TYPE
    -- FROM BTCAMLDB_OWN.KYC_CUST_BASE
    -- WHERE CUST_ID != :current_cust_id
    --   AND :encrypted_email IS NOT NULL
    --   AND CUST_EMAIL = :encrypted_email
    -- UNION ALL
    
    -- 주소 매칭
    SELECT CUST_ID, 'ADDRESS' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id
      AND :address IS NOT NULL
      AND CUST_ADDR = :address
      AND CUST_DTL_ADDR = :detail_address
    
    UNION ALL
    
    -- 직장명 매칭
    SELECT CUST_ID, 'WORKPLACE_NAME' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id
      AND :workplace_name IS NOT NULL
      AND WPLC_NM IS NOT NULL
      AND WPLC_NM = :workplace_name
    
    UNION ALL
    
    -- 직장주소 매칭
    SELECT CUST_ID, 'WORKPLACE_ADDRESS' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id
      AND :workplace_address IS NOT NULL
      AND WPLC_ADDR IS NOT NULL
      AND WPLC_ADDR = :workplace_address
      AND (:workplace_detail_address IS NULL OR WPLC_DTL_ADDR = :workplace_detail_address)
),
UNIQUE_CANDIDATES AS (
    -- 중복 제거 및 매칭 타입 집계
    SELECT 
        CUST_ID,
        LISTAGG(MATCH_TYPE, ',') WITHIN GROUP (ORDER BY MATCH_TYPE) AS MATCH_TYPES
    FROM DUPLICATE_CANDIDATES
    GROUP BY CUST_ID
)
SELECT 
    UC.MATCH_TYPES "MATCH_TYPES",  -- 가장 왼쪽으로 이동
    KB.CUST_ID "고객ID",
    KB.KYC_EXE_MEM_ID "MID",
    KB.CUST_KO_NM "성명",
    KB.CUST_EN_NM "영문명",
    (SELECT AML_DTL_CD_NM FROM SM_CD_DTL WHERE AML_COMN_CD = 'RLNM_CERT_CD' AND AML_DTL_CD = KB.RLNM_CERT_CD) "실명번호 구분",
    AES_DECRYPT(KB.RLNM_CERT_VAL) "실명번호",
    KB.CUST_BDAY "생년월일",
    AES_DECRYPT(KB.CUST_EMAIL) "E-mail",
    N1.NAT_KO_NM "국적",
    AES_DECRYPT(KB.CUST_TEL_NO) "휴대폰 번호",
    N2.NAT_KO_NM "거주주소국가",
    KB.CUST_ZIPCD "거주주소우편번호",
    CASE 
        WHEN KB.CUST_DTL_ADDR IS NOT NULL 
        THEN KB.CUST_ADDR || ' ' || KB.CUST_DTL_ADDR
        ELSE KB.CUST_ADDR
    END AS "거주주소",
    KB.WPLC_NM "직장명",
    CASE 
        WHEN KB.WPLC_DTL_ADDR IS NOT NULL 
        THEN KB.WPLC_ADDR || ' ' || KB.WPLC_DTL_ADDR
        ELSE KB.WPLC_ADDR
    END AS "직장주소",
    JB.JOB_LV_1_NM "직업/업종",
    JB.JOB_LV_2_NM "직업/업종상세"
    
FROM UNIQUE_CANDIDATES UC
INNER JOIN BTCAMLDB_OWN.KYC_CUST_BASE KB ON UC.CUST_ID = KB.CUST_ID
LEFT JOIN BTCAMLDB_OWN.KYC_JOB_BASE JB ON KB.JOB_CD = JB.AML_JOB_CD
LEFT JOIN BTCAMLDB_OWN.DM_SYS_NAT_BASE N1 ON KB.CUST_NTNLT_CD = N1.LEN3_ABBR_NAT_CD
LEFT JOIN BTCAMLDB_OWN.DM_SYS_NAT_BASE N2 ON KB.CUST_LIVE_NAT_CD = N2.LEN3_ABBR_NAT_CD
WHERE :phone_suffix IS NULL 
   OR SUBSTR(AES_DECRYPT(KB.CUST_TEL_NO), -4) = :phone_suffix
ORDER BY KB.CUST_ID
FETCH FIRST 50 ROWS ONLY