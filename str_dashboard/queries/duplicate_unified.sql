-- duplicate_unified.sql
-- 모든 중복 조건을 UNION ALL로 한 번에 처리
-- 바인드 변수명을 명확하게 분리
WITH DUPLICATE_CANDIDATES AS (
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
    WHERE CUST_ID != :current_cust_id_wpn
      AND :workplace_name IS NOT NULL
      AND WPLC_NM IS NOT NULL
      AND WPLC_NM = :workplace_name
    
    UNION ALL
    
    -- 직장주소 매칭
    SELECT CUST_ID, 'WORKPLACE_ADDRESS' AS MATCH_TYPE
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE CUST_ID != :current_cust_id_wpa
      AND :workplace_address IS NOT NULL
      AND WPLC_ADDR IS NOT NULL
      AND WPLC_ADDR = :workplace_address
      AND WPLC_DTL_ADDR = :workplace_detail_address
),
-- 전화번호 뒷자리 매칭 (별도 처리)
PHONE_MATCHED_CUST AS (
    SELECT CUST_ID
    FROM BTCAMLDB_OWN.KYC_CUST_BASE
    WHERE :phone_suffix IS NOT NULL
      AND SUBSTR(AES_DECRYPT(CUST_TEL_NO), -4) = :phone_suffix
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