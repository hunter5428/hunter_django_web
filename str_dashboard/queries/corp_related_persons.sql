-- 법인의 관련인 정보 조회
-- 바인드 변수: :cust_id
SELECT 
    (SELECT AML_DTL_CD_NM
     FROM SM_CD_DTL
     WHERE AML_COMN_CD = 'AML_RELPR_TYPE_CD'
       AND AML_DTL_CD = c1_0.relpr_type_cd) "관계",
    n1_0.relpr_ko_nm "관계인명",
    n1_0.relpr_en_nm "관계인 영문명",
    ((n1_0.relpr_addr || ' ') || n1_0.relpr_dtl_addr) "관계인 거주지정보",
    CASE
        WHEN (j1_0.job_lv_2_id IS NOT NULL) THEN (((j1_0.job_lv_1_nm || '(') || j1_0.job_lv_2_nm) || ')')
        ELSE j1_0.job_lv_1_nm 
    END AS "관계인 직업",
    ((n1_0.relpr_wplc_addr || ' ') || n1_0.relpr_dtl_addr) "관계인 직장주소",
    n1_0.relpr_bday "관계인 생년월일",
    n1_0.relpr_gndr_cd "관계인 성별",
    AES_DECRYPT(n1_0.RLNM_CERT_VAL) "관계인 실명번호",
    c1_0.real_ownr_stke_rate "관계인지분율"
FROM btcamldb_own.kyc_corp_relpr_base c1_0
JOIN btcamldb_own.kyc_ncus_relpr_base n1_0
    ON c1_0.cust_id = n1_0.cust_id
    AND c1_0.relpr_seq = n1_0.relpr_seq
LEFT JOIN btcamldb_own.kyc_job_base j1_0 
    ON n1_0.relpr_job_cd = j1_0.aml_job_cd
WHERE c1_0.cust_id = :cust_id
ORDER BY c1_0.relpr_seq;