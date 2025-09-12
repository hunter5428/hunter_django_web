-- ALERT_ID로 얻은 CUST_ID를 기준으로 고객 기본정보 조회 (확장 버전)
-- 바인드 변수 :custId 1개
select
  -- 기존 항목들
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='CUST_TYPE_CD' and AML_DTL_CD=c1_0.CUST_TYPE_CD) as "고객구분",
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='CUST_RKGD_CD' and AML_DTL_CD=c2_0.cust_rkgd_cd) as "RA등급",
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='RLNM_CERT_CD' and AML_DTL_CD=c1_0.rlnm_cert_cd) as "실명번호구분",
  s1_0.nat_ko_nm                          as "국적",
  nvl(c1_0.cust_bday, c1_0.corp_estb_dt)  as "생년월일/설립입",
  AES_DECRYPT(CUST_TEL_NO)                 as "연락처",
  
  -- 추가 항목: 성별 (개인 고객일 경우만 유효)
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='CUST_GNDR_CD' and AML_DTL_CD=c1_0.cust_gndr_cd) as "성별",
  
  -- 추가 항목: 거주지 정보
  (c1_0.cust_addr || ' ' || c1_0.cust_dtl_addr) as "거주지 정보",
  
  -- 기존 직업/업종 (개선된 버전)
  decode(
    c1_0.CUST_TYPE_CD,
    '01',
      (case
         when j1_0.job_lv_2_nm is null then j1_0.job_lv_1_nm
         else j1_0.job_lv_1_nm || ' (' || j1_0.job_lv_2_nm || ')'
       end),
    c1_0.bztyp_dtl_ctnt
  )                                        as "직업/업종",
  
  -- 추가 항목: 직장명
  c1_0.wplc_nm as "직장명",
  
  -- 추가 항목: 직장주소
  (c1_0.wplc_addr || ' ' || c1_0.wplc_dtl_addr) as "직장주소",
  
  -- 추가 항목: 자금의 원천
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='PERS_PRMY_INCM_SURC_CD' and AML_DTL_CD=c1_0.prmy_incm_surc_cd) as "자금의 원천",
  
  -- 거래목적 (기존)
  (select AML_DTL_CD_NM from SM_CD_DTL where AML_COMN_CD='PERS_TRAN_PUPS_CD' and AML_DTL_CD=c1_0.tran_pups_cd) as "거래목적",
  
  -- 추가 항목: 위험등급
  case
    when c1_0.AML_RSLT_CD = '200' then 'WLF 검토대기'
    when c1_0.AML_RSLT_CD = '400' then '거래거절'
    when c1_0.AML_RSLT_CD = '600' then 'CDD'
    when c1_0.AML_RSLT_CD = '700' then 'EDD'
    when c1_0.AML_RSLT_CD = '800' then 'EEDD'
  end as "위험등급",
  
  -- 기존 항목들 계속
  c1_0.clb_yn                              as "고액자산가",
  (select count(r1_0.str_rpt_mngt_no) from btcamldb_own.str_rpt_base r1_0 where r1_0.cust_id=c1_0.cust_id and r1_0.xml_cret_file_nm is not null) as "STR보고건수",
  (select to_char(max(r2_0.str_rpt_dtm),'YYYYMMDD') from btcamldb_own.str_rpt_base r2_0 where r2_0.cust_id=c1_0.cust_id and r2_0.xml_cret_file_nm is not null) as "최종STR보고일",
  (select count(*) from btcamldb_own.str_alert_list a1_0 where a1_0.cust_id=c1_0.cust_id) as "Alert 건수"
from btcamldb_own.kyc_cust_base c1_0
left join btcamldb_own.dm_sys_nat_base s1_0 on s1_0.len3_abbr_nat_cd=c1_0.cust_ntnlt_cd
left join btcamldb_own.ra_cust_rkat_grd_list c2_0 on c2_0.cust_id=c1_0.cust_id
  and c2_0.rkat_dtm=(select max(c3_0.rkat_dtm) from btcamldb_own.ra_cust_rkat_grd_list c3_0 where c3_0.cust_id=c1_0.cust_id)
left join btcamldb_own.kyc_job_base j1_0 on j1_0.aml_job_cd=c1_0.job_cd
left join btcamldb_own.kyc_bztyp_base b1_0 on b1_0.aml_bztyp_type_cd=c1_0.bztyp_type_cd
where c1_0.cust_id = :custId;