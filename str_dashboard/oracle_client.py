# HUNTER_APP_DJANGO/str_dashboard/oracle_client.py
"""
jaydebeapi + JPype1 기반 오라클 JDBC 연결 유틸.
- 폐쇄망 환경에서 JAR 경로/계정은 환경변수로 주입 권장.
- settings.py의 Django DB와는 별개로, 조회 용도로만 사용합니다.
"""
import os
import logging
import jaydebeapi

logger = logging.getLogger(__name__)

# 환경변수 예시 (Windows PowerShell/명령프롬프트):
# set ORACLE_JDBC_URL=jdbc:oracle:thin:@//127.0.0.1:40112/PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM
# set ORACLE_USERNAME=LIMJUNHYEOK
# set ORACLE_PASSWORD=********
# set ORACLE_JAR=C:\ojdbc11-21.5.0.0.jar
# set ORACLE_DRIVER=oracle.jdbc.driver.OracleDriver

JDBC_URL = os.getenv('ORACLE_JDBC_URL', '')
USERNAME = os.getenv('ORACLE_USERNAME', '')
PASSWORD = os.getenv('ORACLE_PASSWORD', '')
DRIVER_PATH = os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')
DRIVER_CLASS = os.getenv('ORACLE_DRIVER', 'oracle.jdbc.driver.OracleDriver')

def get_connection():
    """
    jaydebeapi.connect 래퍼. 연결 성공/실패 로그만 남기고 커넥션을 반환.
    JPype JVM은 jaydebeapi 내부에서 관리되므로 보통 별도 JVM 경로 지정은 불필요합니다.
    """
    try:
        conn = jaydebeapi.connect(
            DRIVER_CLASS,
            JDBC_URL,
            [USERNAME, PASSWORD],
            DRIVER_PATH
        )
        logger.debug("Oracle JDBC connected.")
        return conn
    except Exception as e:
        logger.exception(f"Oracle JDBC connection failed: {e}")
        raise

def fetchall(sql: str, params=None):
    """
    간단 조회 예시. (DAO/Service 레이어에서 호출)
    with 문으로 커넥션 안전하게 닫아주세요.
    """
    params = params or []
    conn = get_connection()
    try:
        curs = conn.cursor()
        curs.execute(sql, params)
        rows = curs.fetchall()
        cols = [d[0] for d in curs.description] if curs.description else []
        return cols, rows
    finally:
        try:
            conn.close()
        except Exception:
            pass
