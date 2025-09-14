-- str_dashboard/queries/redshift_orderbook.sql
-- 가상자산 거래 원장 조회
-- 파라미터: user_id, start_time, end_time

SELECT 
    user_id,
    market_nm,
    ticker_nm,
    TO_CHAR(trade_date, 'YYYY-MM-DD') AS trade_date,
    TO_CHAR(trade_date, 'HH24:MI:SS') AS trade_time,
    trade_quantity,
    trade_price,
    trade_amount,
    trade_amount_krw,
    trans_from,
    trans_to,
    trans_cat,
    balance_market,
    balance_asset
FROM fms.BDM_VRTL_AST_TRAN_LEDG_FACT 
WHERE trade_date >= %s
    AND trade_date < %s
    AND user_id = %s
ORDER BY trade_date ASC