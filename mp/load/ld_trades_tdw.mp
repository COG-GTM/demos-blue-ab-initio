<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: ld_trades_tdw.mp
  JIRA: GMRD-237
  Author: GMRD Data Engineering
  Description: Loads enriched trade data into Teradata staging and triggers
               the conformed model load via stored procedure. Uses TPT Stream
               operator for staging insert and BTEQ for procedure call.
  Schedule: After txfm_trade_enrichment.mp completes
  SLA: Must complete within 45 minutes
-->
<graph version="3.6.1">
  <name>ld_trades_tdw</name>
  <description>GMRD - Trade staging and conformed load to Teradata</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="INPUT_DIR" required="true" description="Directory containing enriched trade files"/>
    <parameter name="TD_JDBC_URL" required="true" description="Teradata JDBC connection URL"/>
    <parameter name="TD_USER" required="true"/>
    <parameter name="TD_PASSWORD" required="true"/>
    <parameter name="ARCHIVE_DIR" required="true"/>
    <parameter name="MAX_SESSIONS" default="16"/>
    <parameter name="BATCH_SIZE" default="50000"/>
  </parameters>

  <phase name="stage_load">
    <!-- Read enriched trades -->
    <component name="read_enriched_trades" type="input_file">
      <property name="filename">${INPUT_DIR}/trades_enriched_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/trade_record.dml::trade_record_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Split by source system for parallel loading -->
    <component name="split_by_source" type="partition_by_expression">
      <input port="in0" component="read_enriched_trades" port="out0"/>
      <property name="expressions">
        <expression port="out0">source_system == "CALYPSO"</expression>
        <expression port="out1">source_system == "MUREX"</expression>
        <expression port="out2">source_system == "SUMMIT"</expression>
      </property>
    </component>

    <!-- Load Calypso trades to staging -->
    <component name="load_calypso_stg" type="output_table">
      <input port="in0" component="split_by_source" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_STG_DB.STG_TRADES_CALYPSO</property>
      <property name="load_method">stream</property>
      <property name="max_sessions">${MAX_SESSIONS}</property>
      <property name="batch_size">${BATCH_SIZE}</property>
      <property name="error_limit">100</property>
      <property name="checkpoint_interval">100000</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_STG_DB.STG_TRADES_CALYPSO
        WHERE TRADE_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
      <property name="column_mapping"><![CDATA[
        FEED_ID           = CAST(feed_id AS BIGINT),
        FEED_TIMESTAMP    = CAST(load_timestamp AS TIMESTAMP(6)),
        BATCH_ID          = ${BATCH_ID},
        TRADE_REF         = trade_id,
        TRADE_VERSION     = CAST(trade_version AS INTEGER),
        TRADE_STATUS      = trade_status,
        TRADE_TYPE        = trade_type,
        PRODUCT_TYPE      = product_type,
        PRODUCT_SUBTYPE   = product_subtype,
        BOOK_ID           = book_id,
        DESK_ID           = desk_id,
        TRADER_ID         = trader_id,
        COUNTERPARTY_ID   = counterparty_id,
        COUNTERPARTY_NAME = counterparty_name,
        TRADE_DATE        = CAST(trade_date AS DATE FORMAT 'YYYY-MM-DD'),
        SETTLEMENT_DATE   = CAST(settlement_date AS DATE FORMAT 'YYYY-MM-DD'),
        MATURITY_DATE     = CAST(maturity_date AS DATE FORMAT 'YYYY-MM-DD'),
        NOTIONAL_AMOUNT   = CAST(notional_amount AS DECIMAL(25,6)),
        NOTIONAL_CCY      = notional_ccy,
        SETTLEMENT_AMOUNT = CAST(settlement_amount AS DECIMAL(25,6)),
        SETTLEMENT_CCY    = settlement_ccy,
        TRADE_PRICE       = CAST(trade_price AS DECIMAL(18,8)),
        MARKET_VALUE      = CAST(market_value AS DECIMAL(25,6)),
        MARKET_VALUE_CCY  = market_value_ccy,
        UNDERLYING_ASSET  = underlying_asset_id,
        STRIKE_PRICE      = CAST(strike_price AS DECIMAL(18,8)),
        OPTION_TYPE       = option_type,
        BUY_SELL_IND      = buy_sell_ind,
        LONG_SHORT_IND    = long_short_ind,
        SOURCE_SYSTEM     = source_system
      ]]></property>
    </component>

    <!-- Load Murex trades to staging -->
    <component name="load_murex_stg" type="output_table">
      <input port="in0" component="split_by_source" port="out1"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_STG_DB.STG_TRADES_MUREX</property>
      <property name="load_method">stream</property>
      <property name="max_sessions">${MAX_SESSIONS}</property>
      <property name="batch_size">${BATCH_SIZE}</property>
      <property name="error_limit">100</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_STG_DB.STG_TRADES_MUREX
        WHERE MX_TRADE_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>

    <!-- Load Summit trades to staging -->
    <component name="load_summit_stg" type="output_table">
      <input port="in0" component="split_by_source" port="out2"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_STG_DB.STG_TRADES_SUMMIT</property>
      <property name="load_method">stream</property>
      <property name="max_sessions">${MAX_SESSIONS}</property>
      <property name="batch_size">${BATCH_SIZE}</property>
      <property name="error_limit">100</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_STG_DB.STG_TRADES_SUMMIT
        WHERE SMT_TRADE_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>
  </phase>

  <phase name="conformed_load">
    <!-- Post-load: Call stored procedures via BTEQ -->
    <component name="call_trade_lifecycle" type="run_program">
      <property name="command"><![CDATA[
        bteq <<EOF
        .LOGON ${TD_JDBC_URL}/${TD_USER},${TD_PASSWORD};
        CALL GMRD_CORE_DB.SP_TRADE_LIFECYCLE(
            CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD'),
            ${BATCH_ID},
            'CALYPSO'
        );
        .IF ERRORCODE <> 0 THEN .QUIT 8;
        CALL GMRD_CORE_DB.SP_TRADE_LIFECYCLE(
            CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD'),
            ${BATCH_ID},
            'MUREX'
        );
        .IF ERRORCODE <> 0 THEN .QUIT 8;
        CALL GMRD_CORE_DB.SP_TRADE_LIFECYCLE(
            CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD'),
            ${BATCH_ID},
            'SUMMIT'
        );
        .IF ERRORCODE <> 0 THEN .QUIT 8;
        .LOGOFF;
        .QUIT 0;
        EOF
      ]]></property>
      <property name="success_codes">0</property>
    </component>
  </phase>

  <phase name="archive">
    <!-- Archive source file -->
    <component name="archive_file" type="run_program">
      <property name="command"><![CDATA[
        gzip -c ${INPUT_DIR}/trades_enriched_${BUSINESS_DATE}.dat \
          > ${ARCHIVE_DIR}/trades_enriched_${BUSINESS_DATE}.dat.gz
      ]]></property>
    </component>
  </phase>
</graph>
