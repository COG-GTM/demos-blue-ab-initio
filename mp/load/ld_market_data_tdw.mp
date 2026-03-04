<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: ld_market_data_tdw.mp
  JIRA: GMRD-279
  Author: GMRD Data Engineering
  Description: Loads validated market data (prices, curves, FX rates,
               volatility surfaces) into Teradata GMRD_CORE_DB market
               data tables. Includes pre-load validation and post-load
               reconciliation.
  Schedule: After txfm_market_data_validation.mp completes
-->
<graph version="3.6.1">
  <name>ld_market_data_tdw</name>
  <description>GMRD - Market data load to Teradata with validation</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="INPUT_DIR" required="true"/>
    <parameter name="TD_JDBC_URL" required="true"/>
    <parameter name="TD_USER" required="true"/>
    <parameter name="TD_PASSWORD" required="true"/>
    <parameter name="ARCHIVE_DIR" required="true"/>
    <parameter name="MAX_SESSIONS" default="8"/>
  </parameters>

  <phase name="load_prices">
    <component name="read_prices" type="input_file">
      <property name="filename">${INPUT_DIR}/eod_prices_validated_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::eod_price_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="load_eod_prices" type="output_table">
      <input port="in0" component="read_prices" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_CORE_DB.MKT_EOD_PRICES</property>
      <property name="load_method">stream</property>
      <property name="max_sessions">${MAX_SESSIONS}</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_CORE_DB.MKT_EOD_PRICES
        WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>
  </phase>

  <phase name="load_rates_and_curves">
    <component name="read_fx_rates" type="input_file">
      <property name="filename">${INPUT_DIR}/fx_rates_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::fx_rate_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="load_fx_rates" type="output_table">
      <input port="in0" component="read_fx_rates" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_CORE_DB.MKT_FX_RATES</property>
      <property name="load_method">stream</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_CORE_DB.MKT_FX_RATES
        WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>

    <component name="read_curves" type="input_file">
      <property name="filename">${INPUT_DIR}/yield_curves_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::yield_curve_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="load_yield_curves" type="output_table">
      <input port="in0" component="read_curves" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_CORE_DB.MKT_YIELD_CURVES</property>
      <property name="load_method">stream</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_CORE_DB.MKT_YIELD_CURVES
        WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>

    <component name="read_vols" type="input_file">
      <property name="filename">${INPUT_DIR}/vol_surfaces_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::vol_surface_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="load_vol_surfaces" type="output_table">
      <input port="in0" component="read_vols" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_CORE_DB.MKT_VOL_SURFACES</property>
      <property name="load_method">stream</property>
      <property name="pre_sql"><![CDATA[
        DELETE FROM GMRD_CORE_DB.MKT_VOL_SURFACES
        WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>
  </phase>

  <phase name="post_load_recon">
    <component name="recon_counts" type="run_sql">
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="sql"><![CDATA[
        EXEC GMRD_CORE_DB.MCR_LOAD_AUDIT(
            ${BATCH_ID},
            'MKT_EOD_PRICES',
            'GMRD_CORE_DB',
            CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD'),
            'COMPLETED',
            (SELECT COUNT(*) FROM GMRD_CORE_DB.MKT_EOD_PRICES
             WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
             AND BATCH_ID = ${BATCH_ID}),
            'Market data load completed'
        );
      ]]></property>
    </component>
  </phase>
</graph>
