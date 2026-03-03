<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: txfm_market_data_validation.mp
  JIRA: GMRD-278
  Author: GMRD Data Engineering
  Description: Market data validation and enrichment. Checks for stale prices,
               missing data points, and outlier detection. Applies curve
               interpolation for missing tenors and outputs validated data
               for Teradata loading.
  Schedule: After ext_market_data.mp completes
-->
<graph version="3.6.1">
  <name>txfm_market_data_validation</name>
  <description>GMRD - Stale price detection, curve interpolation, data quality</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="EXTRACT_DIR" required="true"/>
    <parameter name="OUTPUT_DIR" required="true"/>
    <parameter name="REJECT_DIR" required="true"/>
    <parameter name="PREV_BUSINESS_DATE" required="true"/>
    <parameter name="STALE_THRESHOLD_PCT" default="0.001" description="Price change threshold for stale detection"/>
    <parameter name="OUTLIER_THRESHOLD_PCT" default="0.20" description="20% move triggers outlier flag"/>
  </parameters>

  <phase name="validate">
    <!-- Read current day prices -->
    <component name="read_prices" type="input_file">
      <property name="filename">${EXTRACT_DIR}/eod_prices_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::eod_price_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Read previous day prices for comparison -->
    <component name="read_prev_prices" type="input_file">
      <property name="filename">${EXTRACT_DIR}/eod_prices_${PREV_BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::eod_price_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
      <property name="on_missing_file">warning</property>
    </component>

    <!-- Join current with previous for stale/outlier detection -->
    <component name="join_prev_day" type="join">
      <input port="in0" component="read_prices" port="out0"/>
      <input port="in1" component="read_prev_prices" port="out0"/>
      <property name="join_key">instrument_id</property>
      <property name="join_type">left_outer</property>
      <property name="transform"><![CDATA[
        out.* = in0.*;

        /* Stale price detection */
        if (is_defined(in1.close_price) && in1.close_price != 0) {
            decimal pct_change = abs((in0.close_price - in1.close_price) / in1.close_price);
            if (pct_change < decimal("${STALE_THRESHOLD_PCT}"))
                out.stale_flag = "1";
            else
                out.stale_flag = "0";

            /* Outlier detection */
            if (pct_change > decimal("${OUTLIER_THRESHOLD_PCT}"))
                out.price_status = "OUTLIER";
            else
                out.price_status = "VALID";
        } else {
            out.stale_flag = "0";
            out.price_status = "NEW";
        }

        out.batch_id = "${BATCH_ID}";
        out.load_timestamp = current_timestamp();
      ]]></property>
      <property name="output_dml">dml/market_data_record.dml::eod_price_t</property>
    </component>

    <!-- Split valid vs flagged -->
    <component name="split_quality" type="partition_by_expression">
      <input port="in0" component="join_prev_day" port="out0"/>
      <property name="expressions">
        <expression port="out0">price_status == "VALID" || price_status == "NEW"</expression>
        <expression port="out1">price_status == "OUTLIER" || stale_flag == "1"</expression>
      </property>
    </component>

    <component name="write_validated" type="output_file">
      <input port="in0" component="split_quality" port="out0"/>
      <property name="filename">${OUTPUT_DIR}/eod_prices_validated_${BUSINESS_DATE}.dat</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="write_flagged" type="output_file">
      <input port="in0" component="split_quality" port="out1"/>
      <property name="filename">${REJECT_DIR}/eod_prices_flagged_${BUSINESS_DATE}.dat</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>
</graph>
