<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: ld_counterparty_tdw.mp
  JIRA: GMRD-264
  Author: GMRD Data Engineering
  Description: Loads conformed counterparty data into Teradata with SCD Type 2
               merge logic. Compares incoming counterparty records with existing
               current records to detect changes, closes old versions, and
               inserts new versions.
  Schedule: Daily, after txfm_counterparty_hierarchy.mp completes
-->
<graph version="3.6.1">
  <name>ld_counterparty_tdw</name>
  <description>GMRD - Counterparty SCD Type 2 load to Teradata</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="INPUT_DIR" required="true"/>
    <parameter name="TD_JDBC_URL" required="true"/>
    <parameter name="TD_USER" required="true"/>
    <parameter name="TD_PASSWORD" required="true"/>
    <parameter name="ARCHIVE_DIR" required="true"/>
  </parameters>

  <phase name="detect_changes">
    <!-- Read incoming conformed counterparties -->
    <component name="read_incoming" type="input_file">
      <property name="filename">${INPUT_DIR}/counterparty_conformed_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/counterparty_record.dml::counterparty_conformed_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Read current counterparties from Teradata -->
    <component name="read_current" type="input_table">
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="query"><![CDATA[
        SELECT
            COUNTERPARTY_ID, LEGAL_NAME, SHORT_NAME, COUNTERPARTY_TYPE,
            LEI_CODE, COUNTRY_CODE, REGION, SECTOR, INDUSTRY_GROUP,
            RATING_SP, RATING_MOODYS, RATING_FITCH, INTERNAL_RATING,
            PD_1Y, LGD_PCT, NETTING_ELIGIBLE, CSA_FLAG, CCP_MEMBER_FLAG,
            REGULATORY_CLASS, BASEL_ASSET_CLASS, STATUS
        FROM GMRD_CORE_DB.CTR_COUNTERPARTIES
        WHERE IS_CURRENT = 1
      ]]></property>
      <property name="output_dml">dml/counterparty_record.dml::counterparty_conformed_t</property>
    </component>

    <!-- Join incoming vs current to detect changes -->
    <component name="detect_scd_changes" type="join">
      <input port="in0" component="read_incoming" port="out0"/>
      <input port="in1" component="read_current" port="out0"/>
      <property name="join_key">counterparty_id</property>
      <property name="join_type">full_outer</property>
      <property name="transform"><![CDATA[
        if (!is_defined(in1.counterparty_id)) {
            /* New counterparty - insert */
            out.* = in0.*;
            out.change_type = "INSERT";
        } else if (!is_defined(in0.counterparty_id)) {
            /* Counterparty removed from source - flag for review */
            out.* = in1.*;
            out.change_type = "MISSING";
        } else {
            /* Existing - check for changes */
            out.* = in0.*;
            if (in0.legal_name != in1.legal_name
                || in0.rating_sp != in1.rating_sp
                || in0.rating_moodys != in1.rating_moodys
                || in0.internal_rating != in1.internal_rating
                || in0.country_code != in1.country_code
                || in0.csa_flag != in1.csa_flag
                || in0.ccp_member_flag != in1.ccp_member_flag
                || in0.regulatory_class != in1.regulatory_class)
            {
                out.change_type = "UPDATE";
            } else {
                out.change_type = "UNCHANGED";
            }
        }
      ]]></property>
    </component>

    <!-- Split by change type -->
    <component name="split_changes" type="partition_by_expression">
      <input port="in0" component="detect_scd_changes" port="out0"/>
      <property name="expressions">
        <expression port="out0">change_type == "INSERT" || change_type == "UPDATE"</expression>
        <expression port="out1">change_type == "UNCHANGED"</expression>
        <expression port="out2">change_type == "MISSING"</expression>
      </property>
    </component>
  </phase>

  <phase name="apply_scd">
    <!-- Close existing versions for updated records -->
    <component name="close_old_versions" type="run_sql">
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="sql"><![CDATA[
        UPDATE GMRD_CORE_DB.CTR_COUNTERPARTIES
        SET IS_CURRENT = 0,
            VALID_TO_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD') - INTERVAL '1' DAY,
            UPDATED_TS = CURRENT_TIMESTAMP(6)
        WHERE IS_CURRENT = 1
        AND COUNTERPARTY_ID IN (
            SELECT COUNTERPARTY_ID
            FROM GMRD_STG_DB.STG_COUNTERPARTY_RAW
            WHERE BATCH_ID = ${BATCH_ID}
        );
      ]]></property>
    </component>

    <!-- Insert new/updated versions -->
    <component name="insert_new_versions" type="output_table">
      <input port="in0" component="split_changes" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_CORE_DB.CTR_COUNTERPARTIES</property>
      <property name="load_method">stream</property>
    </component>

    <!-- Log missing counterparties for investigation -->
    <component name="write_missing_log" type="output_file">
      <input port="in0" component="split_changes" port="out2"/>
      <property name="filename">${ARCHIVE_DIR}/counterparty_missing_${BUSINESS_DATE}.dat</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>
</graph>
