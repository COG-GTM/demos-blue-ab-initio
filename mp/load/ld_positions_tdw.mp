<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: ld_positions_tdw.mp
  JIRA: GMRD-251
  Author: GMRD Data Engineering
  Description: Loads enriched position data into Teradata staging and triggers
               the position snapshot stored procedure for daily PnL calculation.
  Schedule: After txfm_position_aggregation.mp completes
-->
<graph version="3.6.1">
  <name>ld_positions_tdw</name>
  <description>GMRD - Position staging and snapshot load to Teradata</description>

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

  <phase name="stage_load">
    <component name="read_positions" type="input_file">
      <property name="filename">${INPUT_DIR}/positions_enriched_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/position_record.dml::position_enriched_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Delete existing staging data for this date/batch -->
    <component name="pre_clean" type="run_sql">
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="sql"><![CDATA[
        DELETE FROM GMRD_STG_DB.STG_POSITIONS_RAW
        WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
        AND BATCH_ID = ${BATCH_ID};
      ]]></property>
    </component>

    <component name="load_positions_stg" type="output_table">
      <input port="in0" component="read_positions" port="out0"/>
      <property name="database_type">teradata</property>
      <property name="connection_url">${TD_JDBC_URL}</property>
      <property name="username">${TD_USER}</property>
      <property name="password">${TD_PASSWORD}</property>
      <property name="target_table">GMRD_STG_DB.STG_POSITIONS_RAW</property>
      <property name="load_method">stream</property>
      <property name="max_sessions">${MAX_SESSIONS}</property>
      <property name="error_limit">50</property>
    </component>
  </phase>

  <phase name="snapshot">
    <component name="call_snapshot_proc" type="run_program">
      <property name="command"><![CDATA[
        bteq <<EOF
        .LOGON ${TD_JDBC_URL}/${TD_USER},${TD_PASSWORD};
        CALL GMRD_CORE_DB.SP_DAILY_POSITION_SNAPSHOT(
            CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD'),
            ${BATCH_ID}
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
    <component name="archive_file" type="run_program">
      <property name="command"><![CDATA[
        gzip -c ${INPUT_DIR}/positions_enriched_${BUSINESS_DATE}.dat \
          > ${ARCHIVE_DIR}/positions_enriched_${BUSINESS_DATE}.dat.gz
      ]]></property>
    </component>
  </phase>
</graph>
