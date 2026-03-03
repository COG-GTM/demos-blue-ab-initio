<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: txfm_counterparty_hierarchy.mp
  JIRA: GMRD-263
  Author: GMRD Data Engineering
  Description: Counterparty golden source resolution. Takes raw counterparty
               data from all source systems, resolves duplicates using LEI
               as the primary key, builds the corporate hierarchy, and outputs
               the conformed counterparty master for SCD Type 2 loading.
  Schedule: Daily, after ext_counterparty.mp completes
-->
<graph version="3.6.1">
  <name>txfm_counterparty_hierarchy</name>
  <description>GMRD - Counterparty golden source resolution and hierarchy build</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="EXTRACT_DIR" required="true"/>
    <parameter name="OUTPUT_DIR" required="true"/>
    <parameter name="REJECT_DIR" required="true"/>
    <parameter name="HIERARCHY_FILE" required="true" description="Corporate hierarchy reference file"/>
  </parameters>

  <phase name="resolve">
    <component name="read_raw_cpty" type="input_file">
      <property name="filename">${EXTRACT_DIR}/counterparty_all_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/counterparty_record.dml::counterparty_raw_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="read_hierarchy" type="input_file">
      <property name="filename">${HIERARCHY_FILE}</property>
      <property name="input_dml">dml/counterparty_record.dml::counterparty_hierarchy_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Deduplicate by LEI (golden key) with source priority: Calypso > Murex > Summit -->
    <component name="dedup_by_lei" type="rollup">
      <input port="in0" component="read_raw_cpty" port="out0"/>
      <property name="group_by">cpty_lei</property>
      <property name="sort_by">source_system ASC</property>
      <property name="transform"><![CDATA[
        /* Priority: CALYPSO (1st alphabetically) > MUREX > SUMMIT */
        out.* = first(in.*);
        /* If LEI is missing, use source_id as fallback key */
        if (is_blank(out.cpty_lei))
            out.cpty_lei = first(in.cpty_source_id) :: "_" :: first(in.source_system);

        /* Merge ratings - take best available from any source */
        if (is_blank(out.cpty_rating_sp))
            out.cpty_rating_sp = max_defined(in.cpty_rating_sp);
        if (is_blank(out.cpty_rating_moodys))
            out.cpty_rating_moodys = max_defined(in.cpty_rating_moodys);
        if (is_blank(out.cpty_rating_fitch))
            out.cpty_rating_fitch = max_defined(in.cpty_rating_fitch);
      ]]></property>
      <property name="output_dml">dml/counterparty_record.dml::counterparty_raw_t</property>
    </component>

    <!-- Map to conformed model -->
    <component name="conform_cpty" type="reformat">
      <input port="in0" component="dedup_by_lei" port="out0"/>
      <property name="transform"><![CDATA[
        out.counterparty_id   = in.cpty_source_id;
        out.legal_name        = in.cpty_legal_name;
        out.short_name        = in.cpty_short_name;
        out.counterparty_type = in.cpty_type;
        out.lei_code          = in.cpty_lei;
        out.country_code      = in.cpty_country;
        out.sector            = in.cpty_sector;
        out.industry_group    = in.cpty_industry;
        out.rating_sp         = in.cpty_rating_sp;
        out.rating_moodys     = in.cpty_rating_moodys;
        out.rating_fitch      = in.cpty_rating_fitch;
        out.internal_rating   = in.cpty_internal_rating;
        out.netting_eligible  = in.cpty_netting_agreement != "" ? "1" : "0";
        out.csa_flag          = in.cpty_csa_flag;
        out.ccp_member_flag   = in.cpty_ccp_flag;
        out.status            = "ACTIVE";
        out.valid_from_date   = "${BUSINESS_DATE}";
        out.valid_to_date     = "9999-12-31";
        out.is_current        = "1";
        out.batch_id          = "${BATCH_ID}";
        out.load_timestamp    = current_timestamp();

        /* Derive region from country */
        if (in.cpty_country in ["GB","DE","FR","IT","ES","NL","BE","CH","IE","LU","AT","SE","NO","DK","FI","PT"])
            out.region = "EMEA";
        else if (in.cpty_country in ["US","CA","MX","BR","AR","CL","CO"])
            out.region = "AMERICAS";
        else if (in.cpty_country in ["JP","CN","HK","SG","AU","NZ","KR","IN","TW","TH","MY","ID","PH"])
            out.region = "APAC";
        else
            out.region = "OTHER";

        /* Derive regulatory class */
        if (in.cpty_type == "SOVEREIGN" || in.cpty_type == "CENTRAL_BANK")
            out.regulatory_class = "SOVEREIGN";
        else if (in.cpty_type == "BANK" || in.cpty_type == "INSTITUTION")
            out.regulatory_class = "INSTITUTION";
        else if (in.cpty_type == "CORPORATE")
            out.regulatory_class = "CORPORATE";
        else
            out.regulatory_class = "RETAIL";

        out.basel_asset_class = out.regulatory_class;
      ]]></property>
      <property name="output_dml">dml/counterparty_record.dml::counterparty_conformed_t</property>
    </component>

    <!-- Join hierarchy data -->
    <component name="join_hierarchy" type="lookup">
      <input port="in0" component="conform_cpty" port="out0"/>
      <input port="lookup" component="read_hierarchy" port="out0"/>
      <property name="lookup_key">counterparty_id = child_cpty_id</property>
      <property name="transform"><![CDATA[
        out.* = in.*;
        if (is_defined(lookup.parent_cpty_id)) {
            out.parent_cpty_id = lookup.parent_cpty_id;
            out.ultimate_parent_id = lookup.ultimate_parent_id;
        }
      ]]></property>
    </component>

    <component name="write_conformed" type="output_file">
      <input port="in0" component="join_hierarchy" port="out0"/>
      <property name="filename">${OUTPUT_DIR}/counterparty_conformed_${BUSINESS_DATE}.dat</property>
      <property name="output_dml">dml/counterparty_record.dml::counterparty_conformed_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>
</graph>
