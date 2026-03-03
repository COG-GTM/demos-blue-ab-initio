<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: txfm_position_aggregation.mp
  JIRA: GMRD-250
  Author: GMRD Data Engineering
  Description: Position aggregation and enrichment. Takes normalised positions
               from multi-source extraction, applies FX conversion, aggregates
               to book/desk level, and outputs for Teradata loading.
  Schedule: Triggered after ext_positions_multi.mp completes
-->
<graph version="3.6.1">
  <name>txfm_position_aggregation</name>
  <description>GMRD - Position aggregation, FX conversion, Greek summation</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="EXTRACT_DIR" required="true"/>
    <parameter name="OUTPUT_DIR" required="true"/>
    <parameter name="LOOKUP_DIR" required="true"/>
    <parameter name="REJECT_DIR" required="true"/>
  </parameters>

  <phase name="read_and_enrich">
    <component name="read_positions" type="input_file">
      <property name="filename">${EXTRACT_DIR}/positions_all_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/position_record.dml::position_raw_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="read_fx_rates" type="input_file">
      <property name="filename">${LOOKUP_DIR}/fx_rates_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::fx_rate_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <component name="read_eod_prices" type="input_file">
      <property name="filename">${LOOKUP_DIR}/eod_prices_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::eod_price_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Derive asset class from product type -->
    <component name="derive_asset_class" type="reformat">
      <input port="in0" component="read_positions" port="out0"/>
      <property name="transform"><![CDATA[
        out.* = in.*;
        if (in.product_type in ["IRS","FRA","SWAP","SWAPTION","CAP","FLOOR"])
            out.asset_class = "IR";
        else if (in.product_type in ["FX_SPOT","FX_FWD","FX_SWAP","FX_OPTION","NDF"])
            out.asset_class = "FX";
        else if (in.product_type in ["EQUITY","EQUITY_SWAP","EQUITY_OPTION","CFD","ETF"])
            out.asset_class = "EQ";
        else if (in.product_type in ["CDS","CDO","CLN","TRS","BOND"])
            out.asset_class = "CR";
        else if (in.product_type in ["COMMODITY","COMMODITY_SWAP","COMMODITY_OPTION"])
            out.asset_class = "CO";
        else
            out.asset_class = "OTHER";
      ]]></property>
      <property name="output_dml">dml/position_record.dml::position_enriched_t</property>
    </component>

    <!-- FX conversion -->
    <component name="fx_convert" type="lookup">
      <input port="in0" component="derive_asset_class" port="out0"/>
      <input port="lookup" component="read_fx_rates" port="out0"/>
      <property name="lookup_key">instrument_ccy = base_ccy</property>
      <property name="transform"><![CDATA[
        out.* = in.*;
        if (in.instrument_ccy == "GBP") {
            out.fx_rate_to_gbp = 1.0;
            out.market_value_gbp = in.market_value_local;
            out.unrealised_pnl_gbp = in.unrealised_pnl;
        } else if (is_defined(lookup.spot_rate)) {
            out.fx_rate_to_gbp = lookup.spot_rate;
            out.market_value_gbp = in.market_value_local * lookup.spot_rate;
            out.unrealised_pnl_gbp = in.unrealised_pnl * lookup.spot_rate;
        } else {
            out.fx_rate_to_gbp = 1.0;
            out.market_value_gbp = in.market_value_local;
            out.unrealised_pnl_gbp = in.unrealised_pnl;
        }
        out.batch_id = "${BATCH_ID}";
        out.load_timestamp = current_timestamp();
      ]]></property>
      <property name="output_dml">dml/position_record.dml::position_enriched_t</property>
    </component>

    <!-- Price lookup for current price -->
    <component name="price_lookup" type="lookup">
      <input port="in0" component="fx_convert" port="out0"/>
      <input port="lookup" component="read_eod_prices" port="out0"/>
      <property name="lookup_key">instrument_id = instrument_id</property>
      <property name="transform"><![CDATA[
        out.* = in.*;
        if (is_defined(lookup.close_price))
            out.current_price = lookup.close_price;
      ]]></property>
      <property name="output_dml">dml/position_record.dml::position_enriched_t</property>
    </component>
  </phase>

  <phase name="aggregate_and_output">
    <!-- Aggregate to desk level -->
    <component name="aggregate_desk" type="rollup">
      <input port="in0" component="price_lookup" port="out0"/>
      <property name="group_by">book_id, desk_id, entity_code, asset_class, business_date</property>
      <property name="transform"><![CDATA[
        out.book_id = in.book_id;
        out.desk_id = in.desk_id;
        out.entity_code = in.entity_code;
        out.asset_class = in.asset_class;
        out.business_date = in.business_date;
        out.total_market_value_gbp = sum(in.market_value_gbp);
        out.total_unrealised_pnl_gbp = sum(in.unrealised_pnl_gbp);
        out.total_delta = sum(in.delta);
        out.total_gamma = sum(in.gamma);
        out.total_vega = sum(in.vega);
        out.total_dv01 = sum(in.dv01);
        out.position_count = count();
        out.instrument_count = count_distinct(in.instrument_id);
        out.batch_id = "${BATCH_ID}";
      ]]></property>
      <property name="output_dml">dml/position_record.dml::position_aggregated_t</property>
    </component>

    <!-- Write enriched positions (detail) -->
    <component name="write_detail" type="output_file">
      <input port="in0" component="price_lookup" port="out0"/>
      <property name="filename">${OUTPUT_DIR}/positions_enriched_${BUSINESS_DATE}.dat</property>
      <property name="output_dml">dml/position_record.dml::position_enriched_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Write aggregated positions -->
    <component name="write_aggregated" type="output_file">
      <input port="in0" component="aggregate_desk" port="out0"/>
      <property name="filename">${OUTPUT_DIR}/positions_aggregated_${BUSINESS_DATE}.dat</property>
      <property name="output_dml">dml/position_record.dml::position_aggregated_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>
</graph>
