<?xml version="1.0" encoding="UTF-8"?>
<!--
  Graph: txfm_trade_enrichment.mp
  JIRA: GMRD-236
  Author: GMRD Data Engineering
  Description: Trade normalisation and enrichment graph. Takes raw extracts
               from Calypso, Murex, and Summit, maps them to the conformed
               trade model, enriches with counterparty lookup and FX conversion.
  Schedule: Triggered after all source extractions complete
  SLA: Must complete within 30 minutes
-->
<graph version="3.6.1">
  <name>txfm_trade_enrichment</name>
  <description>GMRD - Trade normalisation, counterparty lookup, FX conversion</description>

  <parameters>
    <parameter name="BUSINESS_DATE" required="true"/>
    <parameter name="BATCH_ID" required="true"/>
    <parameter name="EXTRACT_DIR" required="true" description="Directory containing source extract files"/>
    <parameter name="OUTPUT_DIR" required="true" description="Output directory for enriched trade files"/>
    <parameter name="REJECT_DIR" required="true"/>
    <parameter name="LOOKUP_DIR" required="true" description="Directory for lookup reference files"/>
  </parameters>

  <phase name="read_sources">
    <!-- Read Calypso extract -->
    <component name="read_calypso" type="input_file">
      <property name="filename">${EXTRACT_DIR}/calypso_trades_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/trade_record.dml::trade_calypso_raw_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Read Murex extract -->
    <component name="read_murex" type="input_file">
      <property name="filename">${EXTRACT_DIR}/murex_trades_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/trade_record.dml::trade_murex_raw_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Read Summit extract -->
    <component name="read_summit" type="input_file">
      <property name="filename">${EXTRACT_DIR}/summit_trades_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/trade_record.dml::trade_summit_raw_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- FX rates lookup -->
    <component name="read_fx_rates" type="input_file">
      <property name="filename">${LOOKUP_DIR}/fx_rates_${BUSINESS_DATE}.dat</property>
      <property name="input_dml">dml/market_data_record.dml::fx_rate_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>

    <!-- Counterparty lookup -->
    <component name="read_cpty_lookup" type="input_file">
      <property name="filename">${LOOKUP_DIR}/counterparty_master.dat</property>
      <property name="input_dml">dml/counterparty_record.dml::counterparty_conformed_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>

  <phase name="normalise">
    <!-- Normalise Calypso to common model -->
    <component name="normalise_calypso" type="reformat">
      <input port="in0" component="read_calypso" port="out0"/>
      <property name="transform"><![CDATA[
        out.trade_id          = in.trade_ref;
        out.trade_version     = in.trade_version;
        out.source_system     = "CALYPSO";
        out.source_trade_ref  = in.trade_ref;
        out.business_date     = "${BUSINESS_DATE}";
        out.trade_status      = in.trade_status;
        out.trade_type        = in.trade_type;
        out.product_type      = in.product_type;
        out.product_subtype   = in.product_subtype;
        out.book_id           = in.book_id;
        out.desk_id           = in.desk_id;
        out.trader_id         = in.trader_id;
        out.counterparty_id   = in.counterparty_id;
        out.counterparty_name = in.counterparty_name;
        out.trade_date        = in.trade_date;
        out.settlement_date   = in.settlement_date;
        out.maturity_date     = in.maturity_date;
        out.notional_amount   = decimal_strip(in.notional_amount);
        out.notional_ccy      = in.notional_ccy;
        out.settlement_amount = decimal_strip(in.settlement_amount);
        out.settlement_ccy    = in.settlement_ccy;
        out.trade_price       = decimal_strip(in.trade_price);
        out.market_value      = decimal_strip(in.market_value);
        out.market_value_ccy  = in.market_value_ccy;
        out.underlying_asset_id = in.underlying_asset;
        out.strike_price      = decimal_strip(in.strike_price);
        out.option_type       = in.option_type;
        out.buy_sell_ind      = in.buy_sell_ind;
        out.long_short_ind    = in.long_short_ind;
        out.batch_id          = "${BATCH_ID}";
        out.load_timestamp    = current_timestamp();

        /* Derive asset class from product type */
        if (in.product_type in ["IRS","FRA","SWAP","SWAPTION","CAP","FLOOR"])
            out.asset_class = "IR";
        else if (in.product_type in ["FX_SPOT","FX_FWD","FX_SWAP","FX_OPTION"])
            out.asset_class = "FX";
        else if (in.product_type in ["EQUITY_SWAP","EQUITY_OPTION","CFD"])
            out.asset_class = "EQ";
        else if (in.product_type in ["CDS","CDO","CLN","TRS"])
            out.asset_class = "CR";
        else if (in.product_type in ["COMMODITY_SWAP","COMMODITY_OPTION"])
            out.asset_class = "CO";
        else
            out.asset_class = "OTHER";

        /* Derive entity from book prefix */
        out.entity_code = string_substring(in.book_id, 1, 4);
      ]]></property>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
    </component>

    <!-- Normalise Murex to common model -->
    <component name="normalise_murex" type="reformat">
      <input port="in0" component="read_murex" port="out0"/>
      <property name="transform"><![CDATA[
        out.trade_id          = in.mx_trade_id;
        out.trade_version     = in.mx_version;
        out.source_system     = "MUREX";
        out.source_trade_ref  = in.mx_trade_id;
        out.business_date     = "${BUSINESS_DATE}";
        out.trade_status      = in.mx_status;
        out.trade_type        = in.mx_trade_type;
        out.product_type      = in.mx_product_type;
        out.product_subtype   = in.mx_product_family;
        out.book_id           = in.mx_book;
        out.desk_id           = in.mx_desk;
        out.trader_id         = in.mx_trader;
        out.counterparty_id   = in.mx_cpty_code;
        out.counterparty_name = in.mx_cpty_long_name;
        out.trade_date        = in.mx_trade_date;
        out.settlement_date   = in.mx_value_date;
        out.maturity_date     = in.mx_maturity_date;
        out.notional_amount   = decimal_strip(in.mx_nominal);
        out.notional_ccy      = in.mx_nominal_ccy;
        out.settlement_amount = decimal_strip(in.mx_settle_amount);
        out.settlement_ccy    = in.mx_settle_ccy;
        out.trade_price       = decimal_strip(in.mx_price);
        out.market_value      = decimal_strip(in.mx_mkt_value);
        out.market_value_ccy  = in.mx_mkt_value_ccy;
        out.underlying_asset_id = in.mx_underlying;
        out.strike_price      = decimal_strip(in.mx_strike);
        out.buy_sell_ind      = in.mx_buy_sell;
        out.batch_id          = "${BATCH_ID}";
        out.load_timestamp    = current_timestamp();
        out.entity_code       = string_substring(in.mx_book, 1, 4);
      ]]></property>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
    </component>

    <!-- Normalise Summit to common model -->
    <component name="normalise_summit" type="reformat">
      <input port="in0" component="read_summit" port="out0"/>
      <property name="transform"><![CDATA[
        out.trade_id          = in.smt_deal_no;
        out.trade_version     = in.smt_version_no;
        out.source_system     = "SUMMIT";
        out.source_trade_ref  = in.smt_deal_no;
        out.business_date     = "${BUSINESS_DATE}";
        out.trade_status      = in.smt_deal_status;
        out.trade_type        = in.smt_deal_type;
        out.product_type      = in.smt_product_group;
        out.product_subtype   = in.smt_product_class;
        out.book_id           = in.smt_book;
        out.desk_id           = in.smt_desk;
        out.entity_code       = in.smt_entity;
        out.counterparty_id   = in.smt_cpty_id;
        out.counterparty_name = in.smt_cpty_name;
        out.trade_date        = in.smt_trade_date;
        out.settlement_date   = in.smt_settle_date;
        out.maturity_date     = in.smt_maturity_date;
        out.notional_amount   = decimal_strip(in.smt_notional);
        out.notional_ccy      = in.smt_notional_ccy;
        out.settlement_amount = decimal_strip(in.smt_principal);
        out.settlement_ccy    = in.smt_principal_ccy;
        out.trade_price       = decimal_strip(in.smt_rate);
        out.market_value      = decimal_strip(in.smt_mkt_value);
        out.market_value_ccy  = in.smt_mkt_value_ccy;
        out.underlying_asset_id = in.smt_underlying_id;
        out.strike_price      = decimal_strip(in.smt_strike_rate);
        out.buy_sell_ind      = in.smt_direction;
        out.batch_id          = "${BATCH_ID}";
        out.load_timestamp    = current_timestamp();
      ]]></property>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
    </component>
  </phase>

  <phase name="enrich">
    <!-- Merge normalised trades -->
    <component name="merge_trades" type="gather">
      <input port="in0" component="normalise_calypso" port="out0"/>
      <input port="in1" component="normalise_murex" port="out0"/>
      <input port="in2" component="normalise_summit" port="out0"/>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
    </component>

    <!-- FX conversion lookup -->
    <component name="fx_lookup" type="lookup">
      <input port="in0" component="merge_trades" port="out0"/>
      <input port="lookup" component="read_fx_rates" port="out0"/>
      <property name="lookup_key">notional_ccy = base_ccy</property>
      <property name="transform"><![CDATA[
        out.* = in.*;
        if (in.notional_ccy == "GBP") {
            out.notional_amount_gbp = in.notional_amount;
            out.market_value_gbp = in.market_value;
        } else if (is_defined(lookup.spot_rate)) {
            out.notional_amount_gbp = in.notional_amount * lookup.spot_rate;
            out.market_value_gbp = in.market_value * lookup.spot_rate;
        } else {
            out.notional_amount_gbp = in.notional_amount;
            out.market_value_gbp = in.market_value;
        }
      ]]></property>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
    </component>

    <!-- Output enriched trades -->
    <component name="write_enriched" type="output_file">
      <input port="in0" component="fx_lookup" port="out0"/>
      <property name="filename">${OUTPUT_DIR}/trades_enriched_${BUSINESS_DATE}.dat</property>
      <property name="output_dml">dml/trade_record.dml::trade_record_t</property>
      <property name="header">true</property>
      <property name="delimiter">|</property>
    </component>
  </phase>
</graph>
