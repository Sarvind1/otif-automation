from imports import *


def fetch_from_redshift(user_id, password, database, host, port, sql_query):
    conn_params = {
    'user': user_id,
    'password': password,
    'database': database,
    'host': host,
    'port': port
    }
    sql_query = sql_query
    
    def connect_to_redshift(params):
        """Establish a connection to Redshift."""
        try:
            conn = connect(**params)
            return conn
        except InterfaceError as e:
            print(f"InterfaceError: {e}")
            raise
    
    def fetch_data(conn, query):
        """Run SET and SELECT queries and return a DataFrame."""
        with conn.cursor() as cursor:
            # cursor.execute("SET enable_case_sensitive_identifier TO TRUE;")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
    
    conn = None
    try:
        conn = connect_to_redshift(conn_params)
        df = fetch_data(conn, sql_query)
        num_records_fetched = len(df)  # Store the number of records fetched
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if conn:
            conn.close()
        
    return df


po_sql_query = """
    WITH RankedData AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY CONCAT(document_number, line_id) 
                ORDER BY snapshot_datetime DESC
            ) AS row_num
        FROM (
            SELECT 
                *, 
                DENSE_RANK() OVER (
                    PARTITION BY CONCAT(document_number, line_id) 
                    ORDER BY snapshot_datetime DESC
                ) AS PORank
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        ) AS POData
        WHERE 
            final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
            AND PORank = 1
            AND (quantity - "quantity_fulfilled/received") > 0
            AND (scm_po_scm_memo IS NULL OR scm_po_scm_memo != 'import_ic_flow')
    )
    SELECT 
        id, 
        TO_DATE(date_created, 'DD.MM.YYYY HH24:MI') AS date_created, 
        document_number,
        subsidiary_no_hierarchy, 
        scm_associated_brands, 
        po_vendor, 
        supplier_confirmation_status,
        final_status, 
        scm_po_scm_memo, 
        marketplace_header, 
        supplier_payment_terms, 
        incoterms,
        line_id, 
        item, 
        asin, 
        quantity, 
        "quantity_fulfilled/received", 
        quantity_on_shipments, 
        quantity_billed,
        item_rate, 
        currency, 
        item_rate_eur, 
        amount_foreign_currency, 
        TO_DATE(first_prd, 'DD.MM.YYYY') AS first_prd,
        prd, 
        planned_prd, 
        TO_DATE(accepted_prd, 'DD.MM.YYYY') AS accepted_prd, 
        prd_status, 
        confirmed_crd,
        quality_control_date, 
        quality_control_status, 
        im_line_signoff, 
        sm_line_signoff, 
        production_status,
        batch_id, 
        wh_type, 
        "considered_for_anti-po", 
        prd_reconfirmation, 
        prd_change_reason,
        invoice_number, 
        invoice_status, 
        "historical_anti-po",
        snapshot_datetime
    FROM RankedData
    WHERE row_num = 1;
    """ 

pl_sql_query = """
    SELECT
        CASE 
            WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
            THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
            ELSE shipment_batch_id_pl_id 
        END AS batch_id,
    
        CASE 
            WHEN pl_status = 'accepted-ffw' THEN 'Signed-Off'
            WHEN pl_status = 'accepted-sm' THEN '14c. FFW Sign-Off Missing'
            WHEN pl_status IN ('ocr1-accepted', 'uploaded', 'ocr2-rejected', 'ocr2-accepted') 
                THEN '14b. SM Sign-Off Missing'
            ELSE '14a. Documents Missing' 
        END AS final_status
    
    FROM 
        razor_db.vendor_portal.invoicing_packinglist_uploads_ddb_logs
    
    WHERE
        pl_status IN (
            'rejected-ffw', 'accepted-ffw', 'accepted-sm', 
            'ocr1-accepted', 'uploaded', 'ocr2-rejected', 'ocr2-accepted'
        )
        AND LENGTH(
            CASE 
                WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
                THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
                ELSE shipment_batch_id_pl_id 
            END
        ) = 12
    
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
                    THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
                    ELSE shipment_batch_id_pl_id 
                END
            ORDER BY 
                CAST(created_date AS TIMESTAMP) DESC, 
                CAST(approximate_ts AS TIMESTAMP) DESC
        ) = 1;
    """

batch_sql_query = """
    SELECT 
    BatchData.batch_id,
    MAX(BatchData.vp_booking_status) AS vp_booking_status,
    MAX(BatchData.freight_forwarder) AS freight_forwarder,
    MAX(BatchData.po_number) AS po_number,
    MAX(BatchData.incoterms) AS incoterms,
    MAX(BatchData.scr_date) AS scr_date,
    MAX(BatchData.scrd_delay_reasons) AS scrd_delay_reasons,
    MAX(BatchData.ccrd_by_freight) AS ccrd_by_freight,
    MAX(BatchData.expected_pickup_date) AS expected_pickup_date,
    MAX(BatchData.actual_pickup_date) AS actual_pickup_date,
    MAX(BatchData.gate_in_date) AS gate_in_date,
    MAX(BatchData.expected_shipping_date) AS expected_shipping_date,
    MAX(BatchData.actual_shipping_date) AS actual_shipping_date
    FROM razor_db.netsuite.batch_lines AS BatchData
    
    LEFT JOIN (
        SELECT 
            batch_id, 
            final_status, 
            line_id
        FROM (
            SELECT 
                batch_id,
                final_status,
                line_id,
                DENSE_RANK() OVER (
                    PARTITION BY CONCAT(batch_id, line_id) 
                    ORDER BY snapshot_date DESC
                ) AS PORank
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        ) AS RankedData
        WHERE PORank = 1
    ) AS POData
    ON BatchData.batch_id = POData.batch_id
    
    WHERE POData.final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
    
    GROUP BY BatchData.batch_id;
    """

inb_sql_query = """
    SELECT
    INBData.shipment_number,
    INBData.date_created,
    INBData.freight_forwarder,
    INBData.external_document_number,
    INBData.status,
    INBData.substatus,
    INBData.market_place,
    INBData.po,
    POData.line_id,
    INBData.item,
    INBData.scm_associated_brand,
    INBData.quantity_expected,
    INBData.quantity_received,
    INBData.quantity_remaining_to_be_received,
    INBData.scm_destination_warehouse,
    INBData.shipment_method,
    INBData.shipment_type,
    INBData.cargo_ready_date AS cargo_ready_date,
    INBData.expected_pick_up_date AS expected_pick_up_date,
    INBData.actual_cargo_pick_up_date AS actual_cargo_pick_up_date,
    INBData.expected_shipping_date AS expected_shipping_date,
    INBData.actual_shipping_date AS actual_shipping_date,
    INBData.expected_arrival_date AS expected_arrival_date,
    INBData.actual_arrival_date AS actual_arrival_date,
    INBData.expected_delivery_date AS expected_delivery_date,
    INBData.actual_delivery_date AS actual_delivery_date,
    INBData.header_snapshot_date AS header_snapshot_date,
    INBData.line_snapshot_date AS line_snapshot_date
    FROM (
        SELECT
            INBH.shipment_number,
            INBH.date_created,
            INBH.freight_forwarder,
            INBH.external_document_number,
            INBH.status,
            INBH.substatus,
            INBH.market_place,
            INBL.po,
            CASE
                WHEN CHARINDEX('_', INBL.join_collum) > 0
                THEN RIGHT(INBL.join_collum, LEN(INBL.join_collum) - CHARINDEX('_', INBL.join_collum))
                ELSE NULL
            END AS line_id,
            INBL.item,
            INBL.scm_associated_brand,
            INBL.quantity_expected,
            INBL.quantity_received,
            INBL.quantity_remaining_to_be_received,
            INBH.scm_destination_warehouse,
            INBH.shipment_method,
            INBH.shipment_type,
            INBH.cargo_ready_date,
            INBH.expected_pick_up_date,
            INBH.actual_cargo_pick_up_date,
            INBH.expected_shipping_date,
            INBH.actual_shipping_date,
            INBH.expected_arrival_date,
            INBH.actual_arrival_date,
            INBH.expected_delivery_date,
            INBH.actual_delivery_date,
            INBL.po_line_unique_key,
            INBH.snapshot_date AS header_snapshot_date,
            INBL.snapshot_date AS line_snapshot_date
        FROM razor_db.public.rgbit_netsuite_inbound_shipments_header AS INBH
        INNER JOIN razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey AS INBL
            ON INBH.shipment_number = INBL.shipment_number
    ) AS INBData
    LEFT JOIN (
        SELECT
            document_number,
            line_id,
            item,
            quantity,
            "quantity_fulfilled/received",
            po_line_unique_key
        FROM (
            SELECT
                document_number,
                line_id,
                item,
                quantity,
                "quantity_fulfilled/received",
                po_line_unique_key,
                DENSE_RANK() OVER (
                    PARTITION BY CONCAT(document_number, line_id)
                    ORDER BY snapshot_datetime DESC
                ) AS PORank
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        ) AS RankedPO
        WHERE PORank = 1
    ) AS POData
    ON INBData.po_line_unique_key = POData.po_line_unique_key
    WHERE (POData.quantity - POData."quantity_fulfilled/received") > 0
      AND INBData.shipment_number IS NOT NULL
      AND INBData.shipment_number <> '';
    """

telex_sql_query = """
    SELECT
    shipment_number,
    MAX(batch_telex_date) AS telex_release_date_supplier,
    MAX(telex_release_date) AS telex_release_date_ffwp
    FROM (
        SELECT
            id,
            recordtype,
            TO_DATE(shp_head.date_created, 'dd.mm.yyyy') AS shipment_created_date,
            shp_head.shipment_number,
            external_document_number,
            status,
            substatus,
            current_owner,
            market_place,
            freight_forwarder,
            vendor_other,
            shipment_method,
            shipment_type,
            palletized,
            spd,
            fba_id,
            port_of_departure_pod,
            port_of_arrival_poa,
            currency_cy,
            cy_to_cy_rate,
            currency_qsc,
            quoted_shipping_cost,
            currency_lmc,
            last_mile_cost,
            cargo_ready_date,
            expected_shipping_date,
            expected_arrival_date,
            expected_delivery_date,
            actual_cargo_pick_up_date,
            actual_shipping_date,
            actual_arrival_date,
            actual_delivery_date,
            CASE
                WHEN gross_volume_cbm IS NULL OR gross_volume_cbm = '' OR gross_volume_cbm = 0 THEN net_volume_cbm_auto_calculated_custom
                ELSE gross_volume_cbm
            END AS gross_volume_cbm,
            net_volume_cbm_auto_calculated_custom,
            net_weight_kg,
            net_weight_kg_auto_calculated_custom,
            vessel_number,
            bol_awb_cim_cmr_no,
            container_number_container_type,
            container_teu,
            container_quantity,
            commercial_invoice,
            packing_lists,
            bol_awb_cim_cmr,
            customs_declarations,
            proof_of_delivery,
            scm_destination_warehouse,
            receiving_warehouse_type,
            unis_facility,
            unis_asn_ref,
            sent_to_fiege,
            everstox_asn_ref,
            everstox_status,
            scm_memo,
            scm_link_to_inbship,
            scm_weeks_to_arrival,
            scm_warehousing_memo,
            scm_applied_purchase_order,
            scm_volume_deviation,
            scm_weight_deviation,
            scm_cy2cycost_per_cbm,
            type_of_dispute,
            datascope_claim_ref,
            substatus_wh,
            TO_DATE(shp_head.telex_release_date, 'dd.mm.yyyy') AS telex_release_date,
            TO_DATE(shp_head.gate_in, 'dd.mm.yyyy') AS gate_in,
            TO_DATE(shp_head.gate_out, 'dd.mm.yyyy') AS gate_out,
            TO_DATE(shp_head.customs_clearance_date, 'dd.mm.yyyy') AS customs_clearance_date,
            customs_clearance,
            snapshot_date,
            ff_remarks,
            shp_line.*,
            TO_DATE(bcd.booking_confirmation_date, 'dd.mm.yyyy') AS booking_confirmation_date,
            TO_DATE(bcd.actual_inbound_date, 'dd.mm.yyyy') AS actual_inbound_date,
            PO_LINE.BL_VALUE,
            PO_LINE.BL_Days,
            PO_LINE.asin,
            PO_LINE.batch_id,
            PO_LINE.amount,
            invoice_tbl.document_number AS Inv_number,
            invoice_tbl.Inv_Amount,
            invoice_tbl.inv_Status,
            invoice_tbl.Inv_Qty,
            batch_telex.batch_telex_date,
            CURRENT_DATE AS data_update_date
        FROM razor_db.public.rgbit_netsuite_inbound_shipments_header shp_head
        INNER JOIN (
            SELECT
                shipment_number AS ship_num,
                PO,
                item,
                Vendor,
                quantity_expected,
                quantity_received,
                po_line_unique_key
            FROM razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey
        ) shp_line
            ON shp_head.shipment_number = shp_line.ship_num
        LEFT JOIN (
            SELECT
                DISTINCT record AS shipment_number,
                MIN(booking_confirmation_date) AS booking_confirmation_date,
                MIN(actual_inbound_date) AS actual_inbound_date
            FROM (
                SELECT *,
                    CASE
                        WHEN field = 'Status' AND new_value IN ('partiallyReceived', 'received') AND old_value = 'inTransit'
                            THEN date
                    END AS actual_inbound_date,
                    CASE
                        WHEN field = 'External Document Number' AND new_value IS NOT NULL AND old_value IS NULL
                            THEN date
                    END AS booking_confirmation_date
                FROM razor_db.public.rgbit_shipment_system_notes_raw_ns
            )
            GROUP BY 1
        ) bcd
            ON shp_head.shipment_number = bcd.shipment_number
        LEFT JOIN (
            SELECT
                DISTINCT asin,
                supplier_payment_terms,
                CAST(SPLIT_PART(supplier_payment_terms, '%', 1) AS INT) AS pi_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 2), 'd ', 2), '') AS INT), 0) AS ci_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 3), 'd ', 2), '') AS INT), 0) AS bl_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 4), ' ', 3), 'd', 1), '') AS INT), 0) AS BL_Days,
                po_line_unique_key,
                document_number,
                item,
                line_id,
                batch_id,
                item_rate_eur AS amount
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        ) PO_LINE
            ON shp_line.po_line_unique_key = PO_LINE.po_line_unique_key
        LEFT JOIN (
            SELECT
                po_line_unique_key,
                LISTAGG(document_number, ', ') AS document_number,
                SUM(maximum_of_amount_eur_consolidated) AS Inv_Amount,
                SUM(absolute_value_of_maximum_of_quantity) AS Inv_Qty,
                LISTAGG(status, ', ') AS INV_STATUS
            FROM razor_db.public.rgbit_po_invoice_mapping
            GROUP BY po_line_unique_key
        ) invoice_tbl
            ON po_line.po_line_unique_key = invoice_tbl.po_line_unique_key
        LEFT JOIN (
            SELECT
                batch_ID,
                MAX(telex_release_date) AS batch_telex_date
            FROM (
                SELECT *,
                    SPLIT_PART(vendor_id_po_number, '#', 2) AS PO_Number,
                    SPLIT_PART(shipment_batch_id_pl_id, '#', 1) AS Batch_ID,
                    TO_DATE(created_date, 'yyyy.mm.dd') AS telex_release_date
                FROM razor_db.vendor_portal.invoicing_packinglist_uploads_ddb_logs
                WHERE telex_Status = 'uploaded'
            )
            GROUP BY 1
        ) batch_telex
            ON po_line.batch_id = batch_telex.batch_id
    ) final
    GROUP BY shipment_number;
    """

pi_sql_query = """
    SELECT 
    approximate_ts,
    vendor_id_po_number,
    invoice_status
    FROM (
        SELECT  
            TO_TIMESTAMP(snapshot_date, 'YYYY-MM-DD HH24:MI:SS') AS snapshot_date,
            sequence_number,
            TO_TIMESTAMP(approximate_ts, 'YYYY-MM-DD HH24:MI:SS') AS approximate_ts,
            vendor_id_po_number,
            invoice_type_invoice_id,
            created_date,
            invoice_pdf_link,
            netsuite_invoice_id,
            invoice_status,
            uploaded_by,
            auto_comments
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY vendor_id_po_number, extracted_part ORDER BY approximate_ts) AS rct,
                ROW_NUMBER() OVER (PARTITION BY vendor_id_po_number, extracted_part ORDER BY approximate_ts DESC) AS rn
            FROM (
                SELECT 
                    SPLIT_PART(invoice_type_invoice_id, '#', 1) AS extracted_part,
                    *
                FROM razor_db.vendor_portal.invoicing_invoice_uploads_ddb_logs
            )
        )
        WHERE rn = 1
        ORDER BY approximate_ts DESC
    );
    """

pi_ns_sql_query = """
    SELECT
    DISTINCT FinalData.document_number,
    FinalData.status,
    FinalData.snapshot_date,
    FinalData.po_number
    FROM (
        SELECT PayData.*, POData.final_status
        FROM (
            -- Extract latest snapshot per document
            SELECT *,
                   RIGHT(s_c_po, 8) AS po_number
            FROM (
                SELECT *,
                       DENSE_RANK() OVER (PARTITION BY document_number ORDER BY snapshot_date DESC) AS PayRank
                FROM razor_db.public.rgbit_po_line_status_pocohort
            ) AS RankedPayData
            WHERE po_number IS NOT NULL
              AND PayRank = 1
              AND document_number NOT LIKE '%cancelled%'
        ) AS PayData
        LEFT JOIN (
            -- Extract latest purchase order status
            SELECT RankedPO.document_number AS po_document_number,
                   RankedPO.final_status
            FROM (
                SELECT document_number,
                       final_status,
                       DENSE_RANK() OVER (PARTITION BY document_number ORDER BY snapshot_date DESC) AS PORank
                FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
            ) AS RankedPO
            WHERE PORank = 1
        ) AS POData
        ON PayData.po_number = POData.po_document_number
    ) AS FinalData
    WHERE FinalData.final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
    AND (po_number IS NOT NULL AND po_number <> '')
    ORDER BY CASE
        WHEN LOWER(TRIM(FinalData.status)) = 'paid in full' THEN 1
        WHEN LOWER(TRIM(FinalData.status)) = 'pending approval' THEN 2
        WHEN LOWER(TRIM(FinalData.status)) = 'open' THEN 3
        ELSE 4
    END;
    """

supplier_confirmation_sql_query = """
    SELECT 
    snapshot_date,
    approximate_ts, 
    po_number, 
    po_status 
    FROM ( 
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY po_number ORDER BY approximate_ts) AS row_num, 
            * 
        FROM 
            razor_db.vendor_portal.purchaseorders_headers_ddb_logs
        WHERE 
            po_number IS NOT NULL AND po_number <> ''
    ) subquery 
    WHERE row_num = 1
    ORDER BY approximate_ts DESC;
    """

master_data_sql_query = """
    WITH RankedData AS (
        SELECT
            MasterData.*,
            ROW_NUMBER() OVER (
                PARTITION BY MasterData.razin, MasterData.market_place
                ORDER BY MasterData.snapshot_date DESC
            ) AS rn
        FROM razor_db.core.razin_product_stage_master_mapping AS MasterData
        INNER JOIN (
            SELECT
                item,
                marketplace_header,
                quantity,
                "quantity_fulfilled/received"
            FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
        ) AS POData
            ON MasterData.razin = POData.item
            AND MasterData.market_place = POData.marketplace_header
        WHERE
            (POData.quantity - COALESCE(POData."quantity_fulfilled/received", 0)) > 0
            AND MasterData.block_reason_code IS NOT NULL
            AND TRIM(MasterData.block_reason_code) <> ''
    )
    SELECT
        razin,
        market_place,
        operating_status,
        block_reason_code,
        preferred_supplier_open_po_stock_impact
    FROM RankedData
    WHERE rn = 1;
    """

compliance_query = """
    SELECT
        id as record_id, '' as deal_name, compliance_test_results as deal_stage,
        razin, marketplace, compliance_status, vendor
    FROM razor_db.core.razin_mp_vendor_master_data;
    """

dod_query = """
    SELECT 
        po_number || razin || line_id::text AS po_razin_id,
        po_created_date,
        po_approval_date,
        supplier_confirmation_date,
        pi_invoice_approval_date,
        pi_payment_date,
        receive_first_prd_date,
        prd_reconfirmed_date,
        po_im_date_value,
        po_sm_date_value,
        batch_created_ts,
        sm_signoff_ts,
        ci_invoice_approval_date,
        ci_payment_date,
        qc_schedule_date,
        ffw_booking_ts,
        spd_ts,
        stock_pickup_date,
        shipment_creation_date,
        shipment_in_transit_date,
        bi_invoice_approval_date,
        bi_payment_date,
        ffwp_telex_release_date,
        shipment_stock_delivery_date,
        item_receipt_date,
        actual_cargo_pick_up_date,
        actual_shipping_date,
        actual_arrival_date,
        actual_delivery_date,
        first_prd_date,
        final_prd_date,
        planned_prd,
        batch_spd,
        qi_date
    FROM (
        SELECT 
            DoDData.*,
            POData.quantity,
            POData."quantity_fulfilled/received"
        FROM razor_db.procurement.otif_tracker AS DoDData
        LEFT JOIN (
            SELECT 
                document_number,
                line_id,
                item,
                quantity,
                "quantity_fulfilled/received",
                DENSE_RANK() OVER (
                    PARTITION BY document_number || line_id::text
                    ORDER BY snapshot_datetime DESC
                ) AS PORank
            FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
        ) AS POData
        ON DoDData.po_number = POData.document_number
        AND DoDData.line_id = POData.line_id
        WHERE PORank = 1
    ) AS FinalData
    WHERE final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
    AND (quantity - "quantity_fulfilled/received") > 0
    AND (scm_po_scm_memo IS NULL OR scm_po_scm_memo != 'import_ic_flow');
    """

def main(creds):
    user = creds['user']
    password = creds['password']
    host = creds['host']
    port = int(creds['port'])
    database = creds['database']
    
    po_data = fetch_from_redshift(user, password, database, host, port, po_sql_query)

    
    pl_data = fetch_from_redshift(user, password, database, host, port, pl_sql_query)

    batch_data = fetch_from_redshift(user, password, database, host, port, batch_sql_query)
    batch_data['Booking Status'] = batch_data.apply(
        lambda row: "Not Booked" if row["vp_booking_status"] == "Cancelled"
        else "Booked" if row["vp_booking_status"] != ""
        else "Booked" if row["freight_forwarder"] != ""
        else "Not Booked", axis=1)

    inb_data = fetch_from_redshift(user, password, database, host, port, inb_sql_query)
    inb_data['PO&RAZIN&ID'] = inb_data['po'].astype(str) + inb_data['item'].astype(str) + inb_data['line_id'].astype(str)

    telex_tableau = fetch_from_redshift(user, password, database, host, port, telex_sql_query)
    telex_tableau['Final Status (Supplier)'] = telex_tableau["telex_release_date_supplier"].apply(lambda x: "Not Released" if x == "" else "Released")
    telex_tableau['Final Status (FFW)'] = telex_tableau["telex_release_date_ffwp"].apply(lambda x: "Not Released" if x == "" else "Released")

    pi_data = fetch_from_redshift(user, password, database, host, port, pi_sql_query)
    pi_data['PO#'] = pi_data['vendor_id_po_number'].apply(lambda x: x[x.find("#")+1:x.find("#")+9] if "#" in x else "")
    data = {
        "invoice_status": [
            "rejected", "ocr2-rejected", "ocr1-rejected", "cancelled", np.nan, "-", "invalid",
            "ocr1-accepted", "ocr2-accepted", "uploaded",
            "accepted", "pending-ns",
            "open-ns", "paid", "rejected-ns"
        ],
        "Status": [
            "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending",
            "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending",
            "04a. SM Review Pending", "04a. SM Review Pending", "04a. SM Review Pending",
            "04b. Accounting Approval Pending", "04b. Accounting Approval Pending",
            "05b. Pending Approval", "05a. Approved", "05b. Pending Approval"
        ]
    }
    pi_data_map = pd.DataFrame(data)
    pi_data["status"] = pi_data["invoice_status"].map(pi_data_map.set_index("invoice_status")["Status"]).fillna("03. PI Upload Pending")

    pi_ns_data = fetch_from_redshift(user, password, database, host, port, pi_ns_sql_query)

    supplier_confirmation = fetch_from_redshift(user, password, database, host, port, supplier_confirmation_sql_query)

    master_data = fetch_from_redshift(user, password, database, host, port, master_data_sql_query)
    master_data["razin_mp"] = master_data["razin"].astype(str) + master_data["market_place"].astype(str)
    master_data["Action"] = master_data["preferred_supplier_open_po_stock_impact"].replace({
        "None": "No Blocker",
        "Reroute to non-Blocked Geo or Cancel PO": "Reroute or Cancel"
    }).fillna(master_data["preferred_supplier_open_po_stock_impact"])

    compliance_hubspot = fetch_from_redshift(user, password, database, host, port, compliance_query)
    compliance_hubspot = compliance_hubspot[["deal_stage", "razin", "marketplace", "compliance_status", "vendor"]]
    eu_markets = {"FR", "BE", "ES", "PL", "NL", "SE", "IT", "DE"}
    compliance_hubspot["Final MP"] = compliance_hubspot["marketplace"].apply(lambda x: "Pan-EU" if x in eu_markets else x)
    compliance_hubspot["RAZIN&MP"] = compliance_hubspot["razin"].astype(str).str.strip() + compliance_hubspot["Final MP"].astype(str)
    compliance_hubspot["Vendor Code"] = compliance_hubspot["compliance_status"].str.extract(r"^(\S+)", expand=False).fillna("")
    compliance_hubspot["RAZIN&MP&Vendor"] = compliance_hubspot["marketplace"] + compliance_hubspot["compliance_status"]

    dod_data = fetch_from_redshift(user, password, database, host, port, dod_query)

    return {
        'po_data': po_data,
        'pl_data': pl_data,
        'batch_data': batch_data,
        'inb_data': inb_data,
        'telex_tableau': telex_tableau,
        'pi_data': pi_data,
        'pi_ns_data': pi_ns_data,
        'supplier_confirmation': supplier_confirmation,
        'master_data': master_data,
        'compliance_hubspot': compliance_hubspot,
        'dod_data': dod_data
    }

