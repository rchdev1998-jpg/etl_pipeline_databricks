transformation_config = [
    {
        "table_key" : "cst_id",
        "load_type" : "scd2",

        "source_table" : "workspace.bronze.crm_cust_info",
        "target_table" : "workspace.silver.crm_cust_info",

        "compare_columns" : [
            "cst_id",
            "cst_key",
            "cst_firstname",
            "cst_lastname",
            "cst_marital_status",
            "cst_gndr",
            "cst_create_date"
        ],

        "transformation" : """
                        select
                        s.cst_id,
                        trim(s.cst_key) as cst_key,
                        case
                            when s.cst_firstname is null then 'n/a'
                            else lower(trim(s.cst_firstname))
                        end as cst_firstname,
                        case
                            when s.cst_lastname is null then 'n/a'
                            else lower(trim(s.cst_lastname))
                        end as cst_lastname,
                        case
                            when s.cst_marital_status is null or s.cst_marital_status = ' ' then 'n/a'
                            when upper(trim(s.cst_marital_status)) = 'M' then 'merraid'
                            when upper(trim(s.cst_marital_status)) = 'S' then 'single'
                            else s.cst_marital_status
                        end as cst_marital_status,
                        case
                            when s.cst_gndr is null or s.cst_gndr = ' ' then 'n/a'
                            when upper(trim(s.cst_gndr)) = 'M' then 'male'
                            when upper(trim(s.cst_gndr)) = 'F' then 'female'
                            else s.cst_gndr
                        end as cst_gndr,
                        s.cst_create_date
                        from
                        (
                            select 
                            *,
                            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as rank
                            from workspace.bronze.crm_cust_info
                            where cst_id is not null
                        ) as s
                        where s.rank = 1"""
    },
    {
        "table_key" : "prd_id",
        "load_type" : "scd2",

        "source_table" : "workspace.bronze.crm_prd_info",
        "target_table" : "workspace.silver.crm_prd_info",

        "compare_columns" : [
            "prd_id",
            "prd_key",
            "cat_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt"
        ],

        "transformation" : f"""
                        select
                            prd_id,
                            prd_key,
                            substring(prd_key, 1, 5) as cat_key,
                            lower(trim(prd_nm)) as prd_nm,
                            case
                                when prd_cost is null then 0
                                else prd_cost
                            end as prd_cost,
                            case
                                when prd_line = 'M' then 'maountain'
                                when prd_line = 'R' then 'road'
                                when prd_line = 'S' then 'other sales'
                                when prd_line = 'T' then 'touring'
                                else 'n/a'
                            end as prd_line,
                            prd_start_dt,
                            prd_end_dt
                            from workspace.bronze.crm_prd_info"""
    },
    {
        "table_key" : "sls_ord_num",
        "load_type" : "overwrite",

        "source_table" : "workspace.bronze.crm_sales_details",
        "target_table" : "workspace.silver.crm_sales_details",

        "compare_columns" : [
            "sls_ord_num",
            "sls_prd_key",
            "sls_cust_id",
            "sls_order_dt",
            "sls_ship_dt",
            "sls_due_dt",
            "sls_sales",
            "sls_quantity",
            "sls_price"
        ],

        "transformation" : """
                        select 
                        sls_ord_num,
                        sls_prd_key,
                        sls_cust_id,
                            CASE
                                WHEN sls_order_dt = 0 THEN NULL
                                ELSE try_to_date(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
                            END AS sls_order_dt,

                            CASE
                                WHEN sls_ship_dt = 0 THEN NULL
                                ELSE try_to_date(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
                            END AS sls_ship_dt,

                            CASE
                                WHEN sls_due_dt = 0 THEN NULL
                                ELSE try_to_date(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
                            END AS sls_due_dt,
                        case
                            when sls_sales is null or sls_sales <=0 or sls_sales != (sls_quantity * abs(sls_price)) then sls_quantity * abs(sls_price)
                            else sls_sales
                        end as sls_sales,
                        sls_quantity,
                        case
                            when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
                            else sls_price
                        end as sls_price
                        from workspace.bronze.crm_sales_details
                        """
    },
    {
        "table_key" : "CID",
        "load_type" : "scd2",

        "source_table" : "workspace.bronze.erp_cust_az12",
        "target_table" : "workspace.silver.erp_cust_az12",

        "compare_columns" : [
            "cid",
            "bdate",
            "gen"
        ],

        "transformation" : """
                        select
                        case
                            when CID lIKE 'NAS%' then substring(CID, 4, len(cid))
                            else CID
                        end as cid,
                        case
                            when BDATE > GETDATE() then null
                            else BDATE
                        end as bdate,
                        case
                            when upper(trim(gen)) in('F', 'FEMALE') then 'Female'
                            when upper(trim(gen)) in('M', 'MALE' ) then 'Male'
                            else 'n/a'
                        end as gen
                        from workspace.bronze.erp_cust_az12
                        """
    },
    {
        "table_key" : "cid",
        "load_type" : "scd2",

        "source_table" : "workspace.bronze.erp_loc_a101",
        "target_table" : "workspace.silver.erp_loc_a101",

        "compare_columns" : [
            "cid",
            "cntry"
        ],

        "transformation" : """
                        select
                        replace(cid, '-', '') as cid,
                        case
                            when trim(cntry) = 'DE' then 'Germany'
                            when trim(cntry) in('US', 'USA') then 'United States'
                            when trim(cntry) = '' or cntry is null then 'n/a'
                            else trim(cntry)
                        end as cntry
                        from workspace.bronze.erp_loc_a101
                        """
    },
    {
        "table_key" : "id",
        "load_type" : "scd2",

        "source_table" : "workspace.bronze.erp_px_cat_g1v2",
        "target_table" : "workspace.silver.erp_px_cat_g1v2",

        "compare_columns" : [
            "id",
            "cat",
            "subcat",
            "maintenance"
        ],
        
        "transformation" : """
                        select
                        id,
                        cat,
                        subcat,
                        maintenance
                        from workspace.bronze.erp_px_cat_g1v2
                        """
    }
]



