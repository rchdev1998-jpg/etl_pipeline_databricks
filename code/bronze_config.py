ingestion_configuration = [
    {
        "source" : "crm",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_crm/cust_info.csv",
        "target_table" : "crm_cust_info"
    },
    {
        "source" : "crm",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_crm/prd_info.csv",
        "target_table" : "crm_prd_info"
    },
    {
        "source" : "crm",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_crm/sales_details.csv",
        "target_table" : "crm_sales_details"
    },
    {
        "source" : "erp",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_erp/CUST_AZ12.csv",
        "target_table" : "erp_cust_az12"
    },
    {
        "source" : "erp",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_erp/LOC_A101.csv",
        "target_table" : "erp_loc_a101"
    },
    {
        "source" : "erp",
        "file_path" : "/Volumes/workspace/bronze/source_system/source_erp/PX_CAT_G1V2.csv",
        "target_table" : "erp_px_cat_g1v2"
    }
]