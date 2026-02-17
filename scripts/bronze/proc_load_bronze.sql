/*
==========================================================
Stored Procedure: Load Bronze Layer
==========================================================
Purpose: This script loads data into the bronze Schema from the source system (CSV files).
Actions: 
        - Truncating bronze tables before populating them.
        - Uses the COPY command (SQL equivalent: BULK INSERT) to load data into the bronze tables. 
          (! COPY FROM path: the path is provided as example for replication purposes. Due to use of the pgAdmin browser version, other options were used to import the data)

Parameters:
  None

USAGE:
  CALL bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql 
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
	row_count INTEGER;
		

BEGIN

	RAISE NOTICE '====================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '====================';


	RAISE NOTICE '--------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '--------------------';

	batch_start_time := NOW();
	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE bronze.crm_cust_info;

	RAISE NOTICE '>> Inserting Data into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info 
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.crm_cust_info;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));


	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE bronze.crm_prd_info;
	
	RAISE NOTICE '>> Inserting Data into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
			WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.crm_prd_info;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));
	

	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE bronze.crm_sales_details;

	RAISE NOTICE '>> Inserting Data into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.crm_sales_details;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));


	RAISE NOTICE '--------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '--------------------';

	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE bronze.erp_cust_az12;

	RAISE NOTICE '>> Inserting Data into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
		WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.erp_cust_az12;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));
	

	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE bronze.erp_loc_a101;

	RAISE NOTICE '>> Inserting Data into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
		WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.erp_loc_a101;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));

	
	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE bronze.erp_px_cat_g1v2;

	RAISE NOTICE '>> Inserting Data into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
		FROM '/Users/jannikededow/Documents/Coding/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
		WITH (
			FORMAT csv,
			HEADER true,
			DELIMITER ',',
			ENCODING 'UTF8'
	);
	end_time := NOW();

	SELECT COUNT(*) INTO row_count FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'Import completed. % rows loaded.', row_count;
	RAISE NOTICE 'Load Duration: % seconds', EXTRACT(EPOCH FROM(end_time - start_time));

	batch_end_time := NOW();
	RAISE NOTICE '================================================================';
	RAISE NOTICE 'Loading Bronze Layer Successful';
	RAISE NOTICE 'Whole Batch Load Duration: % seconds', EXTRACT(EPOCH FROM(batch_end_time - batch_start_time));
	RAISE NOTICE '================================================================';

	EXCEPTION
	    WHEN OTHERS THEN
	        RAISE NOTICE '================================================================';
	        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
	        RAISE NOTICE 'Error Message: %', SQLERRM;
	        RAISE NOTICE 'Error Code: %', SQLSTATE;
			RAISE NOTICE 'Failed at: %', NOW();
	        RAISE NOTICE '================================================================';

END;

$$;





