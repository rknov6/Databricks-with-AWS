from pyspark import pipelines as dp
from pyspark.sql.functions import col

dp.create_streaming_table(
    name = "dev.silver.customers",
    comment = "This is silver customers table",
    expect_all_or_drop={
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_customer_name": "customer_name is not null"
    }
)

dp.create_auto_cdc_flow(
    source = 'dev.bronze.customers',
    target = 'dev.silver.customers',
    keys= ["customer_id"],
    sequence_by="ts_ms",
    stored_as_scd_type= 2,
    apply_as_deletes="op = 'd'" or "__deleted = 'true'",
    except_column_list= ["op","ts_ms","__deleted","ingest_timestamp","_source"]
)


dp.create_streaming_table(
    name = "dev.silver.orders",
    comment = 'This is silver orders table',
    expect_all_or_drop={
        "valid_order_id": "order_id IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_order_status": "amount > 0"
    }
)

dp.create_auto_cdc_flow(
    source = 'dev.bronze.orders',
    target = 'dev.silver.orders',
    keys= ["order_id"],
    sequence_by="ts_ms",
    stored_as_scd_type= 2,
    apply_as_deletes="op = 'd'" or "__deleted = 'true'",
    except_column_list= ["op","ts_ms","__deleted","ingest_timestamp","_source"]
)