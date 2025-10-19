import dlt

expectations = {
    "rule_1" : "user_id IS NOT NULL"
}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimUser_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimuser")
    return df

dlt.create_streaming_table(
    name = "dimuser",
    expect_all_or_drop = expectations
)

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimUser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  name = None,
  once = False
)

