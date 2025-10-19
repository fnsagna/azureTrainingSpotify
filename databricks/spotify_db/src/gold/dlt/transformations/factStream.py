import dlt

@dlt.table
def factStream_stg():
    df = spark.readStream.table("spotify_catalog.silver.factStream")
    return df

dlt.create_streaming_table("factStream")

dlt.create_auto_cdc_flow(
  target = "factStream",
  source = "factStream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_column_list = None,
  name = None,
  once = False
)

