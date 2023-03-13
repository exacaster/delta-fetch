## Recommendations

### How to prepare data for delta-fetch
Delta Fetch is a stateless HTTP API service that works on Delta tables stored in S3. This is why the user needs to prepare the data so that it is persisted a format and layout that it could be quickly accessed in random IO fashion. Delta Fetch uses a combination of Delta tables metadata and Parquet pushdown filters to achieve these instant-like responses.

In order to be able to quickly access the data in Parquet files you need to configure block size to a smaller value that you would normally do. You can do this only for the writes of the data that will be used by Delta Fetch:
```python
...
# Fetch current Parquet block size so we could revert after writing value before
initial_parquet_block_size = spark.conf.get("parquet.block.size", None)
# Decrease Parquet block size to skip unnecessary row groups
# when lookup is needed by an API.
spark.conf.set("parquet.block.size", 1048576)

<write_data_for_delta_fetch>

# Restore Parquet block size to previous value
spark.conf.set("parquet.block.size", initial_parquet_block_size)
...
```
`1048576` (1mb) value for `parquet.block.size` is a value that we recommend.

There are few drawbacks of having Parquet block size this small:
* Increased metadata overhead: Smaller block sizes can result in more metadata overhead, as each block has its own metadata. This can increase the size of the Parquet file, which can impact storage costs.
* Slower writes: With smaller block sizes, there are more row groups to write, which can slow down the write performance.
* Increased file fragmentation: With smaller block sizes, there are more row groups, which can lead to increased file fragmentation. This can result in slower query performance due to more seek operations on the file system.

### Some caveats

#### "OPTIMZE ... ZORDER ..." command
We suggest **not** to use `OPTIMIZE ... ZORDER ...` since it usually stores data split by 1GB chunks. Writing such a huge files using single Spark task is suboptimal. We suggest rely on [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) in combination with [df.orderBy(...)](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.orderBy.html#pyspark.sql.DataFrame.orderBy). Where you order by the columns that you are planning to use as "keys" in Delta Fetch API.

#### "spark.sql.files.maxRecordsPerFile" property
Do **not** use `spark.sql.files.maxRecordsPerFile` with delta-fetch. This property adds additional Spark stage after all the processing is done and shuffles the data to different files loosing all sorting that was applied before. 
