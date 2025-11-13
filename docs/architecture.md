# Concept

Delta Fetch heavily relies on Delta table metadata, which contains statistics about each Parquet file.
The same metadata that is used for [data skipping](https://docs.delta.io/latest/optimizations-oss.html#data-skipping)
is used to read only relevant files, in particular - minimum and maximum value of each column in each file. The Delta table
metadata is cached for better performance and can be refreshed by enabling auto cache update or making API requests
with the `...?exact=true` query parameter.

## How a single request works

- The user makes an API request to one of the configured API resources.
- Delta Fetch reads Delta table metadata from file storage and stores it in memory.
- Delta Fetch finds the relevant file paths in the stored metadata and starts reading them.
- Delta Fetch uses the Hadoop Parquet Reader implementation, which supports filter push down to avoid reading the entire file.
- Delta Fetch reads all relevant Parquet files in parallel using a dedicated thread pool, then applies the requested or configured limit to the combined results.

## Considerations
Although, we have been using [open source](https://delta.io/) version of Delta table, Delta Fetch should also work with
[Databricks](https://docs.databricks.com/delta/index.html) version.

Delta Fetch works best when resources are configured with static delta-path values.
Additionally, it is recommended to store your data sorted by the column used on data filters.
This allows Delta Fetch to read fewer Parquet files.

To learn more about Delta table configuration, which works best with Delta Fetch, click  [here](./recommendations.md).
