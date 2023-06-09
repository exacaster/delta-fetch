# Delta Fetch

HTTP API for [Delta Lake](https://delta.io/). Delta Fetch allows to configure HTTP endpoints to retrieve rows from
Delta Lake tables.

- :building_construction: [How it works?](./docs/architecture.md)
- :bangbang: [Storage recommendations](./docs/recommendations.md)

## Benchmarks

After applying methods mentioned in [storage recommendations](./docs/recommendations.md) you can easly achieve fetch times that are around 1s on any amount of data since it scales horizontally pretty good.

### Example

Delta table that in S3 looks like this:

![Delta table structure on S3](./docs/s3_delta_example.png "Delta table structure on S3")

It has 24498176 records. Here are few examples of the requests time it took to serve a requests (using file index cache):
```
time curl http://localhost:8080/api/data/disable_optimize_ordered/872480210503_234678
{"version":5,"data":{"user_id":"872480210503_234678","sub_type":"PREPAID","activation_date":"2018-09-01","status":"ACTIVE","deactivation_date":"9999-01-01"}}curl   0.00s user 0.01s system 1% cpu 0.982 total
---
time curl http://localhost:8080/api/data/disable_optimize_ordered/579520210231_237911
{"version":5,"data":{"user_id":"579520210231_237911","sub_type":"PREPAID","activation_date":"2018-06-24","status":"ACTIVE","deactivation_date":"9999-01-01"}}curl   0.00s user 0.01s system 0% cpu 1.250 total
---
➜  ~ time curl http://localhost:8080/api/data/disable_optimize_ordered/875540210000_245810
{"version":2,"data":{"user_id":"875540210000_245810","sub_type":"PREPAID","activation_date":"2018-09-01","status":"ACTIVE","deactivation_date":"9999-01-01"}}curl   0.00s user 0.01s system 1% cpu 0.870 total
```

## Configuration

### Cache
Service caches Delta table index (value ranges) after the first request to resource API is made.
After the first request, following requests are using in-memory index to find Parquet files
that potentially contain desired value. You can force the service to update the index by adding `?exact=true` query
param, when making a HTTP request. You can also enable background process, which will updated cached index
on your specified interval:

```yaml
app:
  cache-update-interval: 10m
```

### Routes
Resources can be configured in the following way:
```yaml
app:
  resources:
    - path: /api/data/{table}/{identifier}
      schema-path: /api/schemas/{table}/{identifier}
      delta-path: s3a://bucket/delta/{table}/
      response-type: SINGLE
      filter-variables:
        - column: id
          path-variable: identifier
```

- `path` property defines API path which will be used to query your Delta tables. Path variables can be defined by using curly braces as shown in the example.
- `schema-path` (optional) property can be used to define API path for Delta table schema.
- `delta-path` property defines S3 path of your Delta table. Path variables on this path will be filled in by variables provided in API path.
- `response-type` (optional, default: `SINGLE`) property defines weather to search for multiple resources, or a single one. Use `LIST` type for multiple resources.
- `max-results` (optional, default: `100`) maximum number of rows that can be returned in case of `LIST` `response-type`.
- `filter-variables` (optional) additional filters applied to Delta table.

### Security
Delta Fetch currently supports two authorization mechanisms.

#### Basic Auth
Example of basic authentication:
```yaml
app:
  security:
    enabled: true
    basic:
      enabled: true
      username: username
      password: password
```

#### OAuth2
We also support OAuth2 with JWT tokens, which are verified by using JWK (OpenID Connect):
```yaml
app:
  security:
    enabled: true
    oauth2:
      enabled: true
      claims-validators:
        issuer: https://issuer.url/oauth/token
      jwks:
        url: https://stagecat.exacaster.com/uaa/token_keys
      allowed-scopes:
        - ws.117.owner
```

If JWT token has any of scope defined in `allowed-scopes`, user is allowed to access the API.
Path variables can also be used in scope list, for example: `ws.{worpsace}.owner`.

### S3 Credentials
To configure credentials for S3 connection use these properties:

```yaml
app:
  hadoop-props:
    "fs.s3a.access.key": XXX
    "fs.s3a.secret.key": XXXXXX
```

## Contributing

See [Contribution guide](./docs/CONTRIBUTING.md)

## License

Delta Fetch is [MIT](./LICENSE.txt) licensed.
