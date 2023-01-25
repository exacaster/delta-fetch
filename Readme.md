# Delta Fetch

HTTP API for [Delta Lake](https://delta.io/). Delta Fetch allows to configure HTTP endpoints to retrieve rows from
Delta Lake tables.

## Configuration

### Routes
Resources can be configured in the following way:
```yaml
app:
  resources:
    - path: /api/data/{table}/{identifier}
      schema-path: /api/schemas/{table}/{identifier}
      delta-path: s3a://bucket/delta/{table}/
      filter-variables:
        - column: id
          path-variable: {identifier}
```

- `path` property defines API path which will be used to query your Delta tables. Path variables can be defined by using curly braces as shown in the example.
- `schema-path` (optional) property can be used to define API path for Delta table schema.
- `delta-path` property defines S3 path of your Delta table. Path variables on this path will be filled in by variables provided in API path.
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
