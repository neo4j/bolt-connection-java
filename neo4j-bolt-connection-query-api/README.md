# Experimental Neo4j Query API module

This experimental module implements `BoltConnectionProvider` that communicates with [Neo4j Query API](https://neo4j.com/docs/query-api/current/).

It supports `http` and `https` URI schemes.

## Limitations

There are a number of limitations that apply to this module. For instance, it does not support streaming. All results are
fetched immediately and streaming is simulated on the client-side on the `BoltConnection` level.

Some other limitations include:
- home database resolution is not supported, the database MUST be specified explicitly
- transaction metadata is not supported
- notifications preferences are not supported
- `resultAvailableAfter` is always -1
- queryType is not supported
- plan is not supported
- profile is not supported

The above list is not exhaustive. There may be some other limitations as this module is experimental.
