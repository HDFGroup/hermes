# KVStore

The Hermes KVStore adapter provides a simplistic in-memory key-value store
interface similar to that of Redis. Currently, we only provide the minimum
functionality to benchmark KVStore over the YCSB benchmark.

## Current Operation Support

Below we describe the operations our KVStore currently supports:
1. Create a table: a table contains a set of records.
2. Create a record: a record is a <key, ARRAY(<field, value>)>. The ARRAY
is stored as a single blob in Hermes. Updating a field in a record will
require reading the entire blob.
3. Update a record
4. Get a set of fields from a record
5. Scan a subset of records
6. Delete a record
7. Delete a table