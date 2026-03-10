# mkpipe-extractor-cassandra

Cassandra extractor plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Reads Cassandra tables into Spark DataFrames using `spark-cassandra-connector`.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  cassandra_source:
    variant: cassandra
    host: localhost
    port: 9042
    database: my_keyspace
    user: cassandra
    password: cassandra
```

---

## Table Configuration

```yaml
pipelines:
  - name: cassandra_to_pg
    source: cassandra_source
    destination: pg_target
    tables:
      - name: events
        target_name: stg_events
        replication_method: full
```

### Incremental Replication

Filtering is done in Spark (post-read), as Cassandra does not support arbitrary WHERE on non-partition-key columns without ALLOW FILTERING:

```yaml
      - name: events
        target_name: stg_events
        replication_method: incremental
        iterate_column: updated_at
```

---

## Read Parallelism

By default Cassandra Spark Connector splits reads based on `spark.cassandra.input.split.size_in_mb` (default: 512 MB per split → very few Spark tasks). Setting `partitions_count` lowers the split size, resulting in more, smaller splits and more parallel Spark tasks:

```yaml
      - name: events
        target_name: stg_events
        replication_method: full
        partitions_count: 8     # target ~8 read splits (512 / 8 = 64 MB each)
```

### How it works

- `partitions_count: N` sets `spark.cassandra.input.split.size_in_mb = 512 / N`
- Lower MB per split → more splits → more parallel Spark tasks reading from Cassandra
- The connector respects token ranges; splits align to Cassandra's data distribution

### Performance Notes

- **Small tables:** default split size is fine — overhead not worth it.
- **Large tables (>500M rows):** `partitions_count: 8–32` gives significant speed-up.
- Too many splits can overwhelm Cassandra with concurrent connections — tune to your cluster size.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Cassandra table name |
| `target_name` | string | required | Destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `iterate_column` | string | — | Column used for incremental filtering (post-read) |
| `partitions_count` | int | `10` | Target number of read splits (controls `split.size_in_mb`) |
| `fetchsize` | int | `100000` | Not used by Cassandra connector (JDBC only) |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
