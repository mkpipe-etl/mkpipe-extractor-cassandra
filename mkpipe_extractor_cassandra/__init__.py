from typing import Optional

from mkpipe.spark.base import BaseExtractor
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = ['com.datastax.spark:spark-cassandra-connector_2.12:3.5.1']

logger = get_logger(__name__)


class CassandraExtractor(BaseExtractor, variant='cassandra'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port or 9042
        self.username = connection.user
        self.password = connection.password
        self.keyspace = connection.database

    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        logger.info({
            'table': table.target_name,
            'status': 'extracting',
            'replication_method': table.replication_method.value,
        })

        spark.conf.set('spark.cassandra.connection.host', self.host)
        spark.conf.set('spark.cassandra.connection.port', str(self.port))
        if self.username:
            spark.conf.set('spark.cassandra.auth.username', self.username)
        if self.password:
            spark.conf.set('spark.cassandra.auth.password', self.password)

        reader = (
            spark.read.format('org.apache.spark.sql.cassandra')
            .option('keyspace', self.keyspace)
            .option('table', table.name)
        )

        if table.partitions_count:
            split_size_mb = max(1, 512 // table.partitions_count)
            spark.conf.set('spark.cassandra.input.split.size_in_mb', str(split_size_mb))

        df = reader.load()

        write_mode = 'overwrite'
        last_point_value = None

        if table.replication_method.value == 'incremental' and table.iterate_column:
            if last_point:
                from pyspark.sql import functions as F
                df = df.filter(F.col(table.iterate_column) > last_point)
                write_mode = 'append'
            row = df.agg({table.iterate_column: 'max'}).first()
            if row and row[0] is not None:
                last_point_value = str(row[0])

        logger.info({
            'table': table.target_name,
            'status': 'extracted',
            'write_mode': write_mode,
        })

        return ExtractResult(df=df, write_mode=write_mode, last_point_value=last_point_value)
