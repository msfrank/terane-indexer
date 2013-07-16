package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.{Keyspace, Cluster}
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.ddl.SchemaChangeResult

trait CassandraKeyspaceOperations {

  implicit val cluster: Cluster

  /**
   * get the Keyspace with the specified name.
   *
   * @param keyspaceName
   * @return
   */
  def getKeyspace(keyspaceName: String): Keyspace = cluster.getKeyspace(keyspaceName)

  /**
   * create a Keyspace with the specified name.
   *
   * @param keyspaceName
   * @return
   */
  def createKeyspace(keyspaceName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,String]()
    opts.put("replication_factor", "1")
    val ksDef = cluster.makeKeyspaceDefinition()
      .setName(keyspaceName)
      .setStrategyClass("SimpleStrategy")
      .setStrategyOptions(opts)
      .addColumnFamily(cluster.makeColumnFamilyDefinition()
      .setName("events")
      .setKeyValidationClass("UUIDType")
      .setComparatorType("UTF8Type"))
      .addColumnFamily(cluster.makeColumnFamilyDefinition()
      .setName("meta")
      .setKeyValidationClass("UUIDType")
      .setComparatorType("UTF8Type"))
    cluster.addKeyspace(ksDef)
  }

}
