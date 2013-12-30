/**
 * Copyright 2013 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.{Keyspace, Cluster}

trait CassandraKeyspaceOperations {

  implicit val cluster: Cluster

  /**
   * get the Keyspace with the specified name.
   */
  def getKeyspace(keyspaceName: String): Keyspace = cluster.getKeyspace(keyspaceName)

  /**
   * create a Keyspace with the specified name.
   */
  def createKeyspace(keyspaceName: String): Keyspace = {
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
    cluster.getKeyspace(keyspaceName)
  }

  /**
   * Return true if the keyspace exists, otherwise false.
   */
  def keyspaceExists(keyspaceName: String): Boolean = {
    try {
      val ksDef = cluster.describeKeyspace(keyspaceName)
      true
    } catch {
      case ex: Throwable =>
      false
    }
  }
}
