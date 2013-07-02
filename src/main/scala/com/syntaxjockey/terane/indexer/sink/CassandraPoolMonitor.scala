package com.syntaxjockey.terane.indexer.sink

import com.netflix.astyanax.connectionpool.{HostConnectionPool, HostStats, Host, ConnectionPoolMonitor}
import scala.collection.JavaConversions._
import java.util
import akka.event.EventStream

class CassandraPoolMonitor(eventStream: EventStream) extends ConnectionPoolMonitor {

  def incOperationFailure(host: Host, reason: Exception) {}

  def getOperationFailureCount: Long = 0

  def incFailover(host: Host, reason: Exception) {}

  def getFailoverCount: Long = 0

  def incOperationSuccess(host: Host, latency: Long) {}

  def getOperationSuccessCount: Long = 0

  def incConnectionCreated(host: Host) {}

  def getConnectionCreatedCount: Long = 0

  def incConnectionClosed(host: Host, reason: Exception) {}

  def getConnectionClosedCount: Long = 0

  def incConnectionCreateFailed(host: Host, reason: Exception) {}

  def getConnectionCreateFailedCount: Long = 0

  def incConnectionBorrowed(host: Host, delay: Long) {}

  def getConnectionBorrowedCount: Long = 0

  def incConnectionReturned(host: Host) {}

  def getConnectionReturnedCount: Long = 0

  def getPoolExhaustedTimeoutCount: Long = 0

  def getOperationTimeoutCount: Long = 0

  def getSocketTimeoutCount: Long = 0

  def getUnknownErrorCount: Long = 0

  def getBadRequestCount: Long = 0

  def getNoHostCount: Long = 0

  def notFoundCount(): Long = 0

  def getInterruptedCount: Long = 0

  def getHostCount: Long = 0

  def getHostAddedCount: Long = 0

  def getHostRemovedCount: Long = 0

  def getHostDownCount: Long = 0

  def onHostAdded(host: Host, pool: HostConnectionPool[_]) {}

  def onHostRemoved(host: Host) {}

  def onHostDown(host: Host, reason: Exception) {}

  def onHostReactivated(host: Host, pool: HostConnectionPool[_]) {}

  def getHostStats: util.Map[Host, HostStats] = Map.empty[Host,HostStats]
}
