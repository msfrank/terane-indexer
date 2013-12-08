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

package com.syntaxjockey.terane.indexer

import akka.actor.ActorSystem

/**
 * Indexer application entry point
 */
object IndexerApp extends App {

  /* get the runtime configuration */
  val config = IndexerConfig.config

  /* start the actor system */
  val system = ActorSystem("terane-indexer", config)

  val settings = IndexerConfig(system).settings

  /* start the supervisor */
  val supervisor = system.actorOf(ClusterSupervisor.props(), "cluster-supervisor")

  /* shut down cleanly */
  sys.addShutdownHook({
    system.shutdown()
  })
}
