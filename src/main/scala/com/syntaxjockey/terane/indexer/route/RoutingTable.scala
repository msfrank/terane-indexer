/*
 *
 * Copyright (c) 2010-2014 Michael Frank <msfrank@syntaxjockey.com>
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
 *
 */

package com.syntaxjockey.terane.indexer.route

import com.syntaxjockey.terane.indexer.{SourceRef, SinkMap}

/**
 *
 */
class RoutingTable(routes: Vector[RouteContext]) {

  /**
   *
   */
  def process(source: SourceRef, sourceEvent: SourceEvent, sinks: SinkMap): Boolean = {
    routes.map { route =>
      if (route.process(source, sourceEvent, sinks))
        return true
    }
    false
  }

  /**
   * Append a route to the end of the routing table.
   */
  def append(route: RouteContext): RoutingTable = new RoutingTable(routes :+ route)

  /**
   * Insert a route in the specified position of the routing table. 
   */
  def insert(position: Int, route: RouteContext): RoutingTable = {
    if (position == 0)
      new RoutingTable(route +: routes)
    else if (position == routes.length - 1)
      new RoutingTable(routes :+ route)
    else if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    else {
      val before = routes.take(position)
      val after = routes.drop(position)
      new RoutingTable((before :+ route) ++ after)
    }
  }

  /**
   * Delete the route at the specified position in the routing table.
   */
  def delete(position: Int): RoutingTable = {
    if (position == 0)
      new RoutingTable(routes.drop(1))
    else if (position == routes.length - 1)
      new RoutingTable(routes.dropRight(1))
    else if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    else {
      val before = routes.take(position)
      val after = routes.drop(position).drop(1)
      new RoutingTable(before ++ after)
    }
  }

  /**
   * Replace the route at the specified position in the routing table with the specified route.
   */
  def replace(position: Int, route: RouteContext): RoutingTable = {
    if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    new RoutingTable(routes.updated(position, route))
  }

  /**
   * Delete all routes in the routing table.
   */
  def flush(): RoutingTable = RoutingTable.empty
}

object RoutingTable {

  val empty = new RoutingTable(Vector.empty)

}
