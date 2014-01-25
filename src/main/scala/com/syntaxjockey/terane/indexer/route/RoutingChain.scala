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
case class RoutingChain(routes: Vector[RouteContext]) {

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
   * Append a route to the end of the routing chain.
   */
  def append(route: RouteContext): RoutingChain = new RoutingChain(routes :+ route)

  /**
   * Insert a route in the specified position of the routing chain.
   */
  def insert(position: Int, route: RouteContext): RoutingChain = {
    if (position == 0)
      new RoutingChain(route +: routes)
    else if (position == routes.length - 1)
      new RoutingChain(routes :+ route)
    else if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    else {
      val before = routes.take(position)
      val after = routes.drop(position)
      new RoutingChain((before :+ route) ++ after)
    }
  }

  /**
   * Delete the route at the specified position in the routing chain.
   */
  def delete(position: Int): RoutingChain = {
    if (position == 0)
      new RoutingChain(routes.drop(1))
    else if (position == routes.length - 1)
      new RoutingChain(routes.dropRight(1))
    else if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    else {
      val before = routes.take(position)
      val after = routes.drop(position).drop(1)
      new RoutingChain(before ++ after)
    }
  }

  /**
   * Replace the route at the specified position in the routing chain with the specified route.
   */
  def replace(position: Int, route: RouteContext): RoutingChain = {
    if (position < 0 || position >= routes.length)
      throw new Exception("invalid position %s".format(position))
    new RoutingChain(routes.updated(position, route))
  }

  /**
   * Delete all routes in the routing chain.
   */
  def flush(): RoutingChain = RoutingChain.empty
}

object RoutingChain {

  val empty = new RoutingChain(Vector.empty)

}
