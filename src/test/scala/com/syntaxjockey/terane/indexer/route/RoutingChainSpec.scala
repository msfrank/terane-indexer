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

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class RoutingChainSpec extends WordSpec with MustMatchers {

  "A RoutingChain" must {

    "append a route" in {
      val initial = new RoutingChain(Vector.empty)
      val updated = initial.append(new RouteContext(None, Vector(MatchesAll), DropAction))
      updated.routes must be === Vector(new RouteContext(None, Vector(MatchesAll), DropAction))
    }

    "insert a route" in {
      val initial = new RoutingChain(Vector(
        new RouteContext(None, Vector(MatchesTag("foo")), DropAction),
        new RouteContext(None, Vector(MatchesAll), StoreAllAction)
      ))
      val updated = initial.insert(1, new RouteContext(None, Vector(MatchesTag("bar")), StoreAction(Vector("sink"))))
      updated.routes must be === Vector(
        new RouteContext(None, Vector(MatchesTag("foo")), DropAction),
        new RouteContext(None, Vector(MatchesTag("bar")), StoreAction(Vector("sink"))),
        new RouteContext(None, Vector(MatchesAll), StoreAllAction)
      )
    }

    "replace a route" in {
      val initial = new RoutingChain(Vector(new RouteContext(None, Vector(MatchesAll), StoreAllAction)))
      val updated = initial.replace(0, new RouteContext(None, Vector(MatchesTag("foo")), DropAction))
      updated.routes must be === Vector(new RouteContext(None, Vector(MatchesTag("foo")), DropAction))
    }

    "delete a route" in {
      val initial = new RoutingChain(Vector(
        new RouteContext(None, Vector(MatchesTag("foo")), DropAction),
        new RouteContext(None, Vector(MatchesAll), StoreAllAction)
      ))
      val updated = initial.delete(0)
      updated.routes must be === Vector(new RouteContext(None, Vector(MatchesAll), StoreAllAction))
    }

    "delete all routes" in {
      val initial = new RoutingChain(Vector(
        new RouteContext(None, Vector(MatchesTag("foo")), DropAction),
        new RouteContext(None, Vector(MatchesTag("bar")), DropAction),
        new RouteContext(None, Vector(MatchesAll), StoreAllAction)
      ))
      val updated = initial.flush()
      updated.routes must be === Vector.empty
    }
  }
}
