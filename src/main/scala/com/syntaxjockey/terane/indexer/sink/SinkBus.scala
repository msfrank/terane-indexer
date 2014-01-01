/*
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

package com.syntaxjockey.terane.indexer.sink

import akka.event.{SubchannelClassification, ActorEventBus}
import akka.util.Subclassification
import akka.actor.ActorRef

/**
 * SinkBus is an event stream which is private to each individual CassandraSink, and
 * is used for broadcast communication between sink components.
 */
class SinkBus extends ActorEventBus with SubchannelClassification {
  type Event = SinkEvent
  type Classifier = Class[_]

  protected implicit val subclassification = new Subclassification[Class[_]] {
    def isEqual(x: Class[_], y: Class[_]) = x == y
    def isSubclass(x: Class[_], y: Class[_]) = y isAssignableFrom x
  }

  protected def classify(event: SinkEvent): Class[_] = event.getClass

  protected def publish(event: SinkEvent, subscriber: ActorRef) { subscriber ! event }
}

/* Messages published on the SinkBus must implement SinkEvent trait */
trait SinkEvent