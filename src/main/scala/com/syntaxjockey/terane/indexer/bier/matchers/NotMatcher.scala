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

package com.syntaxjockey.terane.indexer.bier.matchers

import akka.actor.{Props, Actor, ActorLogging, ActorRefFactory}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers

/**
 * NotMatcher matches terms which are the negation of the child matchers.
 */
case class NotMatcher(children: List[Matchers])(implicit factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps
  import Matchers._

  implicit val timeout = Timeout(5 seconds)

  private[this] val _children = children.sortWith {(m1,m2) => m1.estimateCost < m2.estimateCost }
  lazy val iterator = factory.actorOf(NotIterator.props(_children.head, _children.tail))

  def estimateCost: Long = _children.head.estimateCost

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }
}

class NotIterator(scanner: Matchers, finders: List[Matchers]) extends Actor with ActorLogging {
  import Matchers._

  def receive = {
    case NextPosting => sender ! Left(NoMoreMatches)
    case FindPosting(id) => sender ! Left(NoMoreMatches)
  }
}

object NotIterator {
  def props(scanner: Matchers, finders: List[Matchers]) = Props(classOf[NotIterator], scanner, finders)
}
