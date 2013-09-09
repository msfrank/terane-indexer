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

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.Matchers._
import com.syntaxjockey.terane.indexer.bier.Matchers.FindPosting
import akka.event.LoggingReceive

/**
 * Matches terms which are the intersection of all child matchers.
 *
 * @param children
 */
case class AndMatcher(children: Set[Matchers])(implicit factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps

  implicit val timeout = Timeout(5 seconds)

  private[this] val _children = children.toList.sortWith {(m1,m2) => m1.estimateCost < m2.estimateCost }
  lazy val iterator = factory.actorOf(AndIterator.props(_children.head, _children.tail))

  def estimateCost: Long = _children.head.estimateCost

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:(%s)".format(this.getClass.getName, children.toList.map(_.hashString).sorted.mkString(","))
}

class AndIterator(scanner: Matchers, finders: List[Matchers]) extends Actor with ActorLogging {
  import AndIterator._
  import context.dispatcher

  val children = scanner +: finders
  var deferredRequests: List[ActorRef] = List.empty

  def receive = LoggingReceive {

    case NextPosting if deferredRequests.isEmpty =>
      self ! ScanToNext
      deferredRequests = deferredRequests :+ sender

    case NextPosting =>
      deferredRequests = deferredRequests :+ sender

    case ScanToNext =>
      val nextPosting = for {
        matchResult <- scanner.nextPosting
        nextPosting <- matchPosting(matchResult)
      } yield nextPosting
      nextPosting.pipeTo(self)

    case matchResult: MatchResult =>
      deferredRequests.head ! matchResult
      deferredRequests = deferredRequests.tail
      if (!deferredRequests.isEmpty)
        self ! ScanToNext

    case FindPosting(id) =>
      findMatching(children.map(_.findPosting(id))).pipeTo(sender)
  }

  def matchPosting(matchResult: MatchResult) = matchResult match {
    case Left(NoMoreMatches) =>
      Future.successful(Left(NoMoreMatches))
    case Right(Posting(id, _)) =>
      findMatching(finders.map(_.findPosting(id))).map {
        case right @ Right(findResult) =>
          right
        case Left(NoMoreMatches) =>
          ScanToNext
      }
  }

  def findMatching(findFutures: List[Future[MatchResult]]): Future[MatchResult] = {
    // convert List[Future[MatchResult]] to Future[List[MatchResult]]
    Future.sequence(findFutures).map[MatchResult] { matchResults: List[MatchResult] =>
      var noMatch = false
      // if any of the finders return NoMoreMatches, then the intersection failed, otherwise return the posting
      for (matchResult <- matchResults if !noMatch) { if (matchResult.isLeft) noMatch = true }
      if (noMatch) Left(NoMoreMatches) else matchResults.head
    }
  }

  override def postStop() {
    children.foreach(_.close())
  }
}

object AndIterator {

  def props(scanner: Matchers, finders: List[Matchers]) = Props(classOf[AndIterator], scanner, finders)

  case object ScanToNext
}
