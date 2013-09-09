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
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers
import akka.event.LoggingReceive

/**
 * NotMatcher matches terms which are the negation of the child matcher.  Negation
 * is implemented by iterating through the specified source matcher and emitting each
 * event which is not present in the specified filter.
 */
case class NotMatcher(source: Matchers, filter: Matchers)(implicit factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps
  import Matchers._

  implicit val timeout = Timeout(5 seconds)

  lazy val iterator = factory.actorOf(NotIterator.props(source, filter))

  def estimateCost: Long = source.estimateCost - filter.estimateCost

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:(%s,%s)".format(this.getClass.getName, source.hashString, filter.hashString)
}

class NotIterator(source: Matchers, filter: Matchers) extends Actor with ActorLogging {
  import AndIterator._
  import Matchers._
  import context.dispatcher

  var deferredRequests: List[ActorRef] = List.empty

  def receive = LoggingReceive {

    case NextPosting if deferredRequests.isEmpty =>
      self ! ScanToNext
      deferredRequests = deferredRequests :+ sender

    case NextPosting =>
      deferredRequests = deferredRequests :+ sender

    case ScanToNext =>
      // get the next posting from the source
      source.nextPosting.flatMap {
        // if the source has no more postings, then stop
        case Left(NoMoreMatches) =>
          Future.successful(Left(NoMoreMatches))
        // otherwise look for the posting in the filter
        case matchResult @ Right(Posting(id, _)) =>
          filter.findPosting(id).map {
            // if the filter doesn't have the posting, then emit the posting
            case Left(NoMoreMatches) =>
              matchResult
            // otherwise process the next posting from the source
            case Right(posting: Posting) =>
              ScanToNext
          }
      }.pipeTo(self)

    case matchResult: MatchResult =>
      deferredRequests.head ! matchResult
      deferredRequests = deferredRequests.tail
      if (!deferredRequests.isEmpty)
        self ! ScanToNext

    case FindPosting(id) =>
      val f1 = source.findPosting(id)
      val f2 = filter.findPosting(id)
      // convert List[Future[MatchResult]] to Future[List[MatchResult]]
      Future.sequence(List(f1, f2)).map[MatchResult] {
        // if f1 returns a posting, and f2 returns NoMoreMatches, then return the posting
        case List(matches @ Right(Posting(_,_)), Left(NoMoreMatches)) =>
          matches
        // otherwise return NoMoreMatches
        case _ =>
          Left(NoMoreMatches)
      }.pipeTo(sender)
  }
}

object NotIterator {
  def props(source: Matchers, filter: Matchers) = Props(classOf[NotIterator], source, filter)

  case object ScanToNext
}
