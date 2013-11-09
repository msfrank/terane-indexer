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
import akka.event.LoggingReceive
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.Matchers._
import com.syntaxjockey.terane.indexer.bier.Matchers.FindPosting
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata

/**
 * Matches terms which are the intersection of all child matchers of the phrase, and
 * whose term positions align.
 */
case class PhraseMatcher(phrase: Seq[Matchers])(implicit factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps

  implicit val timeout = Timeout(5 seconds)

  private[this] val children = phrase.sortWith {(m1,m2) => m1.estimateCost < m2.estimateCost }.toList
  lazy val iterator = factory.actorOf(PhraseIterator.props(children.head, children.tail, phrase))

  def estimateCost: Long = phrase.head.estimateCost

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:(%s)".format(this.getClass.getName, children.map(_.hashString).sorted.mkString(","))
}

/**
 * PhraseIterator operates the same way as AndInterator, but has a post-processing
 * step where it compares the positions of each term to the phrase.
 */
class PhraseIterator(scanner: Matchers, finders: List[Matchers], phrase: Seq[Matchers]) extends Actor with ActorLogging {
  import PhraseIterator._
  import context.dispatcher

  val phrasePositions: Map[Matchers,Int] = 0.until(phrase.length).map(position => phrase(position) -> position).toMap

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
      val futures = finders.map { matcher =>
        matcher.findPosting(id).map(WrappedMatchResult(_, matcher))
      }
      findMatching(futures).pipeTo(sender)
  }

  def matchPosting(matchResult: MatchResult) = matchResult match {
    case Left(NoMoreMatches) =>
      Future.successful(Left(NoMoreMatches))
    case Right(Posting(id, _)) =>
      val futures = finders.map { matcher =>
        matcher.findPosting(id).map(WrappedMatchResult(_, matcher))
      }
      findMatching(futures).map {
        case right @ Right(findResult) =>
          right
        case Left(NoMoreMatches) =>
          ScanToNext
      }
  }

  def findMatching(findFutures: List[Future[WrappedMatchResult]]): Future[MatchResult] = {
    // convert List[Future[WrappedMatchResult]] to Future[List[WrappedMatchResult]]
    Future.sequence(findFutures).map[MatchResult] { matchResults: List[WrappedMatchResult] =>
      var noMatch = false
      // if any of the finders return NoMoreMatches, then the intersection failed, otherwise return the posting
      for (matchResult <- matchResults if !noMatch) { if (matchResult.result.isLeft) noMatch = true }
      if (noMatch) Left(NoMoreMatches) else checkPositions(matchResults)
    }
  }

  /**
   *
   */
  def checkPositions(matchResults: List[WrappedMatchResult]): MatchResult = {
    // create an array of term position sets, in phrase term order
    val positions: Array[Option[Set[Int]]] = Array.fill(matchResults.length)(None)
    matchResults.foreach { case WrappedMatchResult(Right(Posting(_, metadata)), matcher) =>
      metadata match {
        case PostingMetadata(Some(_positions)) =>
          positions(phrasePositions(matcher)) = Some(_positions.toSet)
        case _ =>
          return Left(NoMoreMatches)
      }
    }
    // get the index of the posting with the smallest position set
    val leastPositionsIndex = 0.until(positions.length).foldLeft(0) {
      case (least, next) if positions(least).isEmpty =>
        positions(next) match {
          case Some(_) => next
          case _ => least
        }
      case (least, next) =>
        positions(next) match {
          case Some(p) => if (p.size < positions(least).size) next else least
          case _ => least
        }
    }
    // knowing the smallest position set, our iteration is bounded by the number of elements in positions(leastPositions)
    val leastPositions = positions(leastPositionsIndex).get
    for (position <- leastPositions) {
      def positionsMatch: Boolean = {
        for (n <- 0.until(positions.length) if n != leastPositionsIndex) {
          phrase(n) match {
            case TermPlaceholder => // do nothing
            case _ =>
              positions(n) match {
                case Some(other) =>
                  val distance = n - leastPositionsIndex
                  if (!other.contains(position + distance)) return false
                case None =>  // do nothing
              }
          }
        }
        true
      }
      if (positionsMatch) return matchResults.head.result
    }
    Left(NoMoreMatches)
  }

  override def postStop() {
    children.foreach(_.close())
  }
}

object PhraseIterator {

  def props(scanner: Matchers, finders: List[Matchers], phrase: Seq[Matchers]) = Props(classOf[PhraseIterator], scanner, finders, phrase)

  case class WrappedMatchResult(result: MatchResult, matcher: Matchers)
  case object ScanToNext
}

/**
 * Represents a wildcard term in a phrase.
 */
case object TermPlaceholder extends Matchers {
  def estimateCost = 0L
  def nextPosting = Future.failed(new NotImplementedError("TermPlaceholder doesn't implement nextPosting"))
  def findPosting(id: UUID) = Future.failed(new NotImplementedError("TermPlaceholder doesn't implement findPosting"))
  def close() {}
  def hashString: String = "%s:".format(this.getClass.getName)
}
