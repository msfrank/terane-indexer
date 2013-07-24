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

/**
 * Matches terms which are the intersection of all child matchers.
 *
 * @param children
 */
case class AndMatcher(children: List[Matchers])(implicit factory: ActorRefFactory) extends Matchers {

  implicit val timeout = Timeout(5 seconds)

  val iterator = factory.actorOf(Props(new AndIterator(children.head, children.tail)))

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }
}

class AndIterator(scanner: Matchers, finders: List[Matchers]) extends Actor with ActorLogging {
  import AndIterator._
  import context.dispatcher

  val children = scanner +: finders
  var deferredRequests: List[ActorRef] = List.empty

  def receive = {

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
  case object ScanToNext
}
