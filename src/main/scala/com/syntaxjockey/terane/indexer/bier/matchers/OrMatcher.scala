package com.syntaxjockey.terane.indexer.bier.matchers

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.Matchers._
import com.syntaxjockey.terane.indexer.bier.matchers.OrIterator._
import com.syntaxjockey.terane.indexer.bier.Matchers.MatchResult

/**
 * Matches terms which are the union of all child matchers.
 *
 * @param children
 */
case class OrMatcher(children: List[Matchers])(implicit factory: ActorRefFactory) extends Matchers {

  implicit val timeout = Timeout(5 seconds)

  val iterator = factory.actorOf(Props(new OrIterator(children)))

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }
}

class OrIterator(children: List[Matchers]) extends Actor with ActorLogging with LoggingFSM[State,Data] {
  import context.dispatcher

  startWith(CacheEmpty, CacheEmpty(children, List.empty))

  when(CacheFull) {

    case Event(NextPosting, CacheFull(activeMatchers, cache)) =>
      sender ! Right(cache.head)
      val leftover = cache.tail
      if (leftover.isEmpty) {
        nextBatch(activeMatchers).pipeTo(self)
        goto(CacheEmpty) using CacheEmpty(activeMatchers, List.empty)
      } else stay() using CacheFull(activeMatchers, leftover)

    case Event(FindPosting(id), _) =>
      findPosting(id).pipeTo(sender)
      stay()
  }

  when(CacheEmpty) {

    case Event(NextPosting, CacheEmpty(activeMatchers, _)) if activeMatchers.isEmpty =>
      sender ! Left(NoMoreMatches)
      stay()

    case Event(NextPosting, CacheEmpty(activeMatchers, deferredRequests)) =>
      if (deferredRequests.isEmpty)
        nextBatch(activeMatchers).pipeTo(self)
      stay() using CacheEmpty(activeMatchers, deferredRequests :+ sender)

    case Event(ChildResults(results), CacheEmpty(_, deferredRequests)) if results.isEmpty =>
      deferredRequests.foreach { deferred: ActorRef => deferred ! Left(NoMoreMatches) }
      stay() using CacheEmpty(List.empty, List.empty)

    case Event(ChildResults(results), CacheEmpty(_, deferredRequests)) =>
      // remove matchers from activeMatchers if they returned NoMoreMatches
      val activeMatchers: List[Matchers] = results.map { result => result match {
        case ChildResult(_, Left(NoMoreMatches)) =>
          None
        case ChildResult(child, Right(posting)) =>
          Some(child)
      }}.flatten
      // remove any Left(NoMoreMatches) results
      var postings: List[Posting] = results.map { result => result match {
        case ChildResult(_, Left(NoMoreMatches)) =>
          None
        case ChildResult(_, Right(posting)) =>
          Some(posting)
      }}.flatten
      // if we have more results then pending requests, then fulfill the requests and move back to CacheFull
      if (postings.length > deferredRequests.length) {
        deferredRequests.foreach { request =>
          request ! Right(postings.head)
          postings = postings.tail
        }
        goto(CacheFull) using CacheFull(activeMatchers, postings)
      }
      // otherwise fulfill as many as we can, request a new batch, and stay in the CacheEmpty state
      else {
        var stillDeferred: List[ActorRef] = deferredRequests
        postings.foreach { posting =>
          stillDeferred.head ! Right(posting)
          stillDeferred = stillDeferred.tail
        }
        nextBatch(activeMatchers).pipeTo(self)
        stay() using CacheEmpty(activeMatchers, stillDeferred)
      }

    case Event(FindPosting(id), _) =>
      findPosting(id).pipeTo(sender)
      stay()
  }

  initialize()

  /**
   *
   * @param activeMatchers
   * @return
   */
  def nextBatch(activeMatchers: List[Matchers]): Future[ChildResults] = {
    val futures = activeMatchers.map { matcher =>
      matcher.nextPosting.map(result => ChildResult(matcher, result))
    }
    // convert List[Future[ChildResult]] to Future[List[ChildResult]]
    Future.sequence(futures).map(ChildResults)
  }

  /**
   *
   * @param id
   * @return
   */
  def findPosting(id: UUID): Future[MatchResult] = {
    // generate a list of futures
    val futures = children.map(_.findPosting(id))
    // convert List[Future[MatchResult]] to Future[List[MatchResult]]
    Future.sequence(futures).map { matchResults: List[MatchResult] =>
      var noMatch = true
      // if any of the finders do not return NoMoreMatches, then the union succeeded, otherwise return NoMoreMatches
      for (matchResult <- matchResults if noMatch) { if (matchResult.isRight) noMatch = false }
      if (noMatch) Left(NoMoreMatches) else matchResults.head
    }
  }

  override def postStop() {
    children.foreach(_.close())
  }
}

object OrIterator {
  case class ChildResult(child: Matchers, result: MatchResult)
  case class ChildResults(results: List[ChildResult])

  sealed trait State
  case object CacheEmpty extends State
  case object CacheFull extends State

  sealed trait Data
  case class CacheEmpty(activeMatchers: List[Matchers], deferredRequests: List[ActorRef]) extends Data
  case class CacheFull(activeMatchers: List[Matchers], cache: List[Posting]) extends Data
}
