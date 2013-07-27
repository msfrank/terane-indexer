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

  val _childStates: Map[Matchers,ChildState] = children.map { child =>
    getChildPosting(child).pipeTo(self)
    child -> ChildState(child, None)
  }.toMap

  startWith(Initializing, Initializing(_childStates, children.length, List.empty))

  when(Initializing) {

    case Event(childState: ChildState, Initializing(childStates, numWaiting, deferredRequests)) if numWaiting == 1 =>
      self ! childState
      goto(Active) using Active(childStates, inProgress = true, deferredRequests)

    case Event(childState: ChildState, Initializing(childStates, numWaiting, deferredRequests)) if numWaiting > 1 =>
      val currState = if (childState.result.isDefined) childStates ++ Map(childState.child -> childState) else childStates
      stay() using Initializing(currState, numWaiting - 1, deferredRequests)

    case Event(ChildState(child, None), Initializing(childStates, numWaiting, deferredRequests)) if numWaiting > 1 =>
      stay() using Initializing(childStates, numWaiting - 1, deferredRequests)

    case Event(NextPosting, Initializing(childStates, numWaiting, deferredRequests)) =>
      stay() using Initializing(childStates, numWaiting, deferredRequests :+ sender)

    case Event(FindPosting(id), initializing: Initializing) =>
      findPosting(id).pipeTo(sender)
      stay()
  }

  when(Active) {

    case Event(NextPosting, Active(childStates, _, _)) if childStates.isEmpty =>
      sender ! Left(NoMoreMatches)
      stay()

    case Event(NextPosting, Active(childStates, true, deferredRequests)) =>
      stay() using Active(childStates, inProgress = true, deferredRequests :+ sender)

    case Event(NextPosting, Active(childStates, false, deferredRequests)) =>
      val updatedRequests = deferredRequests :+ sender
      // send smallest posting
      val smallest = getSmallestPosting(childStates)
      updatedRequests.head ! Right(smallest.result.get)
      val updatedStates = childStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, updatedRequests.tail)

    case Event(ChildState(_, None), Active(childStates, _, deferredRequests)) if childStates.size <= 1 =>
      deferredRequests.foreach(_ ! Left(NoMoreMatches))
      stay() using Active(childStates, inProgress = false, List.empty)

    case Event(ChildState(child, None), Active(childStates, _, deferredRequests)) if deferredRequests.isEmpty =>
      stay() using Active(childStates - child, inProgress = false, List.empty)

    case Event(ChildState(child, None), Active(childStates, _, deferredRequests)) =>
      // remove child
      var updatedStates = childStates - child
      // send smallest posting
      val smallest = getSmallestPosting(updatedStates)
      deferredRequests.head ! Right(smallest.result.get)
      updatedStates = updatedStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, List.empty)

    case Event(childState @ ChildState(child, Some(posting)), Active(childStates, _, deferredRequests)) if deferredRequests.isEmpty =>
      stay() using Active(childStates ++ Map(childState.child -> childState), inProgress = false, deferredRequests)

    case Event(childState @ ChildState(child, Some(posting)), Active(childStates, _, deferredRequests)) =>
      // update child
      var updatedStates = childStates ++ Map(childState.child -> childState)
      // send smallest posting
      val smallest = getSmallestPosting(updatedStates)
      deferredRequests.head ! Right(smallest.result.get)
      updatedStates = updatedStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, deferredRequests.tail)

    case Event(FindPosting(id), _) =>
      findPosting(id).pipeTo(sender)
      stay()
  }

  initialize()

  /**
   *
   * @param childStates
   * @return
   */
  def getSmallestPosting(childStates: Map[Matchers,ChildState]): ChildState = {
    val sorted = childStates.values.toSeq.sortWith {
      case (ChildState(leftChild, Some(leftPosting)), ChildState(rightChild, Some(rightPosting))) =>
        if (leftPosting.id.toString < rightPosting.id.toString) true else false
    }
    sorted.head
  }

  /**
   *
   * @param child
   * @return
   */
  def getChildPosting(child: Matchers): Future[ChildState] = child.nextPosting.map {
    case Left(NoMoreMatches) =>
      ChildState(child, None)
    case Right(posting: Posting) =>
      ChildState(child, Some(posting))
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
      var finalResult: MatchResult = Left(NoMoreMatches)
      // if any of the finders do not return NoMoreMatches, then the union succeeded, otherwise return NoMoreMatches
      for (matchResult <- matchResults if finalResult.isLeft) { if (matchResult.isRight) finalResult = matchResult }
      finalResult
    }
  }

  override def postStop() {
    children.foreach(_.close())
  }
}

object OrIterator {
  case class ChildState(child: Matchers, result: Option[Posting])

  sealed trait State
  case object Initializing extends State
  case object Active extends State

  sealed trait Data
  case class Initializing(childStates: Map[Matchers,ChildState], numWaiting: Int, deferredRequests: List[ActorRef]) extends Data
  case class Active(childStates: Map[Matchers,ChildState], inProgress: Boolean, deferredRequests: List[ActorRef]) extends Data
}
