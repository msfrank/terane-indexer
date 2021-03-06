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
import com.syntaxjockey.terane.indexer.bier.Matchers._
import com.syntaxjockey.terane.indexer.bier.matchers.OrIterator._
import com.syntaxjockey.terane.indexer.bier.Matchers.MatchResult

/**
 * Matches terms which are the union of all child matchers.
 *
 * @param children
 */
case class OrMatcher(children: Set[Matchers])(implicit factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps

  implicit val timeout = Timeout(5 seconds)

  private[this] val _children = children.toList.sortWith {(m1,m2) => m1.estimateCost < m2.estimateCost }
  lazy val iterator = factory.actorOf(OrIterator.props(_children))

  def estimateCost: Long = children.foldLeft(0L) {(acc: Long, m: Matchers) => acc + m.estimateCost }

  def nextPosting = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID) = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:(%s)".format(this.getClass.getName, children.toList.map(_.hashString).sorted.mkString(","))
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
      goto(Active) using Active(childStates, inProgress = true, None, deferredRequests)

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

    /**
     * The next posting has been requested, and there are no active children.  Send NoMoreMatches
     * to the requestor.
     */
    case Event(NextPosting, Active(childStates, inProgress, last, deferredRequests)) if childStates.isEmpty && deferredRequests.isEmpty =>
      sender ! Left(NoMoreMatches)
      stay() using Active(childStates, inProgress, last, deferredRequests)

    /**
     * The next posting has been requested, and the FSM is in progress.  Add the request to the tail
     * of the requests list.
     */
    case Event(NextPosting, Active(childStates, true, last, deferredRequests)) =>
      stay() using Active(childStates, inProgress = true, last, deferredRequests :+ sender)

    /**
     * The next posting has been requested, and the FSM is not in progress.  If the current
     * smallest posting has not already been seen, then send it to the first requestor and remove
     * the request from our pending requests list.  Next, update the state to note that we have
     * consumed the smallest posting.  Lastly, request the next posting and mark the FSM as in
     * progress.
     */
    case Event(NextPosting, Active(childStates, false, last, deferredRequests)) =>
      val updatedRequests = deferredRequests :+ sender
      // send smallest posting
      val smallest = getSmallestPosting(childStates)
      val smallestId = smallest.result.get.id
      if (last.isEmpty || !smallestId.equals(last.get))
        updatedRequests.head ! Right(smallest.result.get)
      val updatedStates = childStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, Some(smallestId), updatedRequests.tail)

    /**
     * The currently updating child has no more postings, and all other children have already
     * been exhausted.  Send NoMoreMatches to all pending requestors, remove the child from the
     * map of child states, and mark the FSM as not in progress.
     */
    case Event(ChildState(child, None), Active(childStates, _, last, deferredRequests)) if childStates.size == 1 =>
      deferredRequests.foreach(_ ! Left(NoMoreMatches))
      stay() using Active(childStates - child, inProgress = false, last, List.empty)

    /**
     * The currently updating child has no more postings, and there are no pending requests.
     * Update our map of child states by removing the exhausted child, and mark the FSM as not
     * in progress.
     */
    case Event(ChildState(child, None), Active(childStates, _, last, deferredRequests)) if deferredRequests.isEmpty =>
      stay() using Active(childStates - child, inProgress = false, last, deferredRequests)

    /**
     * The currently updating child has no more postings, and there are pending requests.  First
     * update our map of child states by removing the exhausted child.  Next, if the current
     * smallest posting has not already been seen, then send it to the first requestor and remove
     * the request from our pending requests list.  Lastly, request the next posting, update our
     * state again to note that we have consumed the smallest posting, and mark the FSM as in progress.
     */
    case Event(ChildState(child, None), Active(childStates, _, last, deferredRequests)) if !deferredRequests.isEmpty =>
      // remove child
      var updatedStates = childStates - child
      // send smallest posting
      val smallest = getSmallestPosting(updatedStates)
      val smallestId = smallest.result.get.id
      val updatedRequests = if (last.isEmpty || !smallestId.equals(last.get)) {
        deferredRequests.head ! Right(smallest.result.get)
        deferredRequests.tail
      } else deferredRequests
      updatedStates = updatedStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, last, updatedRequests)

    /**
     * we have received the child update and there are no pending requests.  Update our
     * map of child states, and mark the FSM as not in progress anymore.
     */
    case Event(childState @ ChildState(child, Some(posting)), Active(childStates, _, last, deferredRequests)) if deferredRequests.isEmpty =>
      stay() using Active(childStates ++ Map(childState.child -> childState), inProgress = false, last, deferredRequests)

    /**
     * we have received the child update and there are pending requests.  First, update our
     * map of child states.  Next, if the current smallest posting has not already been seen,
     * then send it to the first requestor and remove the request from our pending requests
     * list.  Lastly, request the next posting, update our state again to note that we have
     * consumed the smallest posting, and mark the FSM as in progress.
     */
    case Event(childState @ ChildState(child, Some(posting)), Active(childStates, _, last, deferredRequests)) if !deferredRequests.isEmpty =>
      // update child
      var updatedStates = childStates ++ Map(childState.child -> childState)
      // send smallest posting
      val smallest = getSmallestPosting(updatedStates)
      val smallestId = smallest.result.get.id
      val updatedRequests = if (last.isEmpty || !smallestId.equals(last.get)) {
        deferredRequests.head ! Right(smallest.result.get)
        deferredRequests.tail
      } else deferredRequests
      updatedStates = updatedStates ++ Map(smallest.child -> ChildState(smallest.child, None))
      // get next posting
      getChildPosting(smallest.child).pipeTo(self)
      stay() using Active(updatedStates, inProgress = true, Some(smallestId), updatedRequests)

    /* find the specified posting in any of the child matchers */
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

  def props(children: List[Matchers]) = Props(classOf[OrIterator], children)

  case class ChildState(child: Matchers, result: Option[Posting])

  sealed trait State
  case object Initializing extends State
  case object Active extends State

  sealed trait Data
  case class Initializing(childStates: Map[Matchers,ChildState], numWaiting: Int, deferredRequests: List[ActorRef]) extends Data
  case class Active(childStates: Map[Matchers,ChildState], inProgress: Boolean, last: Option[UUID], deferredRequests: List[ActorRef]) extends Data
}
