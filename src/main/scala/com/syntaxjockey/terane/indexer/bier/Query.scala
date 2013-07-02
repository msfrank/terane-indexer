package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.EventRouter.{DescribeQueryResponse, GetEvents, DescribeQuery, CreateQuery}
import java.util.UUID
import org.joda.time.{DateTimeZone, DateTime}
import akka.actor.{FSM, Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.bier.Query.{State, Data}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store

class Query(id: UUID, createQuery: CreateQuery, store: Store) extends Actor with ActorLogging with FSM[State,Data] {
  import Query._

  val created = DateTime.now(DateTimeZone.UTC)

  /* step 0: transform the query string into a Matchers tree */
  // FIXME: pass current fields map
  val unlinkedQuery = buildQueryMatchers(createQuery.query, Map.empty)

  /* start the FSM */
  startWith(WaitingForMatcherEstimates, QueryData(unlinkedQuery))
  initialize()

  /* step 2: get term estimates and possibly reorder the query */
  when(WaitingForMatcherEstimates) {
    case Event(describeQuery: DescribeQuery, _) =>
      sender ! DescribeQueryResponse(id, created, "Waiting for matcher estimates")
      stay()
  }

  /* step 3: replace all TermMatcher instances with Terms */

  /* step 4: service client requests */
  when(Active) {
    case Event(GetEvents, _) =>
      stay()
    case Event(describeQuery: DescribeQuery, _) =>
      sender ! DescribeQueryResponse(id, created, "Active")
      stay()
  }

//  /**
//   * Return at most the specified number of posting ids.  This method may return
//   * less if the matcher has less than maxPostings.
//   *
//   */
//  def getPostings(maxPostings: Int): List[UUID] = {
//    var postings = List.empty[UUID]
//    var n = 0
//    while (n < maxPostings) {
//      for (postingOrDone <- getNextPosting) {
//        postingOrDone match {
//          case Left(_) =>
//            return postings
//          case Right(Posting(id, _)) =>
//            postings = postings :+ id
//        }
//      }
//      n += 1
//    }
//    postings
//  }
}

object Query {
  import TickleParser._
  import com.syntaxjockey.terane.indexer.bier.matchers._

  /**
   * Given a raw query string, produce Matchers tree.
   *
   * @param qs
   * @return
   */
  def buildQueryMatchers(qs: String, fields: Map[FieldIdentifier,Field]): Option[Matchers] = {
    parseSubjectOrGroup(parseQueryString(qs).query, fields)
  }

  /**
   * Recursively descend the syntax tree and build a Matchers tree.
   *
   * @param subjectOrGroup
   * @return
   */
  def parseSubjectOrGroup(subjectOrGroup: SubjectOrGroup, fields: Map[FieldIdentifier,Field]): Option[Matchers] = {
    subjectOrGroup match {
      case Left(Subject(value, fieldName, fieldType)) =>
        val _fieldName = fieldName.getOrElse("message")
        val _fieldType = fieldType.getOrElse(EventValueType.TEXT)
        fields.get(FieldIdentifier(_fieldName, _fieldType)) match {
          case Some(field) =>
            Some(new TermMatcher[String](field, value))
          case missing =>
            None
        }
      case Right(AndGroup(children)) =>
        val andMatcher = new AndMatcher(children map { child => parseSubjectOrGroup(child, fields) } flatten)
        if (andMatcher.children.isEmpty) None else Some(andMatcher)
      case Right(OrGroup(children)) =>
        val orMatcher = new OrMatcher(children map { child => parseSubjectOrGroup(child, fields) } flatten)
        if (orMatcher.children.isEmpty) None else Some(orMatcher)
      case Right(unknown) =>
        throw new Exception("unknown group type " + unknown.toString)
    }
  }

  /* our FSM states */
  sealed trait State
  case object WaitingForMatcherEstimates extends State
  case object Active extends State

  sealed trait Data
  case class QueryData(matchers: Option[Matchers]) extends Data
}
