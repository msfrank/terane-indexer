package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.EventRouter.CreateQuery
import java.util.UUID
import org.joda.time.{DateTimeZone, DateTime}

class Query(createContext: CreateQuery) {

  val id = UUID.randomUUID()
  val created = DateTime.now(DateTimeZone.UTC)
  val query: Matchers = parseQueryString(createContext.query)

  def parseQueryString(qs: String): Matchers = {
    new TermMatcher[String]("message", createContext.query)
  }
}
