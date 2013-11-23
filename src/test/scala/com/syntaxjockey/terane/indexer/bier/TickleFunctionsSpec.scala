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

package com.syntaxjockey.terane.indexer.bier

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import org.joda.time.{DateTimeZone, DateTime}
import org.slf4j.LoggerFactory

import com.syntaxjockey.terane.indexer.TestCluster
import com.syntaxjockey.terane.indexer.bier.datatypes.DataType
import com.syntaxjockey.terane.indexer.bier.TickleParser._

class TickleFunctionsSpec extends TestCluster("TickleParserSpec") with WordSpec with MustMatchers {

  val logger = LoggerFactory.getLogger(classOf[TickleParserSpec])

  val params = TickleParserParams("fieldname")

  "TickleFunctions.parseTargetFunction()" must {


    "parse the cidr() function" in {
      val query = TickleParser.parseQueryString("""?fieldname = cidr(192.168.0/24)""")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEqualsRange(
            TargetRange(Some(TargetAddress("192.168.0.0")), Some(TargetAddress("192.168.0.255")), DataType.ADDRESS, false, false))))
        )
      )
    }

    "parse the now() function with empty arguments" in {
      val query = TickleParser.parseQueryString("""?fieldname = now()""")
      logger.debug(TickleParser.prettyPrint(query))
      inside(query) {
        case Query(Left(Expression(Some("fieldname"), PredicateEquals(TargetDatetime(datetime))))) =>
          val now = DateTime.now(DateTimeZone.UTC)
          val dt = DateTime.parse(datetime)
          val variance = Math.abs(now.getMillis - dt.getMillis)
          variance must be < 500.toLong
      }
    }

    "parse the now() function with timezone argument" in {
      val query = TickleParser.parseQueryString("""?fieldname = now(America/Los_Angeles)""")
      logger.debug(TickleParser.prettyPrint(query))
      inside(query) {
        case Query(Left(Expression(Some("fieldname"), PredicateEquals(TargetDatetime(datetime))))) =>
          val now = DateTime.now(DateTimeZone.UTC)
          val dt = DateTime.parse(datetime)
          val variance = Math.abs(now.getMillis - dt.getMillis)
          variance must be < 500.toLong
      }
    }
  }
}
