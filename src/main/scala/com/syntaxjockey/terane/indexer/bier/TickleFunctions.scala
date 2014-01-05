package com.syntaxjockey.terane.indexer.bier

import akka.actor.ActorRefFactory

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.TickleParser._
import org.joda.time.{DateTimeZone, DateTime}
import java.net.InetAddress
import java.nio.ByteBuffer

object TickleFunctions {

  def parseTargetFunction(functionName: String, functionArgs: String): Target = {
    targetFunctions.get(functionName) match {
      case Some(f) =>
        f(functionArgs)
      case None => throw new Exception("unknown function %s()".format(functionName))
    }
  }

  def target_cidr(args: String): Target = {
    def int2TargetAddress(addr: Int): TargetAddress = {
      TargetAddress(InetAddress.getByAddress(ByteBuffer.allocate(4).putInt(addr).array()).getHostAddress)
    }
    try {
      val parts = args.trim.split("/")
      if (parts.length == 2) {
        val network: String = parts(0)
        val postfix: String = parts(1)
        val octets: Array[Int] = network.split('.').map(_.toInt).padTo(4, 0)
        if (octets.length > 4) throw new Exception()
        var addr: Int = 0
        addr |= octets(0) << 24
        addr |= octets(1) << 16
        addr |= octets(2) << 8
        addr |= octets(3) << 0
        val shift = postfix.toInt
        if (shift > 32) throw new Exception()
        val mask: Int = 0xffffffff >>> shift
        val start = int2TargetAddress(addr)
        val end = int2TargetAddress(addr | mask)
        TargetRange(Some(start), Some(end), DataType.ADDRESS, startExcl = false, endExcl = false)
      } else throw new Exception()
    } catch {
      case ex: Throwable =>
        throw new Exception("invalid CIDR range %s".format(args.trim))
    }
  }

  def target_now(args: String): Target = args.trim match {
    case "" => TargetDatetime(DateTime.now(DateTimeZone.UTC).toString)
    case timezone =>
      try {
        TargetDatetime(DateTime.now(DateTimeZone.forID(timezone)).toString)
      } catch {
        case ex: IllegalArgumentException => throw new Exception("unknown timezone %s".format(timezone))
      }
  }

  def coerce_text(args: String): Target = TargetText(args)
  def coerce_literal(args: String): Target = TargetLiteral(args)
  def coerce_integer(args: String): Target = TargetInteger(args)
  def coerce_float(args: String): Target = TargetFloat(args)
  def coerce_datetime(args: String): Target = TargetDatetime(args)
  def coerce_address(args: String): Target = TargetAddress(args)
  def coerce_hostname(args: String): Target = TargetHostname(args)

  val targetFunctions: Map[String, String => Target] = Map(
    "cidr" -> TickleFunctions.target_cidr,
    "now" -> TickleFunctions.target_now,
    "text" -> TickleFunctions.coerce_text,
    "literal" -> TickleFunctions.coerce_literal,
    "integer" -> TickleFunctions.coerce_integer,
    "float" -> TickleFunctions.coerce_float,
    "datetime" -> TickleFunctions.coerce_datetime,
    "address" -> TickleFunctions.coerce_address,
    "hostname" -> TickleFunctions.coerce_hostname
  )

  def parsePredicateFunction(factory: ActorRefFactory, subject: String, functionName: String, functionArgs: Seq[TargetValue], params: TickleParserParams): Matchers = {
    val signature = (functionName, functionArgs.map(_.dataType))
    predicateFunctions.get(signature) match {
      case Some(f) =>
        f(functionName, functionArgs, factory)
      case None => throw new Exception("unknown function %s()".format(functionName))
    }
  }

  val predicateFunctions: Map[(String, Seq[DataType.DataType]), (String, Seq[TargetValue], ActorRefFactory) => Matchers] = Map()
}
