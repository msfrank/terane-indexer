package com.syntaxjockey.terane.indexer.bier

import akka.actor.ActorRefFactory

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.TickleParser._

object TickleFunctions {

  def parsePredicateFunction(factory: ActorRefFactory, subject: String, functionName: String, functionArgs: Seq[TargetValue], params: TickleParserParams): Option[Matchers] = {
    val signature = (functionName, functionArgs.map(_.dataType))
    functions.get(signature) match {
      case Some(f) =>
        f(functionName, functionArgs, factory)
      case None => None
    }
  }

  def function_cidr(subject: String, args: Seq[TargetValue], factory: ActorRefFactory): Option[Matchers] = args match {
    case Seq(cidr: TargetText) =>
      None
  }

  val functions: Map[(String, Seq[DataType.DataType]), (String, Seq[TargetValue], ActorRefFactory) => Option[Matchers]] = Map(
    ("cidr", Seq(DataType.TEXT)) -> TickleFunctions.function_cidr
  )
}
