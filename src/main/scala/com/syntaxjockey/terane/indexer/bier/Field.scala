package com.syntaxjockey.terane.indexer.bier

import org.joda.time.DateTime
import org.xbill.DNS.{Name, Address => DNSAddress}
import java.util.Date
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.matchers.{TermMatcher, AndMatcher}
import akka.actor.ActorRefFactory

abstract class Field

class TextField extends Field {
  def parseValue(text: Text): Seq[(String,PostingMetadata)] = {
    val terms: Array[String] = text.underlying.toLowerCase.split("""\s+""")
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    0 until terms.size foreach { position =>
      val term = terms(position)
      val postingMetadata = positions.getOrElseUpdate(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, text: String): Matchers = {
    AndMatcher(parseValue(Text(text)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class LiteralField extends Field {
  def parseValue(literal: Literal): Seq[(String,PostingMetadata)] = {
    Seq((literal.underlying, PostingMetadata(None)))
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, literal: String): Matchers = {
    AndMatcher(parseValue(Literal(literal)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class IntegerField extends Field {
  def parseValue(long: Integer): Seq[(Long,PostingMetadata)] = {
    Seq((long.underlying, PostingMetadata(None)))
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, integer: String): Matchers = {
    AndMatcher(parseValue(Integer(integer.toLong)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class FloatField extends Field {
  def parseValue(double: Float): Seq[(Double,PostingMetadata)] = {
    Seq((double.underlying, PostingMetadata(None)))
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, float: String): Matchers = {
    AndMatcher(parseValue(Float(float.toDouble)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class DatetimeField extends Field {
  def parseValue(datetime: Datetime): Seq[(Date,PostingMetadata)] = {
    Seq((datetime.underlying.toDate, PostingMetadata(None)))
  }
  def parseDatetimeString(s: String): DateTime = {
    DateTime.parse(s)
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, datetime: String): Matchers = {
    AndMatcher(parseValue(Datetime(parseDatetimeString(datetime))).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class AddressField extends Field {
  def parseValue(address: Address): Seq[(Array[Byte],PostingMetadata)] = {
    Seq((address.underlying.getAddress, PostingMetadata(None)))
  }
  def parseAddressString(s: String): InetAddress = {
    DNSAddress.getByAddress(s)
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, address: String): Matchers = {
    AndMatcher(parseValue(Address(parseAddressString(address))).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

class HostnameField extends Field {
  def parseValue(hostname: Hostname): Seq[(String,PostingMetadata)] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    val nlabels = hostname.underlying.labels
    (0 until nlabels reverse) foreach { position =>
      val label = hostname.underlying.getLabelString(position).toLowerCase
      val postingMetadata = positions.getOrElseUpdate(label, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
  def parseHostnameString(s: String): Name = {
    Name.fromString(s)
  }
  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, hostname: String): Matchers = {
    AndMatcher(parseValue(Hostname(parseHostnameString(hostname))).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}

object Field {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])

  def apply(fieldId: FieldIdentifier): Field = fieldId.fieldType match {
    case DataType.TEXT =>
      new TextField()
    case DataType.LITERAL =>
      new LiteralField()
    case DataType.INTEGER =>
      new IntegerField()
    case DataType.FLOAT =>
      new FloatField()
    case DataType.DATETIME =>
      new DatetimeField()
    case DataType.ADDRESS =>
      new AddressField()
    case DataType.HOSTNAME =>
      new HostnameField()
  }
}
