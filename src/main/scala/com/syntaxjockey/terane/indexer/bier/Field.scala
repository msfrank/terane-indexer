package com.syntaxjockey.terane.indexer.bier

import org.joda.time.DateTime
import org.xbill.DNS.{Address, Name}
import java.util.Date
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.matchers.{TermMatcher, AndMatcher}

abstract class Field

class TextField extends Field {
  def parseValue(text: String): Seq[(String,PostingMetadata)] = {
    val terms: Array[String] = text.toLowerCase.split("""\s+""")
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    0 until terms.size foreach { position =>
      val term = terms(position)
      val postingMetadata = positions.getOrElseUpdate(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
  def makeMatcher(fieldId: FieldIdentifier, text: String): Matchers = {
    AndMatcher(parseValue(text).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class LiteralField extends Field {
  def parseValue(literal: List[String]): Seq[(String,PostingMetadata)] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    0 until literal.size foreach { position =>
      val term = literal(position)
      val postingMetadata = positions.getOrElseUpdate(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
  def makeMatcher(fieldId: FieldIdentifier, literal: String): Matchers = {
    AndMatcher(parseValue(List(literal)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class IntegerField extends Field {
  def parseValue(long: Long): Seq[(Long,PostingMetadata)] = {
    Seq((long, PostingMetadata(None)))
  }
  def makeMatcher(fieldId: FieldIdentifier, integer: String): Matchers = {
    AndMatcher(parseValue(integer.toLong).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class FloatField extends Field {
  def parseValue(double: Double): Seq[(Double,PostingMetadata)] = {
    Seq((double, PostingMetadata(None)))
  }
  def makeMatcher(fieldId: FieldIdentifier, float: String): Matchers = {
    AndMatcher(parseValue(float.toDouble).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class DatetimeField extends Field {
  def parseValue(datetime: DateTime): Seq[(Date,PostingMetadata)] = {
    Seq((datetime.toDate, PostingMetadata(None)))
  }
  def parseDatetimeString(s: String): DateTime = {
    DateTime.parse(s)
  }
  def makeMatcher(fieldId: FieldIdentifier, datetime: String): Matchers = {
    AndMatcher(parseValue(parseDatetimeString(datetime)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class AddressField extends Field {
  def parseValue(address: InetAddress): Seq[(Array[Byte],PostingMetadata)] = {
    Seq((address.getAddress, PostingMetadata(None)))
  }
  def parseAddressString(s: String): InetAddress = {
    Address.getByAddress(s)
  }
  def makeMatcher(fieldId: FieldIdentifier, address: String): Matchers = {
    AndMatcher(parseValue(parseAddressString(address)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

class HostnameField extends Field {
  def parseValue(hostname: Name): Seq[(String,PostingMetadata)] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    val nlabels = hostname.labels
    (0 until nlabels reverse) foreach { position =>
      val label = hostname.getLabelString(position).toLowerCase
      val postingMetadata = positions.getOrElseUpdate(label, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
  def parseHostnameString(s: String): Name = {
    Name.fromString(s)
  }
  def makeMatcher(fieldId: FieldIdentifier, hostname: String): Matchers = {
    AndMatcher(parseValue(parseHostnameString(hostname)).map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)
  }
}

object Field {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])

  def apply(fieldId: FieldIdentifier): Field = fieldId.fieldType match {
    case EventValueType.TEXT =>
      new TextField()
    case EventValueType.LITERAL =>
      new LiteralField()
    case EventValueType.INTEGER =>
      new IntegerField()
    case EventValueType.FLOAT =>
      new FloatField()
    case EventValueType.DATETIME =>
      new DatetimeField()
    case EventValueType.ADDRESS =>
      new AddressField()
    case EventValueType.HOSTNAME =>
      new HostnameField()
  }
}
