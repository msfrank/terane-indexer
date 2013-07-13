package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.Date
import org.joda.time.DateTime
import org.xbill.DNS.Name
import java.net.InetAddress
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier

class Field

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
  def makeValue(text: String): Seq[(String,PostingMetadata)] = parseValue(text)
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
  def makeValue(literal: String): Seq[(String,PostingMetadata)] = parseValue(List(literal))
}

class IntegerField extends Field {
  def parseValue(long: Long): Seq[(Long,PostingMetadata)] = {
    Seq((long, PostingMetadata(None)))
  }
  def makeValue(long: String): Seq[(Long,PostingMetadata)] = parseValue(long.toLong)
}

class FloatField extends Field {
  def parseValue(double: Double): Seq[(Double,PostingMetadata)] = {
    Seq((double, PostingMetadata(None)))
  }
  def makeValue(double: String): Seq[(Double,PostingMetadata)] = parseValue(double.toDouble)
}

class DatetimeField extends Field {
  def parseValue(datetime: DateTime): Seq[(Date,PostingMetadata)] = {
    Seq((datetime.toDate, PostingMetadata(None)))
  }
  def makeValue(datetime: String): Seq[(Date,PostingMetadata)] = parseValue(DateTime.parse(datetime))
}

class AddressField extends Field {
  def parseValue(address: InetAddress): Seq[(Array[Byte],PostingMetadata)] = {
    Seq((address.getAddress, PostingMetadata(None)))
  }
  def makeValue(address: String): Seq[(Array[Byte],PostingMetadata)] = parseValue(InetAddress.getByName(address))
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
  def makeValue(hostname: String): Seq[(String,PostingMetadata)] = parseValue(Name.fromString(hostname))
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
