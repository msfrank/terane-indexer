package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.Date
import org.joda.time.DateTime
import org.xbill.DNS.Name
import java.net.InetAddress

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
}

class IntegerField extends Field {
  def parseValue(long: Long): Seq[(Long,PostingMetadata)] = {
    Seq((long, PostingMetadata(None)))
  }
}

class FloatField extends Field {
  def parseValue(double: Double): Seq[(Double,PostingMetadata)] = {
    Seq((double, PostingMetadata(None)))
  }
}

class DatetimeField extends Field {
  def parseValue(datetime: DateTime): Seq[(Date,PostingMetadata)] = {
    Seq((datetime.toDate, PostingMetadata(None)))
  }
}

class AddressField extends Field {
  def parseValue(address: InetAddress): Seq[(Array[Byte],PostingMetadata)] = {
    Seq((address.getAddress, PostingMetadata(None)))
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
}

object Field {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])
}
