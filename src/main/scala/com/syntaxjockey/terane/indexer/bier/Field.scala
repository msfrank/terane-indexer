package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.Date
import org.joda.time.DateTime
import org.xbill.DNS.Name

class Field

class TextField extends Field {
  def parseValue(text: String): Seq[(String,PostingMetadata)] = {
    val terms: Array[String] = text.toLowerCase.split(""""\\s+"""")
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    0 until terms.length foreach { position =>
      val term = terms(position)
      val postingMetadata = positions.getOrElse(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
}

class LiteralField extends Field {
  def parseValue(literal: List[String]): Seq[(String,PostingMetadata)] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    0 until literal.length foreach { position =>
      val term = literal(position)
      val postingMetadata = positions.getOrElse(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
}

class DatetimeField extends Field {
  def parseValue(datetime: DateTime): Seq[(Date,PostingMetadata)] = {
    Seq((datetime.toDate, PostingMetadata(None)))
  }
}

class HostnameField extends Field {
  def parseValue(hostname: Name): Seq[(String,PostingMetadata)] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    (hostname.labels - 1) to 0 foreach { position =>
      val label = hostname.getLabel(position).toString
      val postingMetadata = positions.getOrElse(label, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
    }
    positions.toMap.toSeq
  }
}

object Field {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])
}
