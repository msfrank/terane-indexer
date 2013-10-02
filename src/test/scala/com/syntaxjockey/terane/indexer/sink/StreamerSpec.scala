package com.syntaxjockey.terane.indexer.sink

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.joda.time.DateTime
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap

class StreamerSpec extends WordSpec with MustMatchers {

  "An object implementing the Streamer trait" must {

    "convert an Int into a String key" in {

      val streamer = new Streamer(DateTime.now(), FieldMap(Map.empty, Map.empty)) {}
      streamer.index2key(0) must be("a")
      streamer.index2key(12) must be("m")
      streamer.index2key(35) must be("J")
      streamer.index2key(68) must be("be")
      streamer.index2key(581) must be("jf")
      streamer.index2key(10246) must be("cGg")
    }
  }
}
