package com.syntaxjockey.terane.indexer.bier.statistics

import com.twitter.algebird.MurmurHash128

object MurmurHash {
  val hashSeed = 786951432
  val murmurHash128 = new MurmurHash128(hashSeed)
}
