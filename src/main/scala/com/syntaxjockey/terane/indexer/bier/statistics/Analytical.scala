package com.syntaxjockey.terane.indexer.bier.statistics

trait Analytical {
  def toHash: Long
  def toBytes: Array[Byte]
  def toString: String
}
