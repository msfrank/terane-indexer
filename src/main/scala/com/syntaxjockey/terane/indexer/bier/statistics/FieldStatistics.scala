package com.syntaxjockey.terane.indexer.bier.statistics

import com.twitter.algebird._

case class FieldStatistics(termFrequencies: CMS, fieldCardinality: HLL, termSet: BF, termCount: Long, fieldSignature: MinHashSignature) {

  def estimateTermFrequency(item: Analytical): Approximate[Long] = termFrequencies.frequency(item.hash)

  def estimateFieldCardinality: Approximate[Long] = fieldCardinality.approximateSize

  def estimateTermSetContains(item: Analytical): ApproximateBoolean = termSet.contains(item.string)

  def getTermCount: Long = termCount

  def getFieldSignature: MinHashSignature = fieldSignature
}

object FieldStatistics {
  val frequenciesEps = 0.001
  val frequenciesConfidence = 0.99
  val frequenciesSeed = 123456789
  val cardinalityBits = 16
  val termsetNumHashes = 16
  val termsetWidth = 64
  val termsetSeed = 987654321
  val fieldSignatureNumHashes = 16
  val fieldSignatureNumBands = 64
  val CMS_MONOID = new CountMinSketchMonoid(frequenciesEps, frequenciesConfidence, frequenciesSeed)
  val HLL_MONOID = new HyperLogLogMonoid(cardinalityBits)
  val BF_MONOID  = new BloomFilterMonoid(termsetNumHashes, termsetWidth, termsetSeed)
  val MH32_MONOID = new MinHasher32(fieldSignatureNumHashes, fieldSignatureNumBands)

  val empty = {
    val termFrequencies = CMS_MONOID.zero
    val fieldCardinality = HLL_MONOID.zero
    val termSet = BF_MONOID.zero
    val fieldSignature = MH32_MONOID.zero
    new FieldStatistics(termFrequencies, fieldCardinality, termSet, 0, fieldSignature)
  }

  def apply(terms: Seq[Analytical]): FieldStatistics = {
    val termFrequencies = CMS_MONOID.create(terms.map(_.hash))
    val fieldCardinality = HLL_MONOID.sum(terms.map(term => HLL_MONOID.create(term.bytes)))
    val termSet = BF_MONOID.sum(terms.map(term => BF_MONOID.create(term.string)))
    val fieldSignature = MH32_MONOID.sum(terms.map(term => MH32_MONOID.init(term.string)))
    new FieldStatistics(termFrequencies, fieldCardinality, termSet, terms.length, fieldSignature)
  }

  def merge(others: Seq[FieldStatistics]): FieldStatistics = {
    val termFrequencies = CMS_MONOID.sum(others.map(_.termFrequencies))
    val fieldCardinality = HLL_MONOID.sum(others.map(_.fieldCardinality))
    val termSet = BF_MONOID.sum(others.map(_.termSet))
    val termCount = others.foldLeft(0L) { case (count,stat) => count + stat.getTermCount }
    val fieldSignature = MH32_MONOID.sum(others.map(_.fieldSignature))
    new FieldStatistics(termFrequencies, fieldCardinality, termSet, termCount, fieldSignature)
  }
}
