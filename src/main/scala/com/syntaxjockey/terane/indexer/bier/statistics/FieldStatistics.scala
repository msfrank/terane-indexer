package com.syntaxjockey.terane.indexer.bier.statistics

import com.twitter.algebird._

case class FieldStatistics(termFrequencies: CMS, fieldCardinality: HLL, termSet: BF, fieldSignature: MinHashSignature) {

  def estimateTermFrequency(item: Long): Approximate[Long] = termFrequencies.frequency(item)

  def getFieldCardinality: Approximate[Long] = fieldCardinality.approximateSize

  def termSetContains(item: String): ApproximateBoolean = termSet.contains(item)

  def getFieldSignature: MinHashSignature = fieldSignature
}

object FieldStatistics {
  val frequenciesEps = 0.001
  val frequenciesConfidence = 0.99
  val frequenciesSeed = 123456789
  val cardinalityBits = 1024 * 512
  val termsetNumHashes = 16
  val termsetWidth = 64
  val termsetSeed = 987654321
  val fieldSignatureNumHashes = 16
  val fieldSignatureNumBands = 64
  val hashSeed = 786951432
  val CMS_MONOID = new CountMinSketchMonoid(frequenciesEps, frequenciesConfidence, frequenciesSeed)
  val HLL_MONOID = new HyperLogLogMonoid(cardinalityBits)
  val BF_MONOID  = new BloomFilterMonoid(termsetNumHashes, termsetWidth, termsetSeed)
  val MH32_MONOID = new MinHasher32(fieldSignatureNumHashes, fieldSignatureNumBands)
  val murmurHash128 = new MurmurHash128(hashSeed)

  def apply(terms: Seq[Analytical]): FieldStatistics = {
    val termFrequencies = CMS_MONOID.create(terms.map(_.toHash))
    val fieldCardinality = HLL_MONOID.sum(terms.map(term => HLL_MONOID.create(term.toBytes)))
    val termSet = BF_MONOID.sum(terms.map(term => BF_MONOID.create(term.toString)))
    val fieldSignature = MH32_MONOID.sum(terms.map(term => MH32_MONOID.init(term.toString)))
    new FieldStatistics(termFrequencies, fieldCardinality, termSet, fieldSignature)
  }

  def merge(others: Seq[FieldStatistics]): FieldStatistics = {
    val termFrequencies = CMS_MONOID.sum(others.map(_.termFrequencies))
    val fieldCardinality = HLL_MONOID.sum(others.map(_.fieldCardinality))
    val termSet = BF_MONOID.sum(others.map(_.termSet))
    val fieldSignature = MH32_MONOID.sum(others.map(_.fieldSignature))
    new FieldStatistics(termFrequencies, fieldCardinality, termSet, fieldSignature)
  }
}
