package cc.refectorie.user.kedarb.dbalign.fields

import uk.ac.shef.wit.simmetrics.similaritymetrics._
import collection.mutable.HashSet

/**
 * @author kedarb
 * @since 3/25/11
 */
object PhraseSimFunc {
  val jaccard = new JaccardSimilarity
  val jaroWinkler = new JaroWinkler
  val cosine = new CosineSimilarity
  val dice = new DiceSimilarity

  def phraseToString(phrase: Seq[String]) = phrase.map(_.toLowerCase.replaceAll("[^a-z0-9]+", " ")).mkString(" ")

  def jaccardSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]) = {
    val score = jaccard.getSimilarity(phraseToString(srcPhrase), phraseToString(dstPhrase))
    if (score.isNaN) 0.0 else score
  }

  def jaroWinklerSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]) = {
    val score = jaroWinkler.getSimilarity(phraseToString(srcPhrase), phraseToString(dstPhrase))
    if (score.isNaN) 0.0 else score
  }

  def cosineSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]) = {
    val score = cosine.getSimilarity(phraseToString(srcPhrase), phraseToString(dstPhrase))
    if (score.isNaN) 0.0 else score
  }

  def diceSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]) = {
    val score = dice.getSimilarity(phraseToString(srcPhrase), phraseToString(dstPhrase))
    if (score.isNaN) 0.0 else score
  }

  protected def getCommonScore(srcTerms: HashSet[String], dstTerms: HashSet[String],
                               tokSimThresh: Double = 0.9, tokMetric: AbstractStringMetric = jaroWinkler) = {
    var commonScore = 0.0
    for (dstTerm <- dstTerms) {
      if (srcTerms(dstTerm)) commonScore += 1
      else {
        var matchedScore = Double.NaN
        for (srcTerm <- srcTerms) {
          val srcTermSimScore = tokMetric.getSimilarity(srcTerm, dstTerm)
          if (srcTermSimScore >= tokSimThresh) {
            if (matchedScore.isNaN || matchedScore < srcTermSimScore) {
              matchedScore = srcTermSimScore
            }
          }
        }
        if (!matchedScore.isNaN) commonScore += matchedScore
      }
    }
    commonScore
  }

  def getTokenizedTermSet(phrase: Seq[String]): HashSet[String] = {
    val terms = new HashSet[String]
    phrase.foreach {
      w: String =>
        val tw = w.toLowerCase.replaceAll("[^a-z0-9]+", " ").trim
        if (tw.length > 0) terms += tw
    }
    terms
  }

  def softCosineSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String],
                         tokSimThresh: Double = 0.95, tokMetric: AbstractStringMetric = jaroWinkler) = {
    val srcTerms = getTokenizedTermSet(srcPhrase)
    val dstTerms = getTokenizedTermSet(dstPhrase)
    if (srcTerms.size == 0 || dstTerms.size == 0) 0.0
    else getCommonScore(srcTerms, dstTerms, tokSimThresh, tokMetric) /
      (math.sqrt(srcTerms.size.toDouble) * math.sqrt(dstTerms.size.toDouble))
  }

  def softDiceScore(srcPhrase: Seq[String], dstPhrase: Seq[String],
                    tokSimThresh: Double = 0.95, tokMetric: AbstractStringMetric = jaroWinkler) = {
    val srcTerms = getTokenizedTermSet(srcPhrase)
    val dstTerms = getTokenizedTermSet(dstPhrase)
    if (srcTerms.size == 0 || dstTerms.size == 0) 0.0
    else (2 * getCommonScore(srcTerms, dstTerms, tokSimThresh, tokMetric)) / (srcTerms.size + dstTerms.size)
  }

  /**
   * Modified version of EditDistance.levAbbrev from Semantic Discovery Toolkit.
   *
   * The Semantic Discovery Toolkit is free software: you can redistribute it and/or modify
   * it under the terms of the GNU Lesser General Public License as published by
   * the Free Software Foundation, either version 3 of the License, or
   * (at your option) any later version.
   */
  def isAbbrev(a: String, b: String): Boolean = {
    var alen = a.length
    val blen = b.length

    // remove last .
    if (alen > 0 && a(alen - 1) == '.') alen -= 1
    // abbreviations are shorter and match on first alphabet
    if (alen >= blen || a(0) != b(0)) return false

    var ai = 1
    var bi = 1
    while (ai < alen && bi < blen) {
      if (a(ai) == b(bi)) {
        ai += 1
        bi += 1
      } else {
        if (a(ai) == '.') {
          ai += 1
          while (ai < alen && !Character.isLetter(a(ai))) ai += 1
          while (bi < blen && b(bi) != ' ') bi += 1
          while (bi < blen && !Character.isLetter(b(bi))) bi += 1
        } else {
          bi += 1
        }
      }
    }

    return alen == ai
  }

  def isAbbrev(a: String, b: Seq[String]): Boolean = {
    if (b.size == 1) isAbbrev(a, b(0))
    else {
      val alen = a.length
      val blen = b.size
      var ai = 0
      var bi = 0
      // move character by character in a (skip '.')
      // move word by word in b
      while (ai < alen && bi < blen) {
        if (a(ai) == b(bi)(0)) {
          ai += 1
          bi += 1
        } else {
          if (a(ai) == '.') {
            ai += 1
            while (ai < alen && !Character.isLetter(a(ai))) ai += 1
          } else {
            return false
          }
        }
      }
      ai == alen && bi == blen
    }
  }
}