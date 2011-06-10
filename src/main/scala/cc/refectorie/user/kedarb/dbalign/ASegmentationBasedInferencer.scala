package cc.refectorie.user.kedarb.dbalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import cc.refectorie.user.kedarb.dynprog.segment.{Segmentation, Segment}
import cc.refectorie.user.kedarb.dynprog.AHypergraphInferState
import fields.Field
import params.Params

/**
 * @author kedarb
 * @since 5/10/11
 */

trait ASegmentationBasedInferencer[Widget, Example <: ACorefSegmentationExample[Widget]]
  extends AHypergraphInferState[Widget, Example, Params] {
  def fields: Seq[Field]

  lazy val N: Int = ex.numTokens

  lazy val isRecord: Boolean = ex.isRecord

  lazy val words: Array[String] = ex.words

  lazy val featSeq: Array[Int] = ex.featSeq

  lazy val featVecSeq: Array[FtrVec] = ex.featVecSeq

  lazy val trueSegmentation: Segmentation = ex.trueSegmentation

  lazy val L = fields.size

  lazy val cachedEmissionScores: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => Double.NaN))

  lazy val cachedEmissionCounts: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => 0.0))

  lazy val cachedPossibleEnds: Array[Boolean] = mapIndex(N + 1, (j: Int) => ex.isPossibleEnd(j))

  // allowed segment for span [i, j) for label a
  def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueInfer) trueSegmentation.contains(Segment(i, j, a))
    else {
      if (isRecord) {
        // only allow sub-segments that are a subset of segment at i
        val optionSegmentAtI = trueSegmentation.segmentAt(i)
        optionSegmentAtI.isDefined && optionSegmentAtI.get.end >= j &&
          (fields(a).name == "O" || cachedPossibleEnds(j) || optionSegmentAtI.get.end == j)
      } else {
        // allow all possible segments
        fields(a).name == "O" || cachedPossibleEnds(j)
      }
    }
  }

  def allowedStart(a: Int): Boolean = true

  def allowedTransition(a: Int, b: Int): Boolean = true

  // params/counts short-cuts
  def genParams = params.genParams

  def genCounts = counts.genParams

  def discParams = params.discParams

  def discCounts = counts.discParams

  def genStartParams = genParams.transitions.starts

  def genStartCounts = genCounts.transitions.starts

  def discStartParams = discParams.transitions.starts

  def discStartCounts = discCounts.transitions.starts

  def genTransitionParams(a: Int) = genParams.transitions.transitions(a)

  def genTransitionCounts(a: Int) = genCounts.transitions.transitions(a)

  def discTransitionParams(a: Int) = discParams.transitions.transitions(a)

  def discTransitionCounts(a: Int) = discCounts.transitions.transitions(a)

  def genEmissionParams(a: Int) = genParams.emissions.emissions(a)

  def genEmissionCounts(a: Int) = genCounts.emissions.emissions(a)

  def discEmissionParams(a: Int) = discParams.emissions.emissions(a)

  def discEmissionCounts(a: Int) = discCounts.emissions.emissions(a)

  // scoring functions
  def prMultiplier: Double = {
    if (useProbs && useWts) 0.5
    else 1.0
  }

  def wtMultiplier: Double = {
    if (useProbs & useWts) 0.5
    else 1.0
  }

  def scoreTransition(a: Int, b: Int, i: Int, j: Int): Double = {
    if (isRecord) 0.0
    else prMultiplier * score(genTransitionParams(a), b) + wtMultiplier * score(discTransitionParams(a), b)
  }

  def scoreStart(a: Int, j: Int): Double = {
    if (isRecord) 0.0
    else prMultiplier * score(genStartParams, a) + wtMultiplier * score(discStartParams, a)
  }

  def scoreSingleEmission(a: Int, k: Int): Double = {
    prMultiplier * score(genEmissionParams(a), featSeq(k)) + wtMultiplier * score(discEmissionParams(a), featVecSeq(k))
  }

  def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double): Unit = {
    if (!isRecord) {
      update(genTransitionCounts(a), b, prMultiplier * x)
      update(discTransitionCounts(a), b, wtMultiplier * x)
    }
  }

  def updateStart(a: Int, j: Int, x: Double): Unit = {
    if (!isRecord) {
      update(genStartCounts, a, prMultiplier * x)
      update(discStartCounts, a, wtMultiplier * x)
    }
  }

  def updateSingleEmission(a: Int, k: Int, x: Double): Unit = {
    cachedEmissionCounts(a)(k) += x
  }

  def scoreEmission(a: Int, i: Int, j: Int): Double = {
    var sumScore = 0.0
    forIndex(i, j, (k: Int) => {
      if (cachedEmissionScores(a)(k).isNaN) cachedEmissionScores(a)(k) = scoreSingleEmission(a, k)
      sumScore += cachedEmissionScores(a)(k)
    })
    sumScore
  }

  def updateEmission(a: Int, i: Int, j: Int, x: Double): Unit = {
    forIndex(i, j, (k: Int) => updateSingleEmission(a, k, x))
  }

  override def updateCounts {
    super.updateCounts
    counts.synchronized {
      forIndex(L, (a: Int) => {
        forIndex(N, (k: Int) => {
          val x = cachedEmissionCounts(a)(k)
          update(genEmissionCounts(a), featSeq(k), prMultiplier * x)
          update(discEmissionCounts(a), featVecSeq(k), wtMultiplier * x)
        })
      })
    }
  }
}