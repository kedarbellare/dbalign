package cc.refectorie.user.kedarb.dbalign

import fields.Field
import params.Params
import cc.refectorie.user.kedarb.dynprog.{ProbStats, InferSpec}

/**
 * @author kedarb
 * @since 5/11/11
 */

class KLGenSegmentationOnlyInferencer(override val fields: Seq[Field], override val ex: SegmentationOnlyExample,
                                      override val params: Params, override val counts: Params,
                                      override val ispec: InferSpec)
  extends SegmentationOnlyInferencer(fields, ex, params, counts, ispec) {
  var expectedLogZ = 0.0

  override def stats: ProbStats = new ProbStats(expectedLogZ, logVZ, logCZ, elogZ, entropy, objective)

  override def scoreTransition(a: Int, b: Int, i: Int, j: Int): Double = {
    require(useProbs && useWts)
    if (isRecord) 0.0
    else score(genTransitionParams(a), b)
  }

  override def scoreStart(a: Int, j: Int): Double = {
    require(useProbs && useWts)
    if (isRecord) 0.0
    else score(genStartParams, a)
  }

  override def scoreSingleEmission(a: Int, k: Int): Double = {
    require(useProbs && useWts)
    score(genEmissionParams(a), featSeq(k))
  }

  override def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double): Unit = {
    require(useProbs && useWts)
    if (!isRecord) {
      expectedLogZ += x * score(discTransitionParams(a), b)
      update(discTransitionCounts(a), b, x)
    }
  }

  override def updateStart(a: Int, j: Int, x: Double): Unit = {
    require(useProbs && useWts)
    if (!isRecord) {
      expectedLogZ += x * score(discStartParams, a)
      update(discStartCounts, a, x)
    }
  }

  override def updateSingleEmission(a: Int, k: Int, x: Double): Unit = {
    require(useProbs && useWts)
    expectedLogZ += x * score(discEmissionParams(a), featVecSeq(k))
    update(discEmissionCounts(a), featVecSeq(k), x)
  }
}