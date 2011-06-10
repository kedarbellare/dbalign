package cc.refectorie.user.kedarb.dbalign.params

import cc.refectorie.user.kedarb.dynprog.AParams
import cc.refectorie.user.kedarb.dynprog.types.{Indexer, ParamVec}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._

/**
 * @author kedarb
 * @since 5/10/11
 */

class TransitionParams(val labelIndexer: Indexer[String],
                       val starts: ParamVec,
                       val transitions: Array[ParamVec]) extends AParams {
  def foreachVec(f: (ParamVec) => Any): Unit = {
    f(starts)
    transitions.foreach(f(_))
  }

  def output(puts: (String) => Any): Unit = {
    puts("")
    foreachIndex(getValues(starts), {
      (a: Int, v: Double) => if (v != 0) puts("S\t%s\t%s".format(labelIndexer(a), fmt(v)))
    })
    foreachIndex(transitions, {
      (a: Int, pv: ParamVec) =>
        puts("")
        foreachSortedIndex(getValues(pv), {
          (b: Int, v: Double) =>
            if (v != 0) puts("T\t%s\t%s\t%s".format(labelIndexer(a), labelIndexer(b), fmt(v)))
        })
    })
  }
}

class EmissionParams(val labelIndexer: Indexer[String],
                     val featureIndexer: Indexer[String],
                     val emissions: Array[ParamVec]) extends AParams {
  def foreachVec(f: (ParamVec) => Any): Unit = {
    emissions.foreach(f(_))
  }

  def output(puts: (String) => Any): Unit = {
    foreachIndex(emissions, {
      (a: Int, pv: ParamVec) =>
        puts("")
        foreachSortedIndex(getValues(pv), {
          (i: Int, v: Double) =>
            if (v != 0) puts("E\t%s\t%s\t%s".format(labelIndexer(a), featureIndexer(i), fmt(v)))
        })
    })
  }
}

class SegmentParams(val transitions: TransitionParams, val emissions: EmissionParams) extends AParams {
  def output(puts: (String) => Any): Unit = {
    transitions.output(puts)
    emissions.output(puts)
  }

  def foreachVec(f: (ParamVec) => Any): Unit = {
    transitions.foreachVec(f)
    emissions.foreachVec(f)
  }
}

class Params(var genParams: SegmentParams, var discParams: SegmentParams) extends AParams {
  def output(puts: (String) => Any): Unit = {
    genParams.output(puts)
    discParams.output(puts)
  }

  def foreachVec(f: (ParamVec) => Any): Unit = {
    genParams.foreachVec(f)
    discParams.foreachVec(f)
  }
}