package cc.refectorie.user.kedarb.dbalign.expts.bft

import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.{ProbStats, Options}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.tools.opts.OptParser
import collection.mutable.HashMap
import cc.refectorie.user.kedarb.dbalign._
import misc._
import params._
import fields._
import cc.refectorie.user.kedarb.dynprog.la.ArrayFromVectors
import actors.Actor
import optimization.linesearch.ArmijoLineSearchMinimization
import optimization.stopCriteria.AverageValueDifference
import optimization.gradientBasedMethods.stats.OptimizerStats
import optimization.gradientBasedMethods.{Optimizer, LBFGS, Objective}

/**
 * @author kedarb
 * @since 5/10/11
 */

object BFTApp extends ACorefSegmentationProblem with BFTEnv with BFTConstrainedInferencer {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val opts = new Options
  val segcorefopts = new SegCorefOptions
  val constraintParamsMap = new HashMap[ObjectId, Array[Double]]

  // global parameters
  var constrainedLearning = false
  var constrainedInference = false

  override def inferPredSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                                     useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    if (constrainedLearning) {
      val corefEx = getCorefSegmentationExample(ex)
      bftInferPredCorefSegmentation(corefEx, localParams, localCounts, useProbs, useWts, stepSize,
        true, false, false)._1
    } else {
      super.inferPredSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
    }
  }

  override def inferBestSegmentation(ex: SegmentationOnlyExample, localParams: Params,
                                     useProbs: Boolean, useWts: Boolean): Segmentation = {
    if (constrainedInference) {
      val corefEx = getCorefSegmentationExample(ex)
      bftInferPredCorefSegmentation(corefEx, localParams, localParams, useProbs, useWts, 0.0,
        true, true, false)._2
    } else {
      super.inferBestSegmentation(ex, localParams, useProbs, useWts)
    }
  }

  def newField(name: String, index: Int): Field = {
    val wt = segcorefopts.simWeight
    if (name == "O")
      FixedLengthOtherField(name, index, 1)
    else if (name == "hotelname")
      new DefaultHashMapPersistentField(name, index, wordIndexer, true, 0.0, wt,
        PhraseHash.unigramWordHash, PhraseSimFunc.softDiceScore(_, _))
    else if (name == "localarea")
      new DefaultHashMapPersistentField(name, index, wordIndexer, false, 0.0, wt,
        PhraseHash.unigramWordHash, PhraseSimFunc.softDiceScore(_, _))
    else if (name == "starrating")
      new DefaultHashMapPersistentField(name, index, wordIndexer, false, 0.0, wt,
        PhraseHash.bigramCharHash, PhraseSimFunc.softCosineSimScore(_, _, 0.99))
    else
      throw fail("Unknown field: " + name)
  }

  def initParams: Unit = {
    params = newParams()
    params.setUniform_!
    params.normalize_!(opts.initSmoothing)
    // smart initialization
    params.discParams.emissions.emissions(otherLabelIndex).increment_!(1)
    for (l <- 0 until L if l != otherLabelIndex)
      params.discParams.transitions.transitions(l).increment_!(l, -0.1)
  }

  def getCorefSegmentationExample(ex: SegmentationOnlyExample): CorefSegmentationExample = {
    new CorefSegmentationExample(ex._id, ex.isRecord, ex.words, ex.features, ex.trueSegmentation)
  }

  override def inferPredCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                          useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    bftInferPredCorefSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize,
      false, false, false)._1
  }

  override def inferBestCorefSegmentation(ex: CorefSegmentationExample, localParams: Params,
                                          useProbs: Boolean, useWts: Boolean): CorefSegmentation = {
    bftInferPredCorefSegmentation(ex, localParams, localParams, useProbs, useWts, 0.0,
      false, true, false)._3
  }

  class BFTCorefSegRecordConstraintsSlave(val id: Int, val params: Params) extends Actor {
    val stats = new ProbStats()
    val useProbs = false
    val useWts = true
    val counts = newParams(useProbs, useWts)

    def act() {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {
            try {
              stats += inferTrueSegmentation(getSegmentationOnlyExample(mention), params, counts, useProbs, useWts, 1)
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
            if (exNum % segcorefopts.pingIterationCount == 0) info("+++ RecordConstraints " + exNum + " examples")
          }
          case TextMention(exNum, mention) => {}
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit()
          }
        }
      }
    }
  }

  class BFTCorefSegTextConstraintsSlave(val id: Int, val params: Params) extends Actor {
    val stats = new ProbStats()
    val useProbs = false
    val useWts = true
    val counts = newParams(useProbs, useWts)

    def act() {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {}
          case TextMention(exNum, mention) => {
            try {
              stats += bftInferPredCorefSegmentation(getCorefSegmentationExample(mention), params,
                counts, useProbs, useWts, segcorefopts.corefSegTextLearnWeight,
                false, false, false, true)._1 * segcorefopts.corefSegTextLearnWeight
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
            if (exNum % segcorefopts.pingIterationCount == 0) info("+++ TextConstraints " + exNum + " examples")
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit()
          }
        }
      }
    }
  }

  class BFTCorefSegExpectationsSlave(val id: Int, val params: Params) extends Actor {
    val stats = new ProbStats()
    val useProbs = false
    val useWts = true
    val counts = newParams(useProbs, useWts)

    def act() {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {
            try {
              val ex = getSegmentationOnlyExample(mention)
              stats += inferDiscSegmentation(ex, params, counts, 1)
              stats -= inferTrueSegmentation(ex, params, counts, useProbs, useWts, 0)
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
            if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Expectations " + exNum + " examples")
          }
          case TextMention(exNum, mention) => {
            try {
              val ex = getCorefSegmentationExample(mention)
              stats += inferDiscCorefSegmentation(ex, params, counts,
                segcorefopts.corefSegTextLearnWeight) * segcorefopts.corefSegTextLearnWeight
              stats -= bftInferPredCorefSegmentation(ex, params,
                counts, useProbs, useWts, 0, false, false, false, false)._1 * segcorefopts.corefSegTextLearnWeight
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
            if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Expectations " + exNum + " examples")
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit()
          }
        }
      }
    }
  }

  def getBftCorefSegRecordConstraints: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BFTCorefSegRecordConstraintsSlave(id, params)
      slave.start
      slave
    })
    var exNum = 0
    foreachRecord((m: DBObject) => {
      val id = exNum % slaves.size
      exNum += 1
      slaves(id) ! RecordMention(exNum, m)
    })
    val counts = newParams(false, true)
    val stats = new ProbStats()
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Params, ProbStats) => {
        counts.add_!(result._1, 1)
        stats += result._2
      }
    }
    (counts, stats)
  }

  def getBftCorefSegTextConstraints: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BFTCorefSegTextConstraintsSlave(id, params)
      slave.start
      slave
    })
    var exNum = 0
    foreachText((m: DBObject) => {
      val id = exNum % slaves.size
      exNum += 1
      slaves(id) ! TextMention(exNum, m)
    })
    val counts = newParams(false, true)
    val stats = new ProbStats()
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Params, ProbStats) => {
        counts.add_!(result._1, 1)
        stats += result._2
      }
    }
    (counts, stats)
  }

  def getBftCorefSegExpectations: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BFTCorefSegExpectationsSlave(id, params)
      slave.start
      slave
    })
    var exNum = 0
    foreachRecord((m: DBObject) => {
      val id = exNum % slaves.size
      exNum += 1
      slaves(id) ! RecordMention(exNum, m)
    })
    foreachText((m: DBObject) => {
      val id = exNum % slaves.size
      exNum += 1
      slaves(id) ! TextMention(exNum, m)
    })
    val counts = newParams(false, true)
    val stats = new ProbStats()
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Params, ProbStats) => {
        counts.add_!(result._1, 1)
        stats += result._2
      }
    }
    (counts, stats)
  }

  class BFTCorefSegmentationObjective extends Objective {
    val paramsArrayFromVectors = new ArrayFromVectors(params.getWtVecs)
    var objectiveValue = Double.NaN
    val invVariance = segcorefopts.invVariance

    val recordConstraints = getBftCorefSegRecordConstraints._1
    var textConstraints = getBftCorefSegTextConstraints._1

    // set parameters and gradient
    parameters = new Array[Double](paramsArrayFromVectors.vectorsArraySize)
    paramsArrayFromVectors.getVectorsInArray(parameters)
    gradient = new Array[Double](paramsArrayFromVectors.vectorsArraySize)

    override def getParameter(index: Int) = parameters(index)

    override def setParameter(index: Int, value: Double) = {
      updateCalls += 1
      objectiveValue = Double.NaN
      parameters(index) = value
    }

    override def getParameters = parameters

    override def setParameters(params: Array[Double]) = {
      updateCalls += 1
      objectiveValue = Double.NaN
      require(params.length == getNumParameters)
      Array.copy(params, 0, parameters, 0, params.length)
    }

    def resetTextConstraints {
      textConstraints = getBftCorefSegTextConstraints._1
    }

    override def setInitialParameters(params: Array[Double]) = setParameters(params)

    def getValueAndGradient: (Params, ProbStats) = getBftCorefSegExpectations

    def updateValueAndGradient: Unit = {
      // set parameters as they may have changed
      paramsArrayFromVectors.setVectorsFromArray(parameters)
      val (expectations, stats) = getValueAndGradient
      // output objective
      objectiveValue = 0.5 * params.wtTwoNormSquared * invVariance + stats.logZ
      info("objective=" + objectiveValue)
      // compute gradient
      java.util.Arrays.fill(gradient, 0.0)
      expectations.wtAdd_!(recordConstraints, -1)
      expectations.wtAdd_!(textConstraints, -1)
      expectations.wtAdd_!(params, invVariance)
      // move expectations to gradient
      new ArrayFromVectors(expectations.getWtVecs).getVectorsInArray(gradient)
    }

    def getValue = {
      if (objectiveValue.isNaN) {
        functionCalls += 1
        updateValueAndGradient
      }
      objectiveValue
    }

    def getGradient = {
      if (objectiveValue.isNaN) {
        gradientCalls += 1
        updateValueAndGradient
      }
      gradient
    }

    override def toString = "objective = " + objectiveValue
  }

  def bftBatchLearnWtsCorefSegmentation(name: String, numIter: Int, useProbs: Boolean, useWts: Boolean) {
    val objective = new BFTCorefSegmentationObjective
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-3)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective): Unit = {
        super.collectIterationStats(optimizer, objective)
        info("*** finished coref+segmentation learning epoch=" + optimizer.getCurrentIteration)
        if (segcorefopts.evalIterationCount > 0 &&
          (optimizer.getCurrentIteration + 1) % segcorefopts.evalIterationCount == 0)
          outputTextSegmentationEval(name + ".iter_" + optimizer.getCurrentIteration, useProbs, useWts)
        objective.asInstanceOf[BFTCorefSegmentationObjective].resetTextConstraints
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def main(args: Array[String]) {
    val parser = new OptParser
    parser.doRegister("exec", opts)
    parser.doRegister("segcoref", segcorefopts)

    if (!parser.doParse(args)) System.exit(1)

    initFields
    initDictionary
    info("#words=" + W + " #wordFeatures=" + WF + " #features=" + F + " #labels=" + L)
    initParams

    // initialize generative model
    if (segcorefopts.segGenLearnIter > 0)
      batchLearnProbsSegmentationOnly("bft.gen", segcorefopts.segGenLearnIter, true, false, false)
    // initialize discriminative model on records only
    if (segcorefopts.segDiscLearnIter > 0)
      batchLearnWtsSegmentationOnly("bft.disc", segcorefopts.segDiscLearnIter, false, true, false)
    // train weights using generative model KL-divergence
    if (segcorefopts.segKLDiscLearnIter > 0)
      batchLearnWtsSegmentationOnly("bft.kl_disc", segcorefopts.segKLDiscLearnIter, true, true, true)
    if (segcorefopts.segGenLearnIter > 0)
      outputTextSegmentationEval("bft.seg.gen", true, false)
    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
      outputTextSegmentationEval("bft.seg.disc", false, true)
    constrainedInference = true
    if (segcorefopts.segGenLearnIter > 0)
      outputTextSegmentationEval("bft.seg.gen.constrained", true, false)
    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
      outputTextSegmentationEval("bft.seg.disc.constrained", false, true)
    constrainedInference = false
    //    if (segcorefopts.segGenLearnIter > 0)
    //      outputTextCorefSegmentationEval("bft.joint.seg_only.gen", true, false)
    //    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
    //      outputTextCorefSegmentationEval("bft.joint.seg_only.disc", false, true)
    if (segcorefopts.corefSegGenLearnIter > 0)
      batchLearnProbsCorefSegmentation("bft.coref.gen", segcorefopts.corefSegGenLearnIter, true, false)
    // batchLearnWtsCorefSegmentation("bft.coref.disc", 100, false, true)
    // onlineLearnWtsCorefSegmentation("bft.coref.disc", 10, false, true)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      miniBatchLearnWtsCorefSegmentation("bft.coref.disc", segcorefopts.corefSegDiscLearnIter, false, true)
    if (segcorefopts.corefSegKLDiscLearnIter > 0)
      batchLearnWtsSegmentationOnly("bft.coref.kl_disc", segcorefopts.corefSegKLDiscLearnIter, true, true, true)
    // output final
    if (segcorefopts.corefSegGenLearnIter > 0)
      outputTextSegmentationEval("bft.coref_seg.gen", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      outputTextSegmentationEval("bft.coref_seg.disc", false, true)
    constrainedInference = true
    if (segcorefopts.corefSegGenLearnIter > 0)
      outputTextSegmentationEval("bft.coref_seg.gen.constrained", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      outputTextSegmentationEval("bft.coref_seg.disc.constrained", false, true)
    constrainedInference = false
    if (segcorefopts.corefSegGenLearnIter > 0)
      outputTextCorefSegmentationEval("bft.joint.coref_seg.gen", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      outputTextCorefSegmentationEval("bft.joint.coref_seg.disc", false, true)
  }
}