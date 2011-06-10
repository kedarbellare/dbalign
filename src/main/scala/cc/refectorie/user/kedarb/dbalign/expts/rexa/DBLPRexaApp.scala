package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.{ProbStats, Options}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.{IndexerUtils, Indexer}
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.tools.opts.OptParser
import cc.refectorie.user.kedarb.dbalign._
import misc._
import params._
import fields._
import io.Source
import java.io._
import collection.mutable.{ArrayBuffer, HashMap}
import actors.Actor
import cc.refectorie.user.kedarb.dynprog.la.ArrayFromVectors
import optimization.linesearch.ArmijoLineSearchMinimization
import optimization.stopCriteria.AverageValueDifference
import optimization.gradientBasedMethods.stats.OptimizerStats
import optimization.gradientBasedMethods.{Optimizer, LBFGS, Objective}

/**
 * @author kedarb
 * @since 5/17/11
 */

object DBLPRexaApp extends ACorefSegmentationProblem with DBLPRexaEnv with RexaConstrainedInferencer {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val opts = new Options
  val segcorefopts = new SegCorefOptions
  val oovIndex = wordFeatureIndexer.indexOf_!("$OOV$")
  val constraintParamsMap = new HashMap[ObjectId, Array[Double]]

  // global parameters
  var constrainedLearning = false
  var constrainedInference = false

  def lindex(s: String): Int = labelIndexer.indexOf_?(s)

  def newField(name: String, index: Int): Field = {
    if (name == "O")
      FixedLengthOtherField(name, index, 1)
    else if (name == "author" || name == "editor")
    //      new DefaultMongoHashMapPersistentField(processedKB, name, index, (name == "author"), 0.7, 1.0,
    //        authorHash, PhraseSimFunc.softCosineSimScore(_, _))
      new DefaultHashMapPersistentField(name, index, wordIndexer, (name == "author"), 0.7, 1.0,
        authorHash, PhraseSimFunc.softCosineSimScore(_, _))
    else if (name == "title")
    //      new DefaultMongoHashMapPersistentField(processedKB, name, index, true, 0.8, 1.0,
    //        titleHash, PhraseSimFunc.softCosineSimScore(_, _))
      new DefaultHashMapPersistentField(name, index, wordIndexer, true, 0.8, 1.0,
        titleHash, PhraseSimFunc.softCosineSimScore(_, _))
    else {
      //      new DefaultMongoHashMapPersistentField(processedKB, name, index, false, 0.0, 1.0,
      //        PhraseHash.unigramWordHash, PhraseSimFunc.softCosineSimScore(_, _))
      val fld = new DefaultHashMapPersistentField(name, index, wordIndexer, false, 0.0, 1.0,
        PhraseHash.noHash, PhraseSimFunc.softCosineSimScore(_, _))
      if (name == "volume" || name == "date" || name == "pages") fld.maxSegmentLength = 6
      fld
    }
  }

  def initParams: Unit = {
    params = newParams()
    params.setUniform_!
    params.normalize_!(opts.initSmoothing)
    // smart initialization
    // params.discParams.emissions.emissions(otherLabelIndex).increment_!(1)
    for (l <- 0 until L if l != otherLabelIndex)
      params.discParams.transitions.transitions(l).increment_!(l, -0.1)
  }

  override def getSegmentationOnlyExample(mention: DBObject) = {
    new SegmentationOnlyExample(mention._id.get, isRecord(mention), getStringArray(mention),
      getFeaturesForMention(mention).map((f: Features) => if (f._1 >= 0) f else (oovIndex, f._2)),
      getTrueSegmentation(mention)) {
      override def isPossibleEnd(j: Int): Boolean = DBLPRexaApp.isPossibleEnd(j, words)
    }
  }

  override def getCorefSegmentationExample(mention: DBObject): CorefSegmentationExample = {
    new CorefSegmentationExample(mention._id.get, isRecord(mention), getStringArray(mention),
      getFeaturesForMention(mention).map((f: Features) => if (f._1 >= 0) f else (oovIndex, f._2)),
      getTrueSegmentation(mention)) {
      override def isPossibleEnd(j: Int): Boolean = DBLPRexaApp.isPossibleEnd(j, words)
    }
  }

  def getCorefSegmentationExample(ex: SegmentationOnlyExample): CorefSegmentationExample = {
    new CorefSegmentationExample(ex._id, ex.isRecord, ex.words, ex.features, ex.trueSegmentation) {
      override def isPossibleEnd(j: Int): Boolean = DBLPRexaApp.isPossibleEnd(j, words)
    }
  }

  override def inferPredSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                                     useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    if (constrainedLearning) {
      val corefEx = getCorefSegmentationExample(ex)
      rexaInferPredCorefSegmentation(corefEx, localParams, localCounts, useProbs, useWts, stepSize,
        true, false, false)._1
    } else {
      super.inferPredSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
    }
  }

  override def inferBestSegmentation(ex: SegmentationOnlyExample, localParams: Params,
                                     useProbs: Boolean, useWts: Boolean): Segmentation = {
    if (constrainedInference) {
      val corefEx = getCorefSegmentationExample(ex)
      rexaInferPredCorefSegmentation(corefEx, localParams, localParams, useProbs, useWts, 0.0,
        true, true, false)._2
    } else {
      super.inferBestSegmentation(ex, localParams, useProbs, useWts)
    }
  }

  override def inferPredCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                          useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    rexaInferPredCorefSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize,
      false, false, false)._1
  }

  override def inferBestCorefSegmentation(ex: CorefSegmentationExample, localParams: Params,
                                          useProbs: Boolean, useWts: Boolean): CorefSegmentation = {
    rexaInferPredCorefSegmentation(ex, localParams, localParams, useProbs, useWts, 0.0,
      false, true, false)._3
  }

  def getRexaTestMentions: Iterator[DBObject] =
    Source.fromFile(segcorefopts.rexaTestFile).getLines().map(newMentionFromCitationString(_))

  override def adjustSegmentationForPunctuation(words: Array[String], segmentation: Segmentation): Segmentation = {
    // do additional post processing
    rexaAdjustSegmentationForPunctuation(otherLabelIndex,
      words, super.adjustSegmentationForPunctuation(words, segmentation))
  }

  override def outputTextSegmentationEval(filePrefix: String, useProbs: Boolean, useWts: Boolean): Unit = {
    val mentions = getRexaTestMentions.toArray
    val numTest = (mentions.size * segcorefopts.rexaTestProportion).toInt
    val numTrain = mentions.size - numTest
    val goldWriter = new PrintStream(new FileOutputStream(filePrefix + ".gold.ww"))
    val guessWriter = new PrintStream(new FileOutputStream(filePrefix + ".guess.ww"))
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    parallel_foreach(segcorefopts.numThreads, mentions.drop(numTrain).toArray, (i: Int, m: DBObject, dbg: Boolean) => {
      // mentions.drop(numTrain).foreach((m: DBObject) => {
      val ex = getSegmentationOnlyExample(m)
      val words = ex.words
      val (origTrueSeg, origPredSeg) = (ex.trueSegmentation, inferBestSegmentation(ex, params, useProbs, useWts))
      val trueSeg = adjustSegmentationForPunctuation(words, origTrueSeg)
      val predSeg = adjustSegmentationForPunctuation(words, origPredSeg)
      tokenPerf.synchronized {
        tokenPerf.add(trueSeg, predSeg)
        segmentPerf.add(trueSeg, predSeg)
        perLabelPerf.add(trueSeg, predSeg)
        goldWriter.println(widgetToFullString(m, trueSeg))
        guessWriter.println(widgetToFullString(m, predSeg))
      }
    })
    info("")
    info("*** Evaluating " + filePrefix)
    tokenPerf.output(info(_))
    perLabelPerf.output(info(_))
    segmentPerf.output(info(_))
    goldWriter.close
    guessWriter.close
  }

  def heldOutOutputTextCorefSegmentationEval(filePrefix: String, useProbs: Boolean, useWts: Boolean): Unit = {
    val oldPingIterCount = segcorefopts.pingIterationCount
    segcorefopts.pingIterationCount = 100
    val mentions = getRexaTestMentions.toArray
    val numTest = (mentions.size * segcorefopts.rexaTestProportion).toInt
    val numTrain = mentions.size - numTest
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    var exNum = 0
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BatchCorefSegmentationEvaluationSlave(id, params, useProbs, useWts)
      slave.start
      slave
    })
    mentions.drop(numTrain).foreach((mention: DBObject) => {
      val id = exNum % segcorefopts.numThreads
      exNum += 1
      slaves(id) ! TextMention(exNum, mention)
    })
    val mentionClusters = new ArrayBuffer[Option[String]]()
    val mentionWords = new ArrayBuffer[Array[String]]
    val mentionSegmentations = new ArrayBuffer[Segmentation]
    val corefSegmentations = new ArrayBuffer[CorefSegmentation]
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Seq[Array[String]], Seq[Segmentation], Seq[Option[String]], Seq[CorefSegmentation]) => {
        mentionWords ++= result._1
        mentionSegmentations ++= result._2
        mentionClusters ++= result._3
        corefSegmentations ++= result._4
      }
    }

    // coreference performance
    var numCorrectMatches = 0.0
    var numPredMatches = 0.0
    var numPossibleMatches = 0.0
    var numWithClusters = 0.0
    var numCorrWithClusters = 0.0
    var numWithoutClusters = 0.0
    var numCorrWithoutClusters = 0.0
    forIndex(mentionWords.size, (i: Int) => {
      val words = mentionWords(i)
      val origPredCorefSeg = corefSegmentations(i)
      // check coreference performance
      val mentionCluster: Option[String] = mentionClusters(i)
      val recordCluster: Option[String] = {
        val matchedRecordId = origPredCorefSeg.corefSegment(0).recordClusterId
        if (!matchedRecordId.isDefined) None
        else {
          val matchedRecord = mentionColl.findOneByID(matchedRecordId.get)
          if (!matchedRecord.isDefined || !matchedRecord.get.isDefinedAt("trueCluster")) None
          else Some(matchedRecord.get("trueCluster").toString)
        }
      }

      val isCorrect = (mentionCluster == recordCluster)
      if (isCorrect) numCorrectMatches += 1
      if (recordCluster.isDefined) numPredMatches += 1
      if (mentionCluster.isDefined) {
        if (isCorrect) numCorrWithClusters += 1
        numPossibleMatches += 1
        numWithClusters += 1
      } else {
        if (isCorrect) numCorrWithoutClusters += 1
        numWithoutClusters += 1
      }

      // check segmentation performance
      val (origTrueSeg, origPredSeg) = (mentionSegmentations(i), origPredCorefSeg.getSegmentation)
      val trueSeg = adjustSegmentationForPunctuation(words, origTrueSeg)
      val predSeg = adjustSegmentationForPunctuation(words, origPredSeg)
      tokenPerf.add(trueSeg, predSeg)
      segmentPerf.add(trueSeg, predSeg)
      perLabelPerf.add(trueSeg, predSeg)
    })
    info("")
    info("*** Evaluating " + filePrefix)
    tokenPerf.output(info(_))
    perLabelPerf.output(info(_))
    segmentPerf.output(info(_))
    val p = if (numPredMatches == 0) 0.0 else (numCorrectMatches / numPredMatches)
    val r = if (numPossibleMatches == 0) 0.0 else (numCorrectMatches / numPossibleMatches)
    val f1 = if (p == 0 && r == 0) 0.0 else (2 * p * r) / (p + r)
    info("corefEval accuracy: precision=" + p + " recall=" + r + " F1=" + f1 +
      " correct=" + numCorrectMatches + " pred=" + numPredMatches + " true=" + numPossibleMatches)
    info("corefEval: clustered(corr/total)=" + numCorrWithClusters + "/" + numWithClusters +
      " acc=" + (numCorrWithClusters / numWithClusters) +
      " singleton(corr/total)=" + numCorrWithoutClusters + "/" + numWithoutClusters +
      " acc=" + (numCorrWithoutClusters / numWithoutClusters))
    segcorefopts.pingIterationCount = oldPingIterCount
  }

  def loadIndexer(indexer: Indexer[String], file: String): Unit = {
    val in = new BufferedReader(new FileReader(file))
    IndexerUtils.deserializeToIndexerString(in.readLine(), indexer)
    in.close
  }

  def checkExists(file: String): Boolean = new File(file).exists()

  class RexaCorefSegRecordConstraintsSlave(val id: Int, val params: Params) extends Actor {
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

  class RexaCorefSegTextConstraintsSlave(val id: Int, val params: Params) extends Actor {
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
              stats += rexaInferPredCorefSegmentation(getCorefSegmentationExample(mention), params,
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

  class RexaCorefSegExpectationsSlave(val id: Int, val params: Params) extends Actor {
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
              stats -= rexaInferPredCorefSegmentation(ex, params,
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

  def getRexaCorefSegRecordConstraints: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new RexaCorefSegRecordConstraintsSlave(id, params)
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

  def getRexaCorefSegTextConstraints: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new RexaCorefSegTextConstraintsSlave(id, params)
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

  def getRexaCorefSegExpectations: (Params, ProbStats) = {
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new RexaCorefSegExpectationsSlave(id, params)
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

  class RexaCorefSegmentationObjective extends Objective {
    val paramsArrayFromVectors = new ArrayFromVectors(params.getWtVecs)
    var objectiveValue = Double.NaN
    val invVariance = segcorefopts.invVariance

    val recordConstraints = getRexaCorefSegRecordConstraints._1
    var textConstraints = getRexaCorefSegTextConstraints._1

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
      textConstraints = getRexaCorefSegTextConstraints._1
    }

    override def setInitialParameters(params: Array[Double]) = setParameters(params)

    def getValueAndGradient: (Params, ProbStats) = getRexaCorefSegExpectations

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

  def rexaBatchLearnWtsCorefSegmentation(name: String, numIter: Int, useProbs: Boolean, useWts: Boolean) {
    val objective = new RexaCorefSegmentationObjective
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
        objective.asInstanceOf[RexaCorefSegmentationObjective].resetTextConstraints
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

    // look up labels from test file
    getRexaTestMentions.foreach {
      m: DBObject => Segmentation.fromBIO(getStringArray(m, "trueLabels"), labelIndexer.indexOf_!(_))
    }
    initFields
    // check whether dictionaries exists
    require(checkExists(segcorefopts.wordFile) && checkExists(segcorefopts.wordFeatureFile) &&
      checkExists(segcorefopts.featureFile))
    info("Loading dictionaries from file ...")
    loadIndexer(wordIndexer, segcorefopts.wordFile)
    loadIndexer(wordFeatureIndexer, segcorefopts.wordFeatureFile)
    loadIndexer(featureIndexer, segcorefopts.featureFile)

    info("#words=" + W + " #wordFeatures=" + WF + " #features=" + F + " #labels=" + L)
    initParams

    // initialize generative model
    val origWt = segcorefopts.segTextLearnWeight
    segcorefopts.segTextLearnWeight = 0.0
    batchLearnProbsSegmentationOnly("rexa.init.gen", 1, true, false, false)
    segcorefopts.segTextLearnWeight = origWt

    if (segcorefopts.segGenLearnIter > 0)
      batchLearnProbsSegmentationOnly("rexa.gen", segcorefopts.segGenLearnIter, true, false, false)
    // initialize discriminative model on records only
    if (segcorefopts.segDiscLearnIter > 0)
      fastOnlineLearnWtsSegmentationOnly("rexa.disc", segcorefopts.segDiscLearnIter, false, true, false)
    // train weights using generative model KL-divergence
    if (segcorefopts.segKLDiscLearnIter > 0)
      fastOnlineLearnWtsSegmentationOnly("rexa.kl_disc", segcorefopts.segKLDiscLearnIter, true, true, true)
    if (segcorefopts.segGenLearnIter > 0)
      outputTextSegmentationEval("rexa.gen", true, false)
    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
      outputTextSegmentationEval("rexa.disc", false, true)
    constrainedInference = true
    if (segcorefopts.segGenLearnIter > 0)
      outputTextSegmentationEval("rexa.gen.constrained", true, false)
    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
      outputTextSegmentationEval("rexa.disc.constrained", false, true)
    constrainedInference = false
    if (segcorefopts.segGenLearnIter > 0)
      heldOutOutputTextCorefSegmentationEval("rexa.joint.gen", true, false)
    if (segcorefopts.segDiscLearnIter > 0 || segcorefopts.segKLDiscLearnIter > 0)
      heldOutOutputTextCorefSegmentationEval("rexa.joint.disc", false, true)
    if (segcorefopts.corefSegGenLearnIter > 0)
      batchLearnProbsCorefSegmentation("rexa.coref_seg.gen", segcorefopts.corefSegGenLearnIter, true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      fastOnlineLearnWtsCorefSegmentation("rexa.coref_seg.disc", segcorefopts.corefSegDiscLearnIter, false, true)
    if (segcorefopts.corefSegGenLearnIter > 0)
      outputTextSegmentationEval("rexa.coref_seg.gen", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      outputTextSegmentationEval("rexa.coref_seg.disc", false, true)
    constrainedInference = true
    if (segcorefopts.corefSegGenLearnIter > 0)
      outputTextSegmentationEval("rexa.coref_seg.gen.constrained", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      outputTextSegmentationEval("rexa.coref_seg.disc.constrained", false, true)
    constrainedInference = false
    if (segcorefopts.corefSegGenLearnIter > 0)
      heldOutOutputTextCorefSegmentationEval("rexa.joint.coref_seg.gen", true, false)
    if (segcorefopts.corefSegDiscLearnIter > 0)
      heldOutOutputTextCorefSegmentationEval("rexa.joint.coref_seg.disc", false, true)
  }
}