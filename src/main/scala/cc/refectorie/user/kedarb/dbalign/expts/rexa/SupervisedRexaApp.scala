package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.{ProbStats, Options, InferSpec}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.{IndexerUtils, Indexer}
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.tools.opts.OptParser
import collection.mutable.HashMap
import cc.refectorie.user.kedarb.dbalign._
import params._
import fields._
import io.Source
import java.io._

/**
 * @author kedarb
 * @since 5/26/11
 */

object SupervisedRexaApp extends ACorefSegmentationProblem with DBLPRexaEnv with RexaConstrainedInferencer {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val opts = new Options
  val segcorefopts = new SegCorefOptions
  val oovIndex = wordFeatureIndexer.indexOf_!("$OOV$")
  val constraintParamsMap = new HashMap[ObjectId, Array[Double]]

  var constrainedInference = false

  def lindex(s: String): Int = labelIndexer.indexOf_?(s)

  def newField(name: String, index: Int): Field = {
    FixedLengthOtherField(name, index, 1000)
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

  override def foreachRandomText(rnd: java.util.Random, f: (DBObject) => Any) {
    val mentions = getRexaTestMentions.toArray
    val numTrain = mentions.size - (mentions.size * segcorefopts.rexaTestProportion).toInt
    val trainMentions = mentions.take(numTrain).toArray
    shuffle(rnd, trainMentions).foreach(f(_))
  }

  override def foreachText(f: (DBObject) => Any) {
    val mentions = getRexaTestMentions.toArray
    val numTrain = mentions.size - (mentions.size * segcorefopts.rexaTestProportion).toInt
    val trainMentions = mentions.take(numTrain).toArray
    trainMentions.foreach(f(_))
  }

  // ignore records
  override def foreachRandomRecord(rnd: java.util.Random, f: (DBObject) => Any) {}

  override def foreachRecord(f: (DBObject) => Any) {}

  override def getSegmentationOnlyExample(mention: DBObject) = {
    new SegmentationOnlyExample(mention._id.get, isRecord(mention), getStringArray(mention),
      getFeaturesForMention(mention).map((f: Features) => if (f._1 >= 0) f else (oovIndex, f._2)),
      getTrueSegmentation(mention)) {
      override def isPossibleEnd(j: Int): Boolean = SupervisedRexaApp.isPossibleEnd(j, words)
    }
  }

  def getCorefSegmentationExample(ex: SegmentationOnlyExample): CorefSegmentationExample = {
    new CorefSegmentationExample(ex._id, ex.isRecord, ex.words, ex.features, ex.trueSegmentation) {
      override def isPossibleEnd(j: Int): Boolean = SupervisedRexaApp.isPossibleEnd(j, words)
    }
  }

  override def inferPredSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                                     useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    val predInfer = new SegmentationOnlyInferencer(fields, ex, localParams, localCounts,
      InferSpec(0, 1, false, true, false, false, useProbs, useWts, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
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

  override def learnTextSegmentation(exNum: Int, mention: DBObject, localParams: Params, localCounts: Params,
                                     useProbs: Boolean, useWts: Boolean, useKLDiv: Boolean,
                                     stepSize: Double): ProbStats = {
    require(useWts || useProbs)
    val ex = getSegmentationOnlyExample(mention)
    val trueStats = inferPredSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
    val predStats = {
      if (useWts) inferDiscSegmentation(ex, localParams, localCounts, -stepSize)
      else new ProbStats
    }
    if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Processed " + exNum + " examples.")
    trueStats - predStats
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

  def loadIndexer(indexer: Indexer[String], file: String): Unit = {
    val in = new BufferedReader(new FileReader(file))
    IndexerUtils.deserializeToIndexerString(in.readLine(), indexer)
    in.close
  }

  def checkExists(file: String): Boolean = new File(file).exists()

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
    if (segcorefopts.segGenLearnIter > 0) {
      batchLearnProbsSegmentationOnly("rexa.supervised.gen", segcorefopts.segGenLearnIter, true, false, false)
      constrainedInference = true
      outputTextSegmentationEval("rexa.supervised.gen.constrained", true, false)
      constrainedInference = false
    }
    // initialize discriminative model on records only
    if (segcorefopts.segDiscLearnIter > 0) {
      batchLearnWtsSegmentationOnly("rexa.supervised.disc", segcorefopts.segDiscLearnIter, false, true, false)
      constrainedInference = true
      outputTextSegmentationEval("rexa.supervised.disc.constrained", false, true)
      constrainedInference = false
    }
  }
}