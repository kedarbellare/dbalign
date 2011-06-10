package cc.refectorie.user.kedarb.dbalign

import java.io.{FileOutputStream, PrintStream}
import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.{ProbStats, Options, InferSpec}
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer, ParamUtils}
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.la.ArrayFromVectors
import optimization.linesearch._
import optimization.gradientBasedMethods.stats._
import optimization.stopCriteria._
import optimization.gradientBasedMethods._
import collection.mutable.ArrayBuffer
import fields.Field
import misc._
import params._
import actors.Actor
import collection.Iterator

/**
 * @author kedarb
 * @since 5/10/11
 */

trait ACorefSegmentationProblem extends ACorefSegmentationEnv with HasLogger {
  type Features = (Int, FtrVec)

  val wordIndexer = new Indexer[String]
  val wordFeatureIndexer = new Indexer[String]
  val featureIndexer = new Indexer[String]
  val labelIndexer = new Indexer[String]
  val fields = new ArrayBuffer[Field]
  val otherLabelIndex = labelIndexer.indexOf_!("O")

  def L = labelIndexer.size

  def lstr(i: Int) = labelIndexer(i)

  def W = wordIndexer.size

  def wstr(i: Int) = wordIndexer(i)

  def F = featureIndexer.size

  def fstr(i: Int) = featureIndexer(i)

  def WF = wordFeatureIndexer.size

  def wfstr(i: Int) = wordFeatureIndexer(i)

  def opts: Options

  /**
   * Segmentation coref options
   */
  def segcorefopts: SegCorefOptions

  /**
   * Model segmentation parameters
   */
  var paramsOpt: Option[Params] = None

  def params: Params = paramsOpt match {
    case Some(x) => x;
    case None => throw fail("No params")
  }

  def params_=(x: Params) = paramsOpt = Some(x)

  /**
   * Iterate over each mention and apply function f
   */
  lazy val numRecords: Int = mentionColl.count(Map("isRecord" -> true)).toInt

  lazy val numTexts: Int = mentionColl.count(Map("isRecord" -> false)).toInt

  lazy val numMentions = numRecords + numTexts

  def augmentRandomKey(rnd: java.util.Random): Unit = {
    var numRecs = 0
    for (mention <- mentionColl.find(Map("isRecord" -> true))) {
      val rndkey = if (numRecs < segcorefopts.maxRecords) rnd.nextInt(numMentions) else Integer.MAX_VALUE
      mentionColl.update(Map("_id" -> mention("_id")), $set("rndkey" -> rndkey))
      numRecs += 1
    }
    var numTxts = 0
    for (mention <- mentionColl.find(Map("isRecord" -> false))) {
      val rndkey = if (numTxts < segcorefopts.maxTexts) rnd.nextInt(numMentions) else Integer.MAX_VALUE
      mentionColl.update(Map("_id" -> mention("_id")), $set("rndkey" -> rndkey))
      numTxts += 1
    }
    mentionColl.ensureIndex(Map("rndkey" -> 1))
    mentionColl.ensureIndex(Map("isRecord" -> 1, "rndkey" -> 1))
  }

  def foreachRecord(f: (DBObject) => Any): Unit = {
    for (mention <- mentionColl.find(Map("isRecord" -> true)).take(segcorefopts.maxRecords)) f(mention)
  }

  def foreachRandomRecord(rnd: java.util.Random, f: (DBObject) => Any): Unit = {
    augmentRandomKey(rnd)
    for (mention <- mentionColl.find(Map("isRecord" -> true)).sort(Map("rndkey" -> 1)).take(segcorefopts.maxRecords))
      f(mention)
  }

  def foreachText(f: (DBObject) => Any): Unit = {
    for (mention <- mentionColl.find(Map("isRecord" -> false)).take(segcorefopts.maxTexts)) f(mention)
  }

  def foreachRandomText(rnd: java.util.Random, f: (DBObject) => Any): Unit = {
    augmentRandomKey(rnd)
    for (mention <- mentionColl.find(Map("isRecord" -> false)).sort(Map("rndkey" -> 1)).take(segcorefopts.maxTexts))
      f(mention)
  }

  def foreachMention(f: (DBObject) => Any): Unit = {
    foreachRecord(f(_))
    foreachText(f(_))
  }

  def foreachRandomMention(rnd: java.util.Random, f: (DBObject) => Any): Unit = {
    foreachRandomRecord(rnd, f(_))
    foreachRandomText(rnd, f(_))
  }

  /**
   * Initialize the fields associated with mentions.
   */
  def initFields: Unit = {
    var count = 0
    // lookup labels from the DB
    foreachRecord {
      mention: DBObject =>
        Segmentation.fromBIO(getStringArray(mention, "trueLabels"), labelIndexer.indexOf_!(_))
        count += 1
        if (count % 10000 == 0) info("Loaded %d records.".format(count))
    }
    info("Done loading %d records.".format(count))
    labelIndexer.lock
    // create fields
    info("Creating fields for labels: " + labelIndexer)
    fields.clear
    for (index <- 0 until L) fields += newField(lstr(index), index)
    // add values to the fields
    count = 0
    foreachRecord {
      mention: DBObject =>
        val words = getStringArray(mention, "words")
        val segmentation = Segmentation.fromBIO(getStringArray(mention, "trueLabels"), labelIndexer.indexOf_?(_))
        forIndex(segmentation.numSegments, {
          s: Int =>
            val segment = segmentation.segment(s)
            fields(segment.label).addValue(mention._id.get, words.slice(segment.begin, segment.end), true)
        })
        count += 1
        if (count % 10000 == 0) info("Loaded %d records.".format(count))
    }
    fields.foreach(fld => info(fld.toString))
  }

  def fmt(segmentation: Segmentation): String = {
    segmentation.segments.map(s => "%s[%d, %d)".format(lstr(s.label), s.begin, s.end)).mkString("[", ", ", "]")
  }

  /**
   * Returns a field for the name.
   */
  def newField(name: String, index: Int): Field

  /**
   * Create dictionary
   */
  class DictionarySlave(val id: Int) extends Actor {
    val wordCounts = new CountVec
    val wordFeatCounts = new CountVec
    val featCounts = new CountVec

    def act() {
      while (true) {
        receive {
          case OnlineMention(exNum, mention, currParams) => {
            getStringArray(mention).foreach(wordCounts.increment_!(_, 1))
            getStringFeatures(mention).foreach(wordFeatCounts.increment_!(_, 1))
            getStringVectorFeatures(mention).foreach {
              feats: Seq[String] => feats.foreach {
                featCounts.increment_!(_, 1)
              }
            }
            if (exNum % 10000 == 0) info("Loaded " + exNum + " mentions.")
          }
          case SlaveDone => {
            reply((wordCounts, wordFeatCounts, featCounts))
            exit()
          }
        }
      }
    }
  }

  def initDictionary: Unit = {
    var count = 0
    val wordFeatCounts = new CountVec
    val featCounts = new CountVec
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new DictionarySlave(id)
      slave.start
      slave
    })
    foreachMention {
      mention: DBObject =>
        val id = count % slaves.size
        count += 1
        slaves(id) ! OnlineMention(count, mention, null)
    }
    for (slave <- slaves) slave !? SlaveDone match {
      case result: (CountVec, CountVec, CountVec) => {
        for (word <- result._1.keys) wordIndexer.indexOf_!(_)
        wordFeatCounts.increment_!(result._2, 1)
        featCounts.increment_!(result._3, 1)
      }
    }
    for ((feat, value) <- wordFeatCounts if value.toInt >= segcorefopts.wordCountThreshold)
      wordFeatureIndexer.indexOf_!(feat)
    for ((feat, value) <- featCounts if value.toInt >= segcorefopts.featureCountThreshold)
      featureIndexer.indexOf_!(feat)
    // wordIndexer.lock
    wordFeatureIndexer.lock
    featureIndexer.lock
  }

  /**
   * Get the feature (vector) sequence for words
   */
  def getFeaturesForMention(mention: DBObject): Array[Features] = {
    val strFeats = getStringFeatures(mention)
    val strSeqFeats = getStringVectorFeatures(mention)
    require(strFeats.size == strSeqFeats.size)
    mapIndex(strFeats.size, (i: Int) => {
      val fi = wordFeatureIndexer.indexOf_?(strFeats(i))
      val fv = new FtrVec
      strSeqFeats(i).map(featureIndexer.indexOf_?(_)).foreach(i => if (i >= 0) fv += i -> 1.0)
      (fi, fv)
    })
  }

  def getTrueSegmentation(mention: DBObject): Segmentation = {
    Segmentation.fromBIO(getStringArray(mention, "trueLabels"), labelIndexer.indexOf_?(_))
  }

  /**
   * Constructor for segmentation params
   */
  def newParams(prDense: Boolean = true, wtDense: Boolean = true): Params = {
    import ParamUtils._
    val genTransitions = new TransitionParams(labelIndexer, newPrVec(prDense, L), newPrVecArray(prDense, L, L))
    val genEmissions = new EmissionParams(labelIndexer, wordFeatureIndexer, newPrVecArray(prDense, L, WF))
    val discTransitions = new TransitionParams(labelIndexer, newWtVec(wtDense, L), newWtVecArray(wtDense, L, L))
    val discEmissions = new EmissionParams(labelIndexer, featureIndexer, newWtVecArray(wtDense, L, F))
    new Params(new SegmentParams(genTransitions, genEmissions), new SegmentParams(discTransitions, discEmissions))
  }

  /**
   * Initialization of parameters
   */
  def initParams: Unit

  /**
   * Output methods
   */
  def adjustSegmentationForPunctuation(words: Array[String], segmentation: Segmentation): Segmentation = {
    val adjSegmentation = new Segmentation(segmentation.length)
    val puncPattern = "^[^A-Za-z0-9]*$"
    forIndex(segmentation.numSegments, {
      s: Int =>
        val segment = segmentation.segment(s)
        val mod_begin = {
          var i = segment.begin
          while (i < segment.end && words(i).matches(puncPattern)) i += 1
          i
        }
        if (mod_begin == segment.end) {
          // whole phrase is punctuations
          require(adjSegmentation.append(Segment(segment.begin, segment.end, otherLabelIndex)))
        } else {
          if (mod_begin > segment.begin) {
            // prefix is punctuation
            require(adjSegmentation.append(Segment(segment.begin, mod_begin, otherLabelIndex)))
          }
          val mod_end = {
            var i = segment.end - 1
            while (i >= mod_begin && words(i).matches(puncPattern)) i -= 1
            i + 1
          }
          if (mod_end == segment.end) {
            // rest is valid
            require(adjSegmentation.append(Segment(mod_begin, segment.end, segment.label)))
          } else {
            // suffix is punctuation
            require(adjSegmentation.append(Segment(mod_begin, mod_end, segment.label)))
            require(adjSegmentation.append(Segment(mod_end, segment.end, otherLabelIndex)))
          }
        }
    })
    adjSegmentation
  }

  def widgetToFullString(mention: DBObject, segmentation: Segmentation): String = {
    val buff = new StringBuffer
    val words = getStringArray(mention)
    buff.append(">>\n>word\n")
    foreachIndex(words, {
      (i: Int, w: String) => buff.append(i).append('\t')
        .append('"').append(w).append('"').append('\n')
    })
    buff.append("\n>mention\n")
    forIndex(segmentation.numSegments, {
      i: Int =>
        val segment = segmentation.segment(i)
        val lbl = lstr(segment.label)
        if (lbl != "O") buff.append(segment.begin).append('\t').append(segment.end - 1).append('\t')
          .append('"').append(lbl).append('"').append('\n')
    })
    buff.append('\n')
    buff.toString
  }

  abstract class CorefSegmentationObjective extends Objective {
    val paramsArrayFromVectors = new ArrayFromVectors(params.getWtVecs)
    var objectiveValue = Double.NaN
    val invVariance = segcorefopts.invVariance

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

    override def setInitialParameters(params: Array[Double]) = setParameters(params)

    def getValueAndGradient: (Params, ProbStats)

    def updateValueAndGradient: Unit = {
      // set parameters as they may have changed
      paramsArrayFromVectors.setVectorsFromArray(parameters)
      val (expectations, stats) = getValueAndGradient
      // output objective
      objectiveValue = 0.5 * params.wtTwoNormSquared * invVariance - stats.logZ
      info("objective=" + objectiveValue)
      // compute gradient
      java.util.Arrays.fill(gradient, 0.0)
      expectations.wtAdd_!(params, -invVariance)
      // point in correct direction
      expectations.wtDiv_!(-1)
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

  /**
   * Parameter learning and inference
   */
  def getSegmentationOnlyExample(mention: DBObject) = {
    new SegmentationOnlyExample(mention._id.get, isRecord(mention), getStringArray(mention),
      getFeaturesForMention(mention), getTrueSegmentation(mention))
  }

  def getCorefSegmentationExample(mention: DBObject) = {
    new CorefSegmentationExample(mention._id.get, isRecord(mention), getStringArray(mention),
      getFeaturesForMention(mention), getTrueSegmentation(mention))
  }

  def inferTrueSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                            useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    val trueInfer = new SegmentationOnlyInferencer(fields, ex, localParams, localCounts,
      InferSpec(0, 1, false, ex.isRecord, false, false, useProbs, useWts, 1, stepSize))
    trueInfer.updateCounts
    trueInfer.stats
  }

  def inferPredSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                            useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    val predInfer = new SegmentationOnlyInferencer(fields, ex, localParams, localCounts,
      InferSpec(0, 1, false, false, false, false, useProbs, useWts, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
  }

  def inferKLPredSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                              stepSize: Double): ProbStats = {
    val predInfer = new KLGenSegmentationOnlyInferencer(fields, ex, localParams, localCounts,
      InferSpec(0, 1, false, false, false, false, true, true, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
  }

  def inferDiscSegmentation(ex: SegmentationOnlyExample, localParams: Params, localCounts: Params,
                            stepSize: Double): ProbStats = {
    val predInfer = new SegmentationOnlyInferencer(fields, ex, localParams, localCounts,
      InferSpec(0, 1, false, false, false, false, false, true, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
  }

  def inferBestSegmentation(ex: SegmentationOnlyExample, localParams: Params,
                            useProbs: Boolean, useWts: Boolean): Segmentation = {
    val bestInfer = new SegmentationOnlyInferencer(fields, ex, localParams, localParams,
      InferSpec(0, 1, false, ex.isRecord, true, false, useProbs, useWts, 1, 0))
    bestInfer.bestWidget
  }

  def inferPredCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                 useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    val predInfer = new CorefSegmentationInferencer(fields, true, ex, localParams, localCounts,
      InferSpec(0, 1, false, false, false, false, useProbs, useWts, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
  }

  def inferDiscCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                 stepSize: Double): ProbStats = {
    val predInfer = new CorefSegmentationInferencer(fields, true, ex, localParams, localCounts,
      InferSpec(0, 1, false, false, false, false, false, true, 1, stepSize))
    predInfer.updateCounts
    predInfer.stats
  }

  def inferBestCorefSegmentation(ex: CorefSegmentationExample, localParams: Params,
                                 useProbs: Boolean, useWts: Boolean): CorefSegmentation = {
    val bestInfer = new CorefSegmentationInferencer(fields, true, ex, localParams, localParams,
      InferSpec(0, 1, false, ex.isRecord, true, false, useProbs, useWts, 1, 0))
    bestInfer.bestWidget
  }

  def learnRecordSegmentation(exNum: Int, mention: DBObject, localParams: Params, localCounts: Params,
                              useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    require(useProbs || useWts)
    val ex = getSegmentationOnlyExample(mention)
    val trueStats = {
      if (useProbs && useWts) inferTrueSegmentation(ex, localParams, localCounts, false, true, stepSize) +
        inferTrueSegmentation(ex, localParams, localCounts, true, false, stepSize)
      else inferTrueSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
    }
    val predStats = {
      if (useWts) inferDiscSegmentation(ex, localParams, localCounts, -stepSize)
      else new ProbStats
    }
    if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Processed " + exNum + " examples.")
    trueStats - predStats
  }

  def learnTextSegmentation(exNum: Int, mention: DBObject, localParams: Params, localCounts: Params,
                            useProbs: Boolean, useWts: Boolean, useKLDiv: Boolean, stepSize: Double): ProbStats = {
    require(useWts || useProbs)
    val ex = getSegmentationOnlyExample(mention)
    if (useWts && !useProbs) {
      // discriminative model only
      new ProbStats
    } else {
      val trueStats = {
        if (useWts && useProbs && useKLDiv) inferKLPredSegmentation(ex, localParams, localCounts, stepSize)
        else inferPredSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
      }
      val predStats = {
        if (useWts) inferDiscSegmentation(ex, localParams, localCounts, -stepSize)
        else new ProbStats
      }
      if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Processed " + exNum + " examples.")
      trueStats - predStats
    }
  }

  def learnTextCorefSegmentation(exNum: Int, mention: DBObject, localParams: Params, localCounts: Params,
                                 useProbs: Boolean, useWts: Boolean, stepSize: Double): ProbStats = {
    val ex = getCorefSegmentationExample(mention)
    val trueStats = inferPredCorefSegmentation(ex, localParams, localCounts, useProbs, useWts, stepSize)
    val predStats = {
      if (useWts) inferDiscSegmentation(getSegmentationOnlyExample(mention), localParams, localCounts, -stepSize)
      else new ProbStats
    }
    if (exNum % segcorefopts.pingIterationCount == 0) info("+++ Processed " + exNum + " examples.")
    trueStats - predStats
  }

  def outputTextSegmentationEval(filePrefix: String, useProbs: Boolean, useWts: Boolean): Unit = {
    val goldWriter = new PrintStream(new FileOutputStream(filePrefix + ".gold.ww"))
    val guessWriter = new PrintStream(new FileOutputStream(filePrefix + ".guess.ww"))
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    foreachText((mention: DBObject) => {
      val ex = getSegmentationOnlyExample(mention)
      val words = ex.words
      val (origTrueSeg, origPredSeg) = (ex.trueSegmentation, inferBestSegmentation(ex, params, useProbs, useWts))
      val trueSeg = adjustSegmentationForPunctuation(words, origTrueSeg)
      val predSeg = adjustSegmentationForPunctuation(words, origPredSeg)
      tokenPerf.add(trueSeg, predSeg)
      segmentPerf.add(trueSeg, predSeg)
      perLabelPerf.add(trueSeg, predSeg)
      goldWriter.println(widgetToFullString(mention, trueSeg))
      guessWriter.println(widgetToFullString(mention, predSeg))
    })
    info("")
    info("*** Evaluating " + filePrefix)
    tokenPerf.output(info(_))
    perLabelPerf.output(info(_))
    segmentPerf.output(info(_))
    goldWriter.close
    guessWriter.close
  }

  def outputTextCorefSegmentationEval(filePrefix: String, useProbs: Boolean, useWts: Boolean): Unit = {
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    var exNum = 0
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BatchCorefSegmentationEvaluationSlave(id, params, useProbs, useWts)
      slave.start
      slave
    })
    foreachText((mention: DBObject) => {
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
  }

  class BatchCorefSegmentationEvaluationSlave(val id: Int, val params: Params, val useProbs: Boolean,
                                              val useWts: Boolean)
    extends Actor {
    val mentionClusters = new ArrayBuffer[Option[String]]()
    val mentionWords = new ArrayBuffer[Array[String]]
    val mentionSegmentations = new ArrayBuffer[Segmentation]
    val corefSegmentations = new ArrayBuffer[CorefSegmentation]

    def act(): Unit = {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {}
          case TextMention(exNum, mention) => {
            val ex = getCorefSegmentationExample(mention)
            mentionWords += ex.words
            mentionSegmentations += ex.trueSegmentation
            corefSegmentations += inferBestCorefSegmentation(ex, params, useProbs, useWts)
            // check coreference performance
            val mentionCluster: Option[String] =
              if (mention.isDefinedAt("trueCluster")) Some(mention("trueCluster").toString)
              else None
            mentionClusters += mentionCluster
            if (exNum % segcorefopts.pingIterationCount == 0)
              info("+++ Processed " + exNum + " examples ...")
          }
          case LearnResult(doExit) => {
            reply((mentionWords.toSeq, mentionSegmentations.toSeq, mentionClusters.toSeq, corefSegmentations.toSeq))
            if (doExit) exit()
          }
        }
      }
    }
  }

  class BatchSegmentationOnlyExpectationSlave(val id: Int, val params: Params, val useProbs: Boolean,
                                              val useWts: Boolean, val useKLDiv: Boolean)
    extends Actor {
    val stats = new ProbStats
    val counts = newParams(useProbs, useWts)

    def act(): Unit = {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {
            try {
              stats += learnRecordSegmentation(exNum, mention, params, counts, useProbs, useWts, 1)
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case TextMention(exNum, mention) => {
            try {
              stats += learnTextSegmentation(exNum, mention, params, counts, useProbs, useWts, useKLDiv,
                segcorefopts.segTextLearnWeight) * segcorefopts.segTextLearnWeight
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit
          }
        }
      }
    }
  }

  class BatchCorefSegmentationExpectationSlave(val id: Int, val params: Params, val useProbs: Boolean,
                                               val useWts: Boolean)
    extends Actor {
    val stats = new ProbStats
    val counts = newParams(useProbs, useWts)

    def act(): Unit = {
      while (true) {
        receive {
          case RecordMention(exNum, mention) => {
            try {
              stats += learnRecordSegmentation(exNum, mention, params, counts, useProbs, useWts, 1)
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case TextMention(exNum, mention) => {
            try {
              stats += learnTextCorefSegmentation(exNum, mention, params, counts, useProbs, useWts,
                segcorefopts.corefSegTextLearnWeight) * segcorefopts.corefSegTextLearnWeight
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit
          }
        }
      }
    }
  }

  class OnlineSegmentationOnlyExpectationSlave(val id: Int, val params: Params, val useProbs: Boolean,
                                               val useWts: Boolean, val useKLDiv: Boolean)
    extends Actor {
    val stats = new ProbStats
    val counts = newParams(false, false)

    def act(): Unit = {
      loop {
        react {
          case RecordMention(exNum, mention) => {
            try {
              stats += learnRecordSegmentation(exNum, mention, params, counts, useProbs, useWts, 1)
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case TextMention(exNum, mention) => {
            try {
              stats += learnTextSegmentation(exNum, mention, params, counts, useProbs, useWts, useKLDiv,
                segcorefopts.segTextLearnWeight) * segcorefopts.segTextLearnWeight
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit
          }
        }
      }
    }
  }

  class OnlineCorefSegmentationExpectationSlave(val id: Int, val params: Params, val useProbs: Boolean,
                                                val useWts: Boolean)
    extends Actor {
    val stats = new ProbStats
    val counts = newParams(false, false)

    def act(): Unit = {
      loop {
        react {
          case RecordMention(exNum, mention) => {
            try {
              stats += learnRecordSegmentation(exNum, mention, params, counts, useProbs, useWts, 1)
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case TextMention(exNum, mention) => {
            try {
              stats += learnTextCorefSegmentation(exNum, mention, params, counts, useProbs, useWts,
                segcorefopts.corefSegTextLearnWeight) * segcorefopts.corefSegTextLearnWeight
            } catch {
              case e: Exception => {}
              case err: Error => {}
            }
          }
          case LearnResult(doExit) => {
            reply((counts, stats))
            if (doExit) exit
          }
        }
      }
    }
  }

  def getSegOnlyBatchExpectations(useProbs: Boolean, useWts: Boolean, useKLDiv: Boolean): (Params, ProbStats) = {
    var exNum = 0
    // create slaves
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BatchSegmentationOnlyExpectationSlave(id, params, useProbs, useWts, useKLDiv)
      slave.start
      slave
    })
    // distribute examples among slaves; each slave calculates the expectation and logZ
    foreachRecord((m: DBObject) => {
      val id = exNum % segcorefopts.numThreads
      exNum += 1
      slaves(id) ! RecordMention(exNum, m)
    })
    if (segcorefopts.segTextLearnWeight > 0)
      foreachText((m: DBObject) => {
        val id = exNum % segcorefopts.numThreads
        exNum += 1
        slaves(id) ! TextMention(exNum, m)
      })
    // gather expectations
    val stats = new ProbStats
    val counts = newParams(useProbs, useWts)
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Params, ProbStats) => {
        counts.add_!(result._1, 1)
        stats += result._2
      }
    }
    (counts, stats)
  }

  def getCorefSegBatchExpectations(useProbs: Boolean, useWts: Boolean): (Params, ProbStats) = {
    var exNum = 0
    // create slaves
    val slaves = mapIndex(segcorefopts.numThreads, (id: Int) => {
      val slave = new BatchCorefSegmentationExpectationSlave(id, params, useProbs, useWts)
      slave.start
      slave
    })
    // distribute examples among slaves; each slave calculates the expectation and logZ
    foreachRecord((m: DBObject) => {
      val id = exNum % segcorefopts.numThreads
      exNum += 1
      slaves(id) ! RecordMention(exNum, m)
    })
    if (segcorefopts.segTextLearnWeight > 0)
      foreachText((m: DBObject) => {
        val id = exNum % segcorefopts.numThreads
        exNum += 1
        slaves(id) ! TextMention(exNum, m)
      })
    // gather expectations
    val stats = new ProbStats
    val counts = newParams(useProbs, useWts)
    for (slave <- slaves) slave !? LearnResult(true) match {
      case result: (Params, ProbStats) => {
        counts.add_!(result._1, 1)
        stats += result._2
      }
    }
    (counts, stats)
  }

  def batchLearnProbsSegmentationOnly(name: String, numIter: Int,
                                      useProbs: Boolean, useWts: Boolean, useKLDiv: Boolean): Unit = {
    forIndex(numIter, (iter: Int) => {
      info("")
      info("*** segmentation learning only epoch=" + iter)
      val (counts, stats) = getSegOnlyBatchExpectations(useProbs, useWts, useKLDiv)
      params.genParams = counts.genParams
      params.normalize_!(opts.initSmoothing)
      info("objective=" + stats.logZ)
      if (segcorefopts.evalIterationCount > 0 && (iter + 1) % segcorefopts.evalIterationCount == 0)
        outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
    })
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def batchLearnProbsCorefSegmentation(name: String, numIter: Int,
                                       useProbs: Boolean, useWts: Boolean): Unit = {
    forIndex(numIter, (iter: Int) => {
      info("")
      info("*** coref+segmentation learning epoch=" + iter)
      val (counts, stats) = getCorefSegBatchExpectations(useProbs, useWts)
      params.genParams = counts.genParams
      params.normalize_!(opts.initSmoothing)
      info("objective=" + stats.logZ)
      if (segcorefopts.evalIterationCount > 0 && (iter + 1) % segcorefopts.evalIterationCount == 0)
        outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
    })
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def batchLearnWtsSegmentationOnly(name: String, numIter: Int,
                                    useProbs: Boolean, useWts: Boolean, useKLDiv: Boolean): Unit = {
    val objective = new CorefSegmentationObjective {
      def getValueAndGradient: (Params, ProbStats) = getSegOnlyBatchExpectations(useProbs, useWts, useKLDiv)
    }
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-3)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective): Unit = {
        super.collectIterationStats(optimizer, objective)
        info("*** finished segmentation learning only epoch=" + optimizer.getCurrentIteration)
        if (segcorefopts.evalIterationCount > 0 &&
          (optimizer.getCurrentIteration + 1) % segcorefopts.evalIterationCount == 0)
          outputTextSegmentationEval(name + ".iter_" + optimizer.getCurrentIteration, useProbs, useWts)
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  class FastOnlineSegmentationMaster(val name: String, val numIter: Int, val numSlaves: Int,
                                     val useProbs: Boolean, val useWts: Boolean, val useKLDiv: Boolean)
    extends Actor {
    var numUpdates = 0.0
    val slaves = mapIndex(numSlaves, (id: Int) => {
      val slave = new FastOnlineSegmentationSlave(id, useProbs, useWts, useKLDiv)
      slave.start()
      slave
    })
    var exNum = 0
    var exIter = newMentionIterator
    var stats = new ProbStats()
    var iter = 0
    val avgParams = newParams(false, true)
    avgParams.genParams = params.genParams
    var done = false

    def newMentionIterator = new Iterator[DBObject] {
      val batchSize = segcorefopts.miniBatchSize
      val numRecs = math.min(numRecords, segcorefopts.maxRecords)
      val numTxts = {
        if (useKLDiv) math.min(numTexts, segcorefopts.maxTexts)
        else 0
      }
      val numRecBatches = (numRecs + batchSize - 1) / batchSize
      val numTxtBatches = (numTxts + batchSize - 1) / batchSize
      // info("#recordBatches=" + numRecBatches + " #textBatches=" + numTxtBatches)

      val recBatchIter = shuffle(segcorefopts.onlineRandom, Range(0, numRecBatches).toArray).iterator
      val txtBatchIter = shuffle(segcorefopts.onlineRandom, Range(0, numTxtBatches).toArray).iterator

      var currBatchIter = Iterator[DBObject]()

      def hasNext: Boolean = {
        if (currBatchIter.hasNext) true
        else if (recBatchIter.hasNext) {
          val currBatchBegin = recBatchIter.next() * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numRecs) - currBatchBegin
          val currBatchArray: Array[DBObject] = mentionColl.find(Map("isRecord" -> true)).skip(currBatchBegin).
            limit(currBatchSize).toArray
          // info("currentBatch=records from " + currBatchBegin + " size=" + currBatchSize)
          currBatchIter = shuffle(segcorefopts.onlineRandom, currBatchArray).iterator
          currBatchIter.hasNext
        } else if (txtBatchIter.hasNext) {
          val currBatchBegin = txtBatchIter.next() * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numTxts) - currBatchBegin
          val currBatchArray: Array[DBObject] = mentionColl.find(Map("isRecord" -> false)).skip(currBatchBegin).
            limit(currBatchSize).toArray
          // info("currentBatch=texts from " + currBatchBegin + " size=" + currBatchSize)
          currBatchIter = shuffle(segcorefopts.onlineRandom, currBatchArray).iterator
          currBatchIter.hasNext
        } else false
      }

      def next(): DBObject = currBatchIter.next()
    }

    def stepSize: Double = {
      val eta = segcorefopts.stepSizeMultiplier /
        math.pow(numUpdates + segcorefopts.stepSizeOffset, segcorefopts.stepSizeReductionPower)
      numUpdates += 1
      eta
    }

    def sendToSlaves() {
      // give example initially to slaves
      forIndex(numSlaves, (id: Int) => {
        if (exIter.hasNext) {
          val m = exIter.next()
          exNum += 1
          slaves(id) ! OnlineMention(exNum, m, params)
        } else {
          slaves(id) ! LearnResult(false)
        }
      })
    }

    def act() {
      loop {
        react {
          case MasterStart => {
            info("")
            info("*** async online segmentation only learning epoch=" + iter)
            sendToSlaves()
          }
          case update: LearnUpdate => {
            params.wtAdd_!(update.counts, stepSize)
            avgParams.wtAdd_!(update.counts, stepSize * numUpdates)
            stats += update.stats
            if (exIter.hasNext) {
              val m = exIter.next()
              exNum += 1
              sender ! OnlineMention(exNum, m, params)
            } else {
              info("Sending learn result to slave")
              sender ! LearnResult(false)
            }
          }
          case SlaveDone => {
            info("#slaves done=" + slaves.count(_.done))
            if (slaves.forall(_.done)) {
              info("objective=" + stats.logZ)
              outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
              exNum = 0
              stats = new ProbStats()
              iter += 1
              if (iter < numIter) {
                exIter = newMentionIterator
                info("")
                info("*** async online segmentation only learning epoch=" + iter)
                sendToSlaves()
              } else {
                // return average parameters
                // params.wtDiv_!(numUpdates / (numUpdates + 1))
                // avgParams.wtDiv_!(numUpdates)
                // params.wtAdd_!(avgParams, -1)
                done = true
                forIndex(numSlaves, slaves(_) ! LearnResult(true))
                exit()
              }
            }
          }
        }
      }
    }
  }

  class FastOnlineSegmentationSlave(val id: Int, val useProbs: Boolean,
                                    val useWts: Boolean, val useKLDiv: Boolean) extends Actor {
    var done = false

    def act() {
      loop {
        react {
          case OnlineMention(exNum, mention, currParams) => {
            done = false
            val stats = new ProbStats()
            val counts = newParams(false, false)
            try {
              if (isRecord(mention)) {
                stats += learnRecordSegmentation(exNum, mention, currParams, counts, useProbs, useWts, 1)
              } else {
                stats += learnTextSegmentation(exNum, mention, currParams, counts, useProbs, useWts, useKLDiv,
                  segcorefopts.segTextLearnWeight) * segcorefopts.segTextLearnWeight
              }
              sender ! LearnUpdate(counts, stats)
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
          }
          case LearnResult(doExit) => {
            info("Before slave " + id + " finished learning: " + done)
            done = true
            info("After slave " + id + " finished learning: " + done)
            sender ! SlaveDone
            if (doExit) exit()
          }
        }
      }
    }
  }

  def fastOnlineLearnWtsSegmentationOnly(name: String, numIter: Int, useProbs: Boolean,
                                         useWts: Boolean, useKLDiv: Boolean): Unit = {
    val master = new FastOnlineSegmentationMaster(name, numIter, segcorefopts.numThreads,
      useProbs, useWts, useKLDiv)
    master.start()
    master ! MasterStart
    while (!master.done) {
      info("Master sleeping")
      Thread.sleep(10000)
    }
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def miniBatchLearnWtsSegmentationOnly(name: String, numIter: Int,
                                        useProbs: Boolean, useWts: Boolean,
                                        useKLDiv: Boolean): Unit = {
    val batchSize = segcorefopts.miniBatchSize
    val numRecs = math.min(numRecords, segcorefopts.maxRecords)
    val numTxts = math.min(numTexts, segcorefopts.maxTexts)
    val numRecBatches = (numRecs + batchSize - 1) / batchSize
    val numTxtBatches = (numTxts + batchSize - 1) / batchSize
    val numSlaves = math.min(batchSize, segcorefopts.numThreads)
    var numUpdates = 0.0
    def stepSize: Double = {
      val eta = segcorefopts.stepSizeMultiplier /
        math.pow(numUpdates + segcorefopts.stepSizeOffset, segcorefopts.stepSizeReductionPower)
      numUpdates += 1
      eta
    }
    forIndex(numIter, (iter: Int) => {
      info("")
      info("*** segmentation learning epoch=" + iter)
      var exNum = 0
      val stats = new ProbStats()
      // augmentRandomKey(segcorefopts.onlineRandom)
      forIndex(numRecBatches, (b: Int) => {
        val currBatchBegin = b * batchSize
        val currBatchSize = math.min(currBatchBegin + batchSize, numRecs) - currBatchBegin
        val slaves = mapIndex(numSlaves, (id: Int) => {
          val slave = new OnlineSegmentationOnlyExpectationSlave(id, params, useProbs, useWts, useKLDiv)
          slave.start
          slave
        })
        var currEx = 0
        for (m <- mentionColl.find(Map("isRecord" -> true)).drop(currBatchBegin).take(currBatchSize)) {
          val id = currEx % slaves.size
          currEx += 1
          exNum += 1
          slaves(id) ! RecordMention(exNum, m)
        }
        val eta = stepSize
        for (slave <- slaves) slave !? LearnResult(true) match {
          case result: (Params, ProbStats) => {
            params.discParams.add_!(result._1.discParams, eta)
            stats += result._2
          }
        }
      })
      if (segcorefopts.segTextLearnWeight > 0)
        forIndex(numTxtBatches, (b: Int) => {
          val currBatchBegin = b * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numTxts) - currBatchBegin
          val slaves = mapIndex(numSlaves, (id: Int) => {
            val slave = new OnlineSegmentationOnlyExpectationSlave(id, params, useProbs, useWts, useKLDiv)
            slave.start
            slave
          })
          var currEx = 0
          for (m <- mentionColl.find(Map("isRecord" -> false)).drop(currBatchBegin).take(currBatchSize)) {
            val id = currEx % slaves.size
            currEx += 1
            exNum += 1
            slaves(id) ! TextMention(exNum, m)
          }
          val eta = stepSize
          for (slave <- slaves) slave !? LearnResult(true) match {
            case result: (Params, ProbStats) => {
              params.discParams.add_!(result._1.discParams, eta)
              stats += result._2
            }
          }
        })
      info("objective=" + stats.logZ)
      if (segcorefopts.evalIterationCount > 0 && (iter + 1) % segcorefopts.evalIterationCount == 0)
        outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
    })
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def batchLearnWtsCorefSegmentation(name: String, numIter: Int,
                                     useProbs: Boolean, useWts: Boolean): Unit = {
    val objective = new CorefSegmentationObjective {
      def getValueAndGradient: (Params, ProbStats) = getCorefSegBatchExpectations(useProbs, useWts)
    }
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
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def onlineLearnWtsCorefSegmentation(name: String, numIter: Int,
                                      useProbs: Boolean, useWts: Boolean): Unit = {
    var numUpdates = 0.0
    def stepSize: Double = {
      val eta = segcorefopts.stepSizeMultiplier /
        math.pow(numUpdates + segcorefopts.stepSizeOffset, segcorefopts.stepSizeReductionPower)
      numUpdates += 1
      eta
    }
    forIndex(numIter, (iter: Int) => {
      info("")
      info("*** coref+segmentation learning epoch=" + iter)
      var exNum = 0
      val stats = new ProbStats()
      foreachRandomRecord(segcorefopts.onlineRandom, (m: DBObject) => {
        exNum += 1
        val counts = newParams(false, false)
        stats += learnRecordSegmentation(exNum, m, params, counts, useProbs, useWts, stepSize)
        params.discParams.add_!(counts.discParams, 1)
      })
      foreachRandomText(segcorefopts.onlineRandom, (m: DBObject) => {
        exNum += 1
        val counts = newParams(false, false)
        stats += learnTextCorefSegmentation(exNum, m, params, counts, useProbs, useWts,
          stepSize * segcorefopts.corefSegTextLearnWeight)
        params.discParams.add_!(counts.discParams, 1)
      })
      info("objective=" + stats.logZ)
      if (segcorefopts.evalIterationCount > 0 && (iter + 1) % segcorefopts.evalIterationCount == 0)
        outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
    })
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  def miniBatchLearnWtsCorefSegmentation(name: String, numIter: Int,
                                         useProbs: Boolean, useWts: Boolean): Unit = {
    val batchSize = segcorefopts.miniBatchSize
    val numRecs = math.min(numRecords, segcorefopts.maxRecords)
    val numTxts = math.min(numTexts, segcorefopts.maxTexts)
    val numRecBatches = (numRecs + batchSize - 1) / batchSize
    val numTxtBatches = (numTxts + batchSize - 1) / batchSize
    val numSlaves = math.min(batchSize, segcorefopts.numThreads)
    var numUpdates = 0.0
    def stepSize: Double = {
      val eta = segcorefopts.stepSizeMultiplier /
        math.pow(numUpdates + segcorefopts.stepSizeOffset, segcorefopts.stepSizeReductionPower)
      numUpdates += 1
      eta
    }
    forIndex(numIter, (iter: Int) => {
      info("")
      info("*** coref+segmentation learning epoch=" + iter)
      var exNum = 0
      val stats = new ProbStats()
      // augmentRandomKey(segcorefopts.onlineRandom)
      forIndex(numRecBatches, (b: Int) => {
        val currBatchBegin = b * batchSize
        val currBatchSize = math.min(currBatchBegin + batchSize, numRecs) - currBatchBegin
        val slaves = mapIndex(numSlaves, (id: Int) => {
          val slave = new OnlineCorefSegmentationExpectationSlave(id, params, useProbs, useWts)
          slave.start
          slave
        })
        var currEx = 0
        for (m <- mentionColl.find(Map("isRecord" -> true)).drop(currBatchBegin).take(currBatchSize)) {
          val id = currEx % slaves.size
          currEx += 1
          exNum += 1
          slaves(id) ! RecordMention(exNum, m)
        }
        val eta = stepSize
        for (slave <- slaves) slave !? LearnResult(true) match {
          case result: (Params, ProbStats) => {
            params.discParams.add_!(result._1.discParams, eta)
            stats += result._2
          }
        }
      })
      if (segcorefopts.corefSegTextLearnWeight > 0)
        forIndex(numTxtBatches, (b: Int) => {
          val currBatchBegin = b * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numTxts) - currBatchBegin
          val slaves = mapIndex(numSlaves, (id: Int) => {
            val slave = new OnlineCorefSegmentationExpectationSlave(id, params, useProbs, useWts)
            slave.start
            slave
          })
          var currEx = 0
          for (m <- mentionColl.find(Map("isRecord" -> false)).drop(currBatchBegin).take(currBatchSize)) {
            val id = currEx % slaves.size
            currEx += 1
            exNum += 1
            slaves(id) ! TextMention(exNum, m)
          }
          val eta = stepSize
          for (slave <- slaves) slave !? LearnResult(true) match {
            case result: (Params, ProbStats) => {
              params.discParams.add_!(result._1.discParams, eta)
              stats += result._2
            }
          }
        })
      info("objective=" + stats.logZ)
      if (segcorefopts.evalIterationCount > 0 && (iter + 1) % segcorefopts.evalIterationCount == 0)
        outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
    })
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  class FastOnlineCorefSegmentationMaster(val name: String, val numIter: Int, val numSlaves: Int,
                                          val useProbs: Boolean, val useWts: Boolean)
    extends Actor {
    var numUpdates = 0.0
    val slaves = mapIndex(numSlaves, (id: Int) => {
      val slave = new FastOnlineCorefSegmentationSlave(id, useProbs, useWts)
      slave.start()
      slave
    })
    var exNum = 0
    var exIter = newMentionIterator
    var stats = new ProbStats()
    var iter = 0
    val avgParams = newParams(false, true)
    avgParams.genParams = params.genParams
    var done = false

    def newMentionIterator = new Iterator[DBObject] {
      val batchSize = segcorefopts.miniBatchSize
      val numRecs = math.min(numRecords, segcorefopts.maxRecords)
      val numTxts = math.min(numTexts, segcorefopts.maxTexts)
      val numRecBatches = (numRecs + batchSize - 1) / batchSize
      val numTxtBatches = (numTxts + batchSize - 1) / batchSize
      // info("#recordBatches=" + numRecBatches + " #textBatches=" + numTxtBatches)

      val recBatchIter = shuffle(segcorefopts.onlineRandom, Range(0, numRecBatches).toArray).iterator
      val txtBatchIter = shuffle(segcorefopts.onlineRandom, Range(0, numTxtBatches).toArray).iterator

      var currBatchIter = Iterator[DBObject]()

      def hasNext: Boolean = {
        if (currBatchIter.hasNext) true
        else if (recBatchIter.hasNext) {
          val currBatchBegin = recBatchIter.next() * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numRecs) - currBatchBegin
          val currBatchArray: Array[DBObject] = mentionColl.find(Map("isRecord" -> true)).skip(currBatchBegin).
            limit(currBatchSize).toArray
          // info("currentBatch=records from " + currBatchBegin + " size=" + currBatchSize)
          currBatchIter = shuffle(segcorefopts.onlineRandom, currBatchArray).iterator
          currBatchIter.hasNext
        } else if (txtBatchIter.hasNext) {
          val currBatchBegin = txtBatchIter.next() * batchSize
          val currBatchSize = math.min(currBatchBegin + batchSize, numTxts) - currBatchBegin
          val currBatchArray: Array[DBObject] = mentionColl.find(Map("isRecord" -> false)).skip(currBatchBegin).
            limit(currBatchSize).toArray
          // info("currentBatch=texts from " + currBatchBegin + " size=" + currBatchSize)
          currBatchIter = shuffle(segcorefopts.onlineRandom, currBatchArray).iterator
          currBatchIter.hasNext
        } else false
      }

      def next(): DBObject = currBatchIter.next()
    }

    def stepSize: Double = {
      val eta = segcorefopts.stepSizeMultiplier /
        math.pow(numUpdates + segcorefopts.stepSizeOffset, segcorefopts.stepSizeReductionPower)
      numUpdates += 1
      eta
    }

    def sendToSlaves() {
      // give example initially to slaves
      forIndex(numSlaves, (id: Int) => {
        if (exIter.hasNext) {
          val m = exIter.next()
          exNum += 1
          slaves(id) ! OnlineMention(exNum, m, params)
        } else {
          slaves(id) ! LearnResult(false)
        }
      })
    }

    def act() {
      loop {
        react {
          case MasterStart => {
            info("")
            info("*** async online coref-segmentation learning epoch=" + iter)
            sendToSlaves()
          }
          case update: LearnUpdate => {
            params.wtAdd_!(update.counts, stepSize)
            avgParams.wtAdd_!(update.counts, stepSize * numUpdates)
            stats += update.stats
            if (exIter.hasNext) {
              val m = exIter.next()
              exNum += 1
              sender ! OnlineMention(exNum, m, params)
            } else {
              sender ! LearnResult(false)
            }
          }
          case SlaveDone => {
            info("#slaves done=" + slaves.count(_.done))
            if (slaves.forall(_.done)) {
              info("objective=" + stats.logZ)
              outputTextSegmentationEval(name + ".iter_" + iter, useProbs, useWts)
              exNum = 0
              stats = new ProbStats()
              iter += 1
              if (iter < numIter) {
                exIter = newMentionIterator
                info("")
                info("*** async online coref-segmentation learning epoch=" + iter)
                sendToSlaves()
              } else {
                // return average parameters
                // params.wtDiv_!(numUpdates / (numUpdates + 1))
                // avgParams.wtDiv_!(numUpdates)
                // params.wtAdd_!(avgParams, -1)
                done = true
                forIndex(numSlaves, slaves(_) ! LearnResult(true))
                exit()
              }
            }
          }
        }
      }
    }
  }

  class FastOnlineCorefSegmentationSlave(val id: Int, val useProbs: Boolean, val useWts: Boolean)
    extends Actor {
    var done = false

    def act() {
      loop {
        react {
          case OnlineMention(exNum, mention, currParams) => {
            done = false
            val stats = new ProbStats()
            val counts = newParams(false, false)
            try {
              if (isRecord(mention)) {
                stats += learnRecordSegmentation(exNum, mention, currParams, counts, useProbs, useWts, 1)
              } else {
                stats += learnTextCorefSegmentation(exNum, mention, currParams, counts, useProbs, useWts,
                  segcorefopts.corefSegTextLearnWeight) * segcorefopts.corefSegTextLearnWeight
              }
              sender ! LearnUpdate(counts, stats)
            } catch {
              case e: Exception => {
                info("caught exception: " + e)
              }
              case err: Error => {
                info("caught error: " + err)
              }
            }
          }
          case LearnResult(doExit) => {
            info("Before slave " + id + " finished learning: " + done)
            done = true
            info("After slave " + id + " finished learning: " + done)
            sender ! SlaveDone
            if (doExit) exit()
          }
        }
      }
    }
  }

  def fastOnlineLearnWtsCorefSegmentation(name: String, numIter: Int, useProbs: Boolean,
                                          useWts: Boolean): Unit = {
    val master = new FastOnlineCorefSegmentationMaster(name, numIter, segcorefopts.numThreads, useProbs, useWts)
    master.start()
    master ! MasterStart
    while (!master.done) {
      info("Master sleeping")
      Thread.sleep(10000)
    }
    outputTextSegmentationEval(name, useProbs, useWts)
  }

  /**
   * Helper objects for parallelization
   */
  case class RecordMention(exNum: Int, mention: DBObject)

  case class TextMention(exNum: Int, mention: DBObject)

  case class LearnResult(doExit: Boolean)

  // for online learning
  case object MasterStart

  case object SlaveDone

  case class OnlineMention(exNum: Int, mention: DBObject, currParams: Params)

  case class LearnUpdate(counts: Params, stats: ProbStats)

}
