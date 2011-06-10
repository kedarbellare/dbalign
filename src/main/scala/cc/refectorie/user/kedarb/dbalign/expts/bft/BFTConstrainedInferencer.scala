package cc.refectorie.user.kedarb.dbalign.expts.bft

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.{ProbStats, InferSpec}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import optimization.projections._
import optimization.stopCriteria._
import optimization.gradientBasedMethods._
import optimization.gradientBasedMethods.stats._
import optimization.linesearch._
import collection.mutable.{HashMap, HashSet, ArrayBuffer}
import cc.refectorie.user.kedarb.dbalign._
import misc._
import params._
import fields._

/**
 * @author kedarb
 * @since 5/27/11
 */

trait BFTConstrainedInferencer {
  def constraintParamsMap: HashMap[ObjectId, Array[Double]]

  def info(msg: Any): Unit

  def lstr(a: Int): String

  def fields: ArrayBuffer[Field]

  def getCachedSegmentFeatSet(len: Int, f: (Int, Int) => Boolean): HashSet[(Int, Int)] = {
    val set = new HashSet[(Int, Int)]
    forIndex(len, (i: Int) => {
      forIndex(i + 1, len + 1, (j: Int) => {
        if (f(i, j)) set += i -> j
      })
    })
    set
  }

  val constraintFeatureIndexer = new Indexer[Symbol]
  val ratef = constraintFeatureIndexer.indexOf_!('starRatingAndNotMatchesPattern)
  val notratef = constraintFeatureIndexer.indexOf_!('notStarRatingAndMatchesPattern)
  val nototherf = constraintFeatureIndexer.indexOf_!('notOtherAndMatchesPattern)
  val singleHotelWordf = constraintFeatureIndexer.indexOf_!('singleHotelStopWord)
  val abbrf = constraintFeatureIndexer.indexOf_!('abbrMatch)
  val simf = constraintFeatureIndexer.indexOf_!('simMatch)
  constraintFeatureIndexer.lock
  val numConstraintFeats = constraintFeatureIndexer.size

  def newConstraintParams = new Array[Double](numConstraintFeats)

  def bftInferPredCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                    useProbs: Boolean, useWts: Boolean, stepSize: Double,
                                    onlySegmentation: Boolean, computeBestSegmentation: Boolean,
                                    outputConstraintParams: Boolean,
                                    updateConstraintParams: Boolean = true): (ProbStats, Segmentation, CorefSegmentation) = {
    val words = ex.words
    var possRecordIds: HashSet[ObjectId] = null
    val constraintParams = constraintParamsMap.getOrElse(ex._id, newConstraintParams)
    val len = words.size

    // caching for speed
    val cachedSimScores = new HashMap[(String, ObjectId, Seq[String]), Double]
    def getSimScore(field: Field, fieldClusterId: ObjectId, phrase: Seq[String]): Double = {
      val key = (field.name, fieldClusterId, phrase)
      if (!cachedSimScores.contains(key)) cachedSimScores(key) = field.simScore(fieldClusterId, phrase)
      cachedSimScores(key)
    }

    val cachedAbbrevMatch = new HashMap[(String, ObjectId), Boolean]
    def getAbbrevMatch(field: Field, fieldClusterId: ObjectId, phrase: Seq[String]): Boolean = {
      if (field.name != "localarea") return false
      else if (phrase.length != 1) return false
      else {
        val key = (phrase(0), fieldClusterId)
        if (!cachedAbbrevMatch.contains(key))
          cachedAbbrevMatch(key) = PhraseSimFunc.isAbbrev(phrase(0), field.getValuePhrase(fieldClusterId))
        cachedAbbrevMatch(key)
      }
    }

    // constraint features
    val _cacheRatingPatternMatches = mapIndex(len, (k: Int) => words(k).matches("^\\d.*\\*$"))
    def getNotMatchesPatternStar(a: Int, k: Int): Boolean = {
      lstr(a) == "starrating" && !_cacheRatingPatternMatches(k)
    }

    def getMatchesPatternNotStar(a: Int, k: Int): Boolean = {
      lstr(a) != "starrating" && _cacheRatingPatternMatches(k)
    }

    val _cacheOtherPatternMatches = mapIndex(len, (k: Int) => BFTSgml2Owpl.simplify(words(k)) != words(k))
    def getMatchesPatternNotOther(a: Int, k: Int): Boolean = {
      lstr(a) != "O" && _cacheOtherPatternMatches(k)
    }

    val hotelStopWords = Set("hotel", "hotels", "resort", "resorts", "in", "or")
    val _cacheHotelStopWords = mapIndex(len, (k: Int) => hotelStopWords(words(k)))
    def getHotelStopWord(a: Int, i: Int, j: Int): Boolean = {
      lstr(a) == "hotelname" && (j - i) == 1 && _cacheHotelStopWords(i)
    }

    class LocalCorefSegmentationInferencer(val doUpdate: Boolean, override val ispec: InferSpec)
      extends CorefSegmentationInferencer(fields, true, ex, localParams, localCounts, ispec) {
      val uniqMatch = new HashSet[Any]
      val b = newConstraintParams
      val constraintCounts = newConstraintParams
      val regularization = newConstraintParams

      override def getPossibleRecordClusterIds(join: Boolean): HashSet[ObjectId] = {
        if (possRecordIds == null) {
          if (onlySegmentation) possRecordIds = new HashSet[ObjectId]
          else possRecordIds = super.getPossibleRecordClusterIds(join)
        }
        possRecordIds
      }

      override def scoreRecordCluster(recordClusterId: Option[ObjectId]): Double = {
        if (onlySegmentation) 0.0
        else super.scoreRecordCluster(recordClusterId)
      }

      override def updateRecordCluster(recordClusterId: Option[ObjectId], x: Double): Unit = {
        if (!onlySegmentation && doUpdate) super.updateRecordCluster(recordClusterId, x)
      }

      def setConstraint(cfeat: Int, bval: Double, count: Double, regVal: Double = 0.01): Unit = {
        b(cfeat) = bval
        regularization(cfeat) = regVal
        constraintCounts(cfeat) += count
      }

      override def scoreEmission(a: Int, i: Int, j: Int): Double = {
        var emitScore = super.scoreEmission(a, i, j)
        if (getHotelStopWord(a, i, j)) emitScore -= constraintParams(singleHotelWordf)
        forIndex(i, j, (k: Int) => {
          if (getNotMatchesPatternStar(a, k)) emitScore -= constraintParams(ratef)
          if (getMatchesPatternNotStar(a, k)) emitScore -= constraintParams(notratef)
          if (getMatchesPatternNotOther(a, k)) emitScore -= constraintParams(nototherf)
        })
        emitScore
      }

      override def updateEmission(a: Int, i: Int, j: Int, x: Double): Unit = {
        if (getHotelStopWord(a, i, j)) setConstraint(singleHotelWordf, 0, -x)
        forIndex(i, j, (k: Int) => {
          if (getNotMatchesPatternStar(a, k)) setConstraint(ratef, 0, -x)
          if (getMatchesPatternNotStar(a, k)) setConstraint(notratef, 0, -x)
          if (getMatchesPatternNotOther(a, k)) setConstraint(nototherf, 0, -x)
        })
        if (doUpdate) super.updateEmission(a, i, j, x)
      }

      override def updateTransition(a: Int, nexta: Int, i: Int, j: Int, x: Double): Unit = {
        if (doUpdate) super.updateTransition(a, nexta, i, j, x)
      }

      override def updateStart(a: Int, j: Int, x: Double): Unit = {
        if (doUpdate) super.updateStart(a, j, x)
      }

      override def updateSingleEmission(a: Int, k: Int, x: Double): Unit = {
        if (doUpdate) super.updateSingleEmission(a, k, x)
      }

      override def scoreFieldClusterOverlap(recordClusterId: ObjectId, field: Field,
                                            fieldClusterId: ObjectId, i: Int, j: Int): Double = {
        var overlapScore = 0.0
        if (field.name != "O") {
          val phrase = words.slice(i, j)
          val simScore = getSimScore(field, fieldClusterId, phrase)
          val abbrMatch = getAbbrevMatch(field, fieldClusterId, phrase)
          if (abbrMatch) {
            overlapScore += constraintParams(abbrf)
          } else if (simScore > field.simThreshold) {
            forIndex(i, j, (k: Int) => {
              if (getSimScore(field, fieldClusterId, Seq(words(k))) > 0) {
                overlapScore += constraintParams(simf)
              }
            })
          }
        }
        overlapScore
      }

      override def updateFieldClusterOverlap(recordClusterId: ObjectId, field: Field,
                                             fieldClusterId: ObjectId, i: Int, j: Int, x: Double): Unit = {
        if (field.name != "O") {
          val phrase = words.slice(i, j)
          val simScore = getSimScore(field, fieldClusterId, phrase)
          val abbrMatch = getAbbrevMatch(field, fieldClusterId, phrase)
          if (abbrMatch) {
            val uniqAbbrf = ('abbrMatch, i)
            if (!uniqMatch(uniqAbbrf)) {
              uniqMatch += uniqAbbrf
              b(abbrf) = b(abbrf) - 0.99
            }
            regularization(abbrf) = 0.01
            constraintCounts(abbrf) += x
          } else if (simScore > field.simThreshold) {
            forIndex(i, j, (k: Int) => {
              if (getSimScore(field, fieldClusterId, Seq(words(k))) > 0) {
                val uniqSimf = ('simMatch, k)
                if (!uniqMatch(uniqSimf)) {
                  uniqMatch += uniqSimf
                  b(simf) = b(simf) - 0.99
                }
                regularization(simf) = 0.01
                constraintCounts(simf) += x
              }
            })
          }
        }
      }
    }

    // inferencer with constraints
    def newInferencer(doUpdate: Boolean) =
      new LocalCorefSegmentationInferencer(doUpdate,
        InferSpec(0, 1, false, false, computeBestSegmentation && doUpdate, false, useProbs, useWts, 1, stepSize))

    // optimize by projected gradient descent
    if (updateConstraintParams) {
      val projection = new BoundsProjection(0, Double.PositiveInfinity)
      // create objective
      val objective = new ProjectedObjective {
        var objectiveValue = Double.NaN
        parameters = constraintParams
        gradient = newConstraintParams

        override def setParameter(index: Int, value: Double) = {
          updateCalls += 1
          objectiveValue = Double.NaN
          parameters(index) = value
        }

        override def setParameters(params: Array[Double]) = {
          updateCalls += 1
          objectiveValue = Double.NaN
          require(params.length == getNumParameters)
          Array.copy(params, 0, parameters, 0, params.length)
        }

        def updateValueAndGradient: Unit = {
          // calculate expectations
          java.util.Arrays.fill(gradient, 0.0)
          val inferencer = newInferencer(false)
          inferencer.updateCounts
          // objective and regularization
          objectiveValue = inferencer.logZ
          forIndex(parameters.length, (f: Int) => {
            objectiveValue += inferencer.b(f) * constraintParams(f) +
              0.5 * inferencer.regularization(f) * constraintParams(f) * constraintParams(f)
            gradient(f) = inferencer.b(f) + inferencer.constraintCounts(f) +
              inferencer.regularization(f) * constraintParams(f)
          })
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

        def projectPoint(point: Array[Double]) = {
          projection.project(point)
          point
        }

        override def toString = objectiveValue.toString
      }
      val ls = new ArmijoLineSearchMinimizationAlongProjectionArc(new InterpolationPickFirstStep(1))
      val optimizer = new ProjectedGradientDescent(ls)
      optimizer.setMaxIterations(10)
      optimizer.optimize(objective, new OptimizerStats, new AverageValueDifference(1e-3))
      constraintParamsMap.update(ex._id, constraintParams)
    }
    if (outputConstraintParams) {
      info("")
      info("constraintParams:\n" + constraintParams.mkString("\n"))
    }

    val predInfer = newInferencer(true)
    predInfer.updateCounts
    (predInfer.stats, predInfer.bestWidget.getSegmentation, predInfer.bestWidget)
  }
}