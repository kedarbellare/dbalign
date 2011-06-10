package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.{ProbStats, InferSpec}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import cc.refectorie.user.kedarb.dynprog.segment._
import optimization.projections._
import optimization.stopCriteria._
import optimization.gradientBasedMethods._
import optimization.linesearch._
import collection.mutable.{HashMap, HashSet, ArrayBuffer}
import cc.refectorie.user.kedarb.dbalign._
import misc._
import params._
import fields._
import stats._

/**
 * @author kedarb
 * @since 5/26/11
 */

trait RexaConstrainedInferencer {
  def constraintParamsMap: HashMap[ObjectId, Array[Double]]

  def info(msg: Any): Unit

  def lstr(a: Int): String

  def lindex(s: String): Int

  def fields: ArrayBuffer[Field]

  def isPossibleEnd(j: Int, words: Array[String]): Boolean

  def getCachedSegmentFeatArray(len: Int, ends: Array[Boolean], f: (Int, Int) => Boolean) =
    mapIndex(len, (i: Int) => mapIndex(len + 1, (j: Int) => j > i && ends(j) && f(i, j)))

  def getCachedLblSet(lbls: Seq[String]): HashSet[String] = {
    val set = new HashSet[String]
    set ++= lbls
    set
  }

  val techWordSet = getCachedLblSet(Seq("thesis", "tech", "technical", "report", "ph.d.", "ph.d"))
  val booktitleWordSet = getCachedLblSet(Seq("Proc.", "Proceedings", "Conference", "Workshop"))
  val journalWordSet = getCachedLblSet(Seq("Journal", "IEEE", "Annals"))
  val noteWordSet = getCachedLblSet(Seq("submitted", "appear"))
  val seriesWordSet = getCachedLblSet(Seq("LNCS", "LNAI", "Lecture", "Lectures"))
  val instituteWordSet = getCachedLblSet(Seq("University", "Department", "Univ", "Dept", "Univ.", "Dept.", "Institute",
    "Corporation", "Laboratory"))
  val publisherWordSet = getCachedLblSet(Seq("Press", "Prentice", "Publishers", "Springer", "Kluwer"))

  // constraint features
  val constraintFeatureIndexer = new Indexer[Symbol]
  val simf = constraintFeatureIndexer.indexOf_!('simMatch)
  val invalidStartf = constraintFeatureIndexer.indexOf_!('invalidStart)
  val singleStopwordf = constraintFeatureIndexer.indexOf_!('singleStopwordIsNotOther)
  val refMarkerf = constraintFeatureIndexer.indexOf_!('refMarkerIsNotOther)
  val countTitlef = constraintFeatureIndexer.indexOf_!('countTitleAtMostOne)
  val countBooktitlef = constraintFeatureIndexer.indexOf_!('countBooktitleAtMostOne)
  val countJournalf = constraintFeatureIndexer.indexOf_!('countJournalAtMostOne)
  val countBooktitleOrJournalf = constraintFeatureIndexer.indexOf_!('countBooktitleOrJournalAtMostOne)
  val notdatef = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotYear)
  val numericSegmentCommaSeparatedf = constraintFeatureIndexer.indexOf_!('numericSegmentCommaSeparated)
  val numericf = constraintFeatureIndexer.indexOf_!('numericFieldAndNotMatchesPattern)
  val volumeNumericCountf = constraintFeatureIndexer.indexOf_!('volumeNumericsAtMostOne)
  val techf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotTech)
  val notef = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotNoteField)
  val booktitlef = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotBooktitleField)
  val journalf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotJournalField)
  val notpagef = constraintFeatureIndexer.indexOf_!('notMatchPatternAndPageField)
  val pagef = constraintFeatureIndexer.indexOf_!('matchPatternAndNotPage)
  val editorf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotEditorField)
  val publisherf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotPublisherField)
  val seriesf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotSeriesField)
  val nameOnlyInitialsf = constraintFeatureIndexer.indexOf_!('nameContainsOnlyInitials)
  val nameOnlyFirstLastf = constraintFeatureIndexer.indexOf_!('nameContainsMoreThanFirstLast)
  val nameInitialsBeforeOrAfterLastf = constraintFeatureIndexer.indexOf_!('nameInitialsBeforeAfterLastName)
  val nameContainsNumericf = constraintFeatureIndexer.indexOf_!('nameContainsNumeric)
  val institutionf = constraintFeatureIndexer.indexOf_!('matchesPatternAndNotInstitutionField)
  val newAuthorSegmentf = constraintFeatureIndexer.indexOf_!('newAuthorsSegmentf)
  val newEditorSegmentf = constraintFeatureIndexer.indexOf_!('newEditorSegmentf)
  constraintFeatureIndexer.lock

  // patterns
  val yearPattern = "\\(?\\s*(19|20)\\d\\d[a-z]?\\s*\\)?"
  val pagesPatt = "(pages|pp\\.|\\d+\\s*[\\-{#]{2,}\\s*\\d+|\\d{3,}\\s*[\\-{#]\\s*\\d{3,})"

  def newConstraintParams: Array[Double] = new Array[Double](constraintFeatureIndexer.size)

  def rexaInferPredCorefSegmentation(ex: CorefSegmentationExample, localParams: Params, localCounts: Params,
                                     useProbs: Boolean, useWts: Boolean, stepSize: Double,
                                     onlySegmentation: Boolean, computeBestSegmentation: Boolean,
                                     outputConstraintParams: Boolean,
                                     updateConstraintParams: Boolean = true): (ProbStats, Segmentation, CorefSegmentation) = {
    val words = ex.words
    val len = words.size
    val lcwords = words.map(_.toLowerCase)
    var possRecordIds: HashSet[ObjectId] = null
    val constraintParams = constraintParamsMap.getOrElse(ex._id, newConstraintParams)
    val ends = mapIndex(len + 1, (j: Int) => isPossibleEnd(j, words))

    // caching for speed
    val cachedSimScores = new HashMap[(Int, ObjectId, Int, Int), Double]
    def getSimScore(field: Field, fieldClusterId: ObjectId, i: Int, j: Int): Double = {
      val key = (field.index, fieldClusterId, i, j)
      if (!cachedSimScores.contains(key)) cachedSimScores(key) = field.simScore(fieldClusterId, words.slice(i, j))
      cachedSimScores(key)
    }

    val _validStarts = mapIndex(fields.size, (l: Int) => {
      fields(l).name == "O" || fields(l).name == "author" || fields(l).name == "editor" || fields(l).name == "title" ||
        fields(l).name == "tech"
    })
    val stopWordSet = getCachedLblSet(Seq("and"))
    val _numericFieldSet = mapIndex(fields.size, (l: Int) => {
      fields(l).name == "pages" || fields(l).name == "volume" || fields(l).name == "date"
    })
    val _nameFieldSet = mapIndex(fields.size, (l: Int) => {
      fields(l).name == "author" || fields(l).name == "editor"
    })
    val _otherField = lindex("O")
    val _authorField = lindex("author")
    val _titleField = lindex("title")
    val _journalField = lindex("journal")
    val _booktitleField = lindex("booktitle")
    val _dateField = lindex("date")
    val _volumeField = lindex("volume")
    val _pagesField = lindex("pages")
    val _techField = lindex("tech")
    val _editorField = lindex("editor")
    val _publisherField = lindex("publisher")
    val _seriesField = lindex("series")
    val _instituteField = lindex("institute")

    // constraint features
    def getInvalidStart(a: Int): Boolean = {
      !_validStarts(a)
    }

    val _cacheStopWords = mapIndex(len, (i: Int) => stopWordSet(lcwords(i)))
    def getSingleStopword(a: Int, i: Int, j: Int): Boolean = {
      (j - i) == 1 && a != _otherField && _cacheStopWords(i)
    }

    val _cacheNumericMatches = mapIndex(len, (k: Int) => words(k).matches(".*\\d.*"))
    val _cacheRefMarker = getCachedSegmentFeatArray(len, ends, {
      (i: Int, j: Int) => i == 0 && (_cacheNumericMatches(i) || (words(i).contains("[") && words(j - 1).contains("]")))
    })
    def getRefMarker(a: Int, i: Int, j: Int): Boolean = {
      a != _otherField && i == 0 && _cacheRefMarker(i)(j)
    }

    def getCountTitle(a: Int): Boolean = {
      a == _titleField
    }

    def getCountJournal(a: Int): Boolean = {
      a == _journalField
    }

    def getCountBooktitle(a: Int): Boolean = {
      a == _booktitleField
    }

    def getCountBooktitleOrJournal(a: Int): Boolean = {
      a == _booktitleField || a == _journalField
    }

    val _cacheMatchYearPattern = mapIndex(len, (k: Int) => lcwords(k).matches(yearPattern))
    def getMatchesPatternAndNotDate(a: Int, k: Int): Boolean = {
      a != _dateField && _cacheMatchYearPattern(k)
    }

    val _cacheContainsComma = mapIndex(len, (k: Int) => words(k).contains(","))
    //    val _cacheNumericSegmentCommaSeparated = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) => _cacheContainsComma.slice(i, j).contains(true)
    //    })
    //    def getNumericSegmentCommaSeparated(a: Int, i: Int, j: Int): Boolean = {
    //      _numericFieldSet(a) && _cacheNumericSegmentCommaSeparated(i)(j)
    //    }

    val _cacheNumericFieldPattern = mapIndex(len, (k: Int) => {
      _cacheNumericMatches(k) ||
        (k < len - 1 && lcwords(k).matches("(pages|pp\\.?|vol\\.?|no\\.?)") && _cacheNumericMatches(k + 1))
    })
    def getNumericFieldAndNotMatchesPattern(a: Int, k: Int): Boolean = {
      !_numericFieldSet(a) && _cacheNumericFieldPattern(k)
    }

    //    val _cacheVolumeNumericCount = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) => j - i > fields(_volumeField).maxSegmentLength ||
    //        _cacheNumericMatches.slice(i, j).count(_ == true) > 1
    //    })
    //    def getVolumeNumericCount(a: Int, i: Int, j: Int): Boolean = {
    //      a == _volumeField && _cacheVolumeNumericCount(i)(j)
    //    }

    val _cacheTechFieldPattern = mapIndex(len, (k: Int) => {
      techWordSet(lcwords(k)) ||
        (k < len - 1 && words(k) == "TR" && words(k + 1).matches("^[\\-\\d].*"))
    })
    def getTechField(a: Int, k: Int): Boolean = {
      // consecutive words "technical report" || "thesis"
      a != _techField && _cacheTechFieldPattern(k)
    }

    val _cacheBooktitleFieldPattern = mapIndex(len, (k: Int) => booktitleWordSet(words(k)))
    def getBooktitleField(a: Int, k: Int): Boolean = {
      a != _booktitleField && _cacheBooktitleFieldPattern(k)
    }

    val _cacheJournalFieldPattern = mapIndex(len, (k: Int) => journalWordSet(words(k)))
    def getJournalField(a: Int, k: Int): Boolean = {
      a != _journalField && _cacheJournalFieldPattern(k)
    }

    val _cacheNoteFieldPattern = mapIndex(len, (k: Int) => noteWordSet(lcwords(k)))
    def getNoteField(a: Int, k: Int): Boolean = {
      a != _otherField && _cacheNoteFieldPattern(k)
    }

    val _cachePagePatternMatches = mapIndex(len, (k: Int) => lcwords(k).matches(pagesPatt))
    def getNotPageField(a: Int, k: Int): Boolean = {
      a == _pagesField && !_cachePagePatternMatches(k)
    }

    def getPageField(a: Int, k: Int): Boolean = {
      a != _pagesField && _cachePagePatternMatches(k)
    }

    val _cacheEditorPatternMatches = mapIndex(len, (k: Int) => words(k).matches("([Ee]ditors?|Ed\\.|[Ee]ds\\.)"))
    def getEditorField(a: Int, k: Int): Boolean = {
      a != _editorField && _cacheEditorPatternMatches(k)
    }

    val _cachePublisherPatternMatches = mapIndex(len, (k: Int) => publisherWordSet(words(k)))
    def getPublisherField(a: Int, k: Int): Boolean = {
      a != _publisherField && _cachePublisherPatternMatches(k)
    }

    val _cacheSeriesPatternMatches = mapIndex(len, (k: Int) => seriesWordSet(words(k)))
    def getSeriesField(a: Int, k: Int): Boolean = {
      a != _seriesField && _cacheSeriesPatternMatches(k)
    }

    val _cachePatternInitials = mapIndex(len, (k: Int) => lcwords(k).matches("[a-z]\\."))
    def getNameOnlyInitials(a: Int, i: Int, j: Int): Boolean = {
      _nameFieldSet(a) && j - i == 1 && _cachePatternInitials(i)
    }

    //    val maxNameLen = 6
    //    val _cachePatternTwoOrMoreChar = mapIndex(len, (k: Int) => lcwords(k).matches("[a-z]{2,}"))
    //    val _cacheNameSegmentInitials = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) => j - i > maxNameLen || !_cachePatternTwoOrMoreChar.slice(i, j).contains(true)
    //    })
    //
    //    val _cacheNameOnlyFirstLast = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) => j - i > maxNameLen || _cachePatternTwoOrMoreChar.slice(i, j).count(_ == true) > 2
    //    })
    //    def getNameOnlyFirstLast(a: Int, i: Int, j: Int): Boolean = {
    //      _nameFieldSet(a) && _cacheNameOnlyFirstLast(i)(j)
    //    }
    //
    //    val _cacheNameInitialsBeforeAfter = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) =>
    //        if (j - i > maxNameLen) true
    //        else {
    //          val sliceInitials = _cachePatternInitials.slice(i, j)
    //          val firstInitialIndex = sliceInitials.indexOf(true)
    //          if (firstInitialIndex < 0) false
    //          else {
    //            val lastInitialIndex = sliceInitials.lastIndexOf(true)
    //            if (lastInitialIndex <= firstInitialIndex) false
    //            else _cachePatternTwoOrMoreChar.slice(i, j).slice(firstInitialIndex, lastInitialIndex).contains(true)
    //          }
    //        }
    //    })
    //    def getNameInitialsBeforeAfterLast(a: Int, i: Int, j: Int): Boolean = {
    //      _nameFieldSet(a) && _cacheNameInitialsBeforeAfter(i)(j)
    //    }
    //
    //    val _cacheNameContainsNumeric = getCachedSegmentFeatArray(len, ends, {
    //      (i: Int, j: Int) => j - i > maxNameLen || _cacheNumericMatches.slice(i, j).contains(true)
    //    })
    //    def getNameContainsNumeric(a: Int, i: Int, j: Int): Boolean = {
    //      _nameFieldSet(a) && _cacheNameContainsNumeric(i)(j)
    //    }

    val _cacheInstitutionPattern = mapIndex(len, (k: Int) => instituteWordSet(words(k)))
    def getInstitutionField(a: Int, k: Int): Boolean = {
      a != _instituteField && _cacheInstitutionPattern(k)
    }

    def getNewAuthorSegment(a: Int, b: Int): Boolean = {
      (a == -1 || a != _authorField) && b == _authorField
    }

    def getNewEditorSegment(a: Int, b: Int): Boolean = {
      (a == -1 || a != _editorField) && b == _editorField
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
        if (getCountTitle(a)) emitScore -= constraintParams(countTitlef)
        if (getCountJournal(a)) emitScore -= constraintParams(countJournalf)
        if (getCountBooktitle(a)) emitScore -= constraintParams(countBooktitlef)
        if (getCountBooktitleOrJournal(a)) emitScore -= constraintParams(countBooktitleOrJournalf)
        // if (getSingleStopword(a, i, j)) emitScore -= constraintParams(singleStopwordf)
        if (getNameOnlyInitials(a, i, j)) emitScore -= constraintParams(nameOnlyInitialsf)
        // if (getNameContainsNumeric(a, i, j)) emitScore -= constraintParams(nameContainsNumericf)
        // if (getNameOnlyFirstLast(a, i, j)) emitScore -= constraintParams(nameOnlyFirstLastf)
        // if (getNameInitialsBeforeAfterLast(a, i, j)) emitScore -= constraintParams(nameInitialsBeforeOrAfterLastf)
        if (getRefMarker(a, i, j)) emitScore -= constraintParams(refMarkerf)
        // if (getVolumeNumericCount(a, i, j)) emitScore -= constraintParams(volumeNumericCountf)
        // if (getNumericSegmentCommaSeparated(a, i, j)) emitScore -= constraintParams(numericSegmentCommaSeparatedf)
        forIndex(i, j, (k: Int) => {
          if (getMatchesPatternAndNotDate(a, k)) emitScore -= constraintParams(notdatef)
          if (getNumericFieldAndNotMatchesPattern(a, k)) emitScore -= constraintParams(numericf)
          if (getTechField(a, k)) emitScore -= constraintParams(techf)
          if (getNoteField(a, k)) emitScore -= constraintParams(notef)
          if (getBooktitleField(a, k)) emitScore -= constraintParams(booktitlef)
          if (getJournalField(a, k)) emitScore -= constraintParams(journalf)
          // if (getNotPageField(a, k)) emitScore -= constraintParams(notpagef)
          if (getPageField(a, k)) emitScore -= constraintParams(pagef)
          if (getInstitutionField(a, k)) emitScore -= constraintParams(institutionf)
          if (getEditorField(a, k)) emitScore -= constraintParams(editorf)
          if (getSeriesField(a, k)) emitScore -= constraintParams(seriesf)
          if (getPublisherField(a, k)) emitScore -= constraintParams(publisherf)
        })
        emitScore
      }

      override def updateEmission(a: Int, i: Int, j: Int, x: Double): Unit = {
        if (getCountTitle(a)) setConstraint(countTitlef, 1, -x)
        if (getCountJournal(a)) setConstraint(countJournalf, 1, -x)
        if (getCountBooktitle(a)) setConstraint(countBooktitlef, 1, -x)
        if (getCountBooktitleOrJournal(a)) setConstraint(countBooktitleOrJournalf, 1, -x, 0.1)
        // if (getSingleStopword(a, i, j)) setConstraint(singleStopwordf, 0, -x)
        if (getNameOnlyInitials(a, i, j)) setConstraint(nameOnlyInitialsf, 0, -x)
        // if (getNameContainsNumeric(a, i, j)) setConstraint(nameContainsNumericf, 0, -x)
        // if (getNameOnlyFirstLast(a, i, j)) setConstraint(nameOnlyFirstLastf, 0, -x)
        // if (getNameInitialsBeforeAfterLast(a, i, j)) setConstraint(nameInitialsBeforeOrAfterLastf, 0, -x)
        // if (getVolumeNumericCount(a, i, j)) setConstraint(volumeNumericCountf, 0, -x)
        if (getRefMarker(a, i, j)) setConstraint(refMarkerf, 0, -x)
        // if (getNumericSegmentCommaSeparated(a, i, j)) setConstraint(numericSegmentCommaSeparatedf, 0, -x)
        forIndex(i, j, (k: Int) => {
          if (getMatchesPatternAndNotDate(a, k)) setConstraint(notdatef, 0, -x, 1)
          if (getNumericFieldAndNotMatchesPattern(a, k)) setConstraint(numericf, 0, -x, 1)
          if (getTechField(a, k)) setConstraint(techf, 0, -x)
          if (getNoteField(a, k)) setConstraint(notef, 0, -x)
          if (getBooktitleField(a, k)) setConstraint(booktitlef, 0, -x)
          if (getJournalField(a, k)) setConstraint(journalf, 0, -x)
          // if (getNotPageField(a, k)) setConstraint(notpagef, 0, -x)
          if (getPageField(a, k)) setConstraint(pagef, 0, -x)
          if (getInstitutionField(a, k)) setConstraint(institutionf, 0, -x)
          if (getEditorField(a, k)) setConstraint(editorf, 0, -x)
          if (getSeriesField(a, k)) setConstraint(seriesf, 0, -x)
          if (getPublisherField(a, k)) setConstraint(publisherf, 0, -x)
        })
        if (doUpdate) super.updateEmission(a, i, j, x)
      }

      override def scoreStart(a: Int, j: Int): Double = {
        var startScore = super.scoreStart(a, j)
        if (getInvalidStart(a)) startScore -= constraintParams(invalidStartf)
        if (getNewAuthorSegment(-1, a)) startScore -= constraintParams(newAuthorSegmentf)
        if (getNewEditorSegment(-1, a)) startScore -= constraintParams(newEditorSegmentf)
        startScore
      }

      override def updateStart(a: Int, j: Int, x: Double) {
        if (getInvalidStart(a)) setConstraint(invalidStartf, 0, -x)
        if (getNewAuthorSegment(-1, a)) setConstraint(newAuthorSegmentf, 1, -x, 1)
        if (getNewEditorSegment(-1, a)) setConstraint(newEditorSegmentf, 1, -x, 1)
        if (doUpdate) super.updateStart(a, j, x)
      }

      override def scoreTransition(a: Int, b: Int, i: Int, j: Int): Double = {
        var transitScore = super.scoreTransition(a, b, i, j)
        if (getNewAuthorSegment(a, b)) transitScore -= constraintParams(newAuthorSegmentf)
        if (getNewEditorSegment(a, b)) transitScore -= constraintParams(newEditorSegmentf)
        transitScore
      }

      override def updateTransition(a: Int, nxta: Int, i: Int, j: Int, x: Double): Unit = {
        if (getNewAuthorSegment(a, nxta)) setConstraint(newAuthorSegmentf, 1, -x, 1)
        if (getNewEditorSegment(a, nxta)) setConstraint(newEditorSegmentf, 1, -x, 1)
        if (doUpdate) super.updateTransition(a, nxta, i, j, x)
      }

      override def updateSingleEmission(a: Int, k: Int, x: Double): Unit = {
        if (doUpdate) super.updateSingleEmission(a, k, x)
      }

      override def scoreFieldClusterOverlap(recordClusterId: ObjectId, field: Field,
                                            fieldClusterId: ObjectId, i: Int, j: Int): Double = {
        var overlapScore = 0.0
        if (field.name != "O") {
          val simScore = getSimScore(field, fieldClusterId, i, j)
          if (simScore > field.simThreshold) {
            forIndex(i, j, (k: Int) => {
              if (getSimScore(field, fieldClusterId, k, k + 1) > 0) {
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
          val simScore = getSimScore(field, fieldClusterId, i, j)
          if (simScore > field.simThreshold) {
            forIndex(i, j, (k: Int) => {
              if (getSimScore(field, fieldClusterId, k, k + 1) > 0) {
                val uniqSimf = ('simMatch, k)
                if (!uniqMatch(uniqSimf)) {
                  uniqMatch += uniqSimf
                  b(simf) -= 0.95
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

    if (updateConstraintParams) {
      // optimize by projected gradient descent
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
          forIndex(parameters.length, (i: Int) => {
            objectiveValue += inferencer.b(i) * constraintParams(i) + 0.5 * inferencer.regularization(i) * constraintParams(i) * constraintParams(i)
            gradient(i) = inferencer.b(i) + inferencer.constraintCounts(i) +
              inferencer.regularization(i) * constraintParams(i)
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
      optimizer.setMaxIterations(5)
      optimizer.optimize(objective, new OptimizerStats, new AverageValueDifference(1e-3))
      constraintParamsMap.update(ex._id, constraintParams)
    }
    if (outputConstraintParams) {
      info("")
      forIndex(constraintParams.size, (i: Int) => {
        if (constraintParams(i) != 0) info("wt(" + constraintFeatureIndexer(i) + ")=" + constraintParams(i))
      })
    }

    val predInfer = newInferencer(true)
    predInfer.updateCounts
    (predInfer.stats, predInfer.bestWidget.getSegmentation, predInfer.bestWidget)
  }
}

