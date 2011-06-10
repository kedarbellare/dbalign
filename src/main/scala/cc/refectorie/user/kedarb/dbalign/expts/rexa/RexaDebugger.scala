package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.Options
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.tools.opts.OptParser
import cc.refectorie.user.kedarb.dbalign._
import fields._

/**
 * @author kedarb
 * @since 5/27/11
 */

object RexaDebugger extends ACorefSegmentationProblem with DBLPRexaEnv {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val opts = new Options
  val segcorefopts = new SegCorefOptions

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

  def initParams {}

  def main(args: Array[String]) {
    val parser = new OptParser
    parser.doRegister("exec", opts)
    parser.doRegister("segcoref", segcorefopts)

    if (!parser.doParse(args)) System.exit(1)

    initFields
    var numTexts = 0
    var numTextsWithCluster = 0
    var numMatchesForClustered = 0
    var numCorrMatchesForClustered = 0
    var numMatchesForSingleton = 0
    foreachText((m: DBObject) => {
      val words = getStringArray(m)
      val ends = mapIndex(words.size + 1, (j: Int) => isPossibleEnd(j, words))
      // info("For words: " + words.mkString(" "))
      val recordMatchIds = new RecordBlocker(fields, words, ends, null, false).getPossibleRecordClusterIds(true)
      // info("#matches=" + recordMatchIds.size)

      numTexts += 1
      if (numTexts % 100 == 0) info("+++ Processed " + numTexts + " texts ...")
      if (m.isDefinedAt("trueCluster")) {
        val trueCluster = m("trueCluster").toString
        numTextsWithCluster += 1
        numMatchesForClustered += recordMatchIds.size
        recordMatchIds.foreach((id: ObjectId) => {
          for (record <- mentionColl.findOneByID(id)) {
            if (record.isDefinedAt("trueCluster") && record("trueCluster").toString == trueCluster) {
              numCorrMatchesForClustered += 1
            }
          }
        })
      } else {
        numMatchesForSingleton += recordMatchIds.size
      }
    })
    info("#texts=" + numTexts)
    info("#textsWithCluster=" + numTextsWithCluster)
    info("#matchesForClustered=" + numMatchesForClustered)
    info("#matchesForSingleton=" + numMatchesForSingleton)
    info("#matches/text=" + ((numMatchesForClustered + numMatchesForSingleton) / numTexts))
    info("#matches/textWithCluster=" + (numMatchesForClustered / numTexts))
    info("#corrMatches=" + numCorrMatchesForClustered + " for #textWithCluster=" + numTextsWithCluster)
  }
}