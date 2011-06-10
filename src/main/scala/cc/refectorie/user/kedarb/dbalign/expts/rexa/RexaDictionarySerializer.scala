package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.Options
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.types.{IndexerUtils, Indexer}
import cc.refectorie.user.kedarb.tools.opts.OptParser
import cc.refectorie.user.kedarb.dbalign._
import fields.Field
import java.io._

/**
 * @author kedarb
 */

object RexaDictionarySerializer extends ACorefSegmentationProblem with DBLPRexaEnv {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val opts = new Options
  val segcorefopts = new SegCorefOptions
  val oovIndex = wordFeatureIndexer.indexOf_!("$OOV$")

  def newField(name: String, index: Int): Field = {
    throw fail("Cannot create field!")
  }

  def initParams {}

  def checkEndOnPunctuation: Unit = {
    var numTxts = 0
    var numEnds = 0
    var numEndsOnPunctuation = 0
    foreachText((m: DBObject) => {
      val words = getStringArray(m)
      val segmentation = getTrueSegmentation(m)
      forIndex(segmentation.numSegments, (i: Int) => {
        val segment = segmentation.segment(i)
        if (segment.label != otherLabelIndex) {
          numEnds += 1
          if (isPossibleEnd(segment.end, words))
            numEndsOnPunctuation += 1
          else
            println("w[end-1]=" + words(segment.end - 1) + " w[end]=" + words(segment.end))
        }
      })
      numTxts += 1
      if (numTxts % 1000 == 0) println("+++ Processed " + numTxts + "/" + numTexts + " texts ...")
    })
    println("#endOnPunc/#end=" + numEndsOnPunctuation + "/" + numEnds)
  }

  def storeIndexer(indexer: Indexer[String], file: String): Unit = {
    val out = new PrintStream(new FileOutputStream(file))
    IndexerUtils.serializeIndexer[String](out.println(_), indexer)
    out.close
  }

  def checkExists(file: String): Boolean = new File(file).exists()

  def main(args: Array[String]) {
    val parser = new OptParser
    parser.doRegister("exec", opts)
    parser.doRegister("segcoref", segcorefopts)

    if (!parser.doParse(args)) System.exit(1)

    // create dictionaries
    initDictionary
    info("Storing dictionaries to file ...")
    storeIndexer(wordIndexer, segcorefopts.wordFile)
    storeIndexer(wordFeatureIndexer, segcorefopts.wordFeatureFile)
    storeIndexer(featureIndexer, segcorefopts.featureFile)
  }
}