package cc.refectorie.user.kedarb.dbalign.expts.bft

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.data.Sgml2Owpl
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.fst.LbledTokSeq
import collection.mutable.ArrayBuffer
import cc.refectorie.user.kedarb.dbalign.{ACorefSegmentationEnv, GlobalConfig}
import cc.refectorie.user.kedarb.dbalign.misc.KnowledgeBase

/**
 * @author kedarb
 * @since 5/10/11
 */

trait BFTEnv extends ACorefSegmentationEnv {
  val mongoHostname = GlobalConfig.get[String]("mongoHost", "localhost")
  val mongoPort = GlobalConfig.get[Int]("mongoPort", 27017)

  // raw data
  val bftRaw = new KnowledgeBase("bft-align-raw", mongoHostname, mongoPort)
  val bftPosts = bftRaw.getRawColl("bft_posts")
  val bftRecords = bftRaw.getRawColl("bft_records")

  // pre-processed
  val processedKB = new KnowledgeBase("bft-align-processed", mongoHostname, mongoPort)
  val mentionColl = processedKB.getMentionColl("bft_mentions")

  var conjunctions: Array[Array[Int]] = Array(Array(-1), Array(1))

  def getStringFeatures(mention: DBObject): Array[String] = {
    val words = getStringArray(mention)
    words.map(BFTSgml2Owpl.simplify(_))
  }

  def getStringVectorFeatures(mention: DBObject): Array[Seq[String]] = {
    val words = getStringArray(mention)
    val features = mapIndex(words.length, (i: Int) => new ArrayBuffer[String])
    forIndex(words.length, {
      i: Int =>
        val feats = features(i)
        val word = words(i)
        val wordNoPunc = word.replaceAll("[^a-z0-9]+", " ").trim
        // feats += "W=" + word
        feats += "WNOPUNC=" + wordNoPunc
        feats += "SIMPLIFIED=" + BFTSgml2Owpl.simplify(word)
        // contains features
        // if (word.contains("*") || word.contains("star")) feats += "CONTAINS_STAR"
        if (word.matches("\\d+(\\.\\d+)?\\*")) feats += "CONTAINS_STAR_PATTERN"
        if (word.contains("+")) feats += "CONTAINS_PLUS"
        if (word.contains("/")) feats += "CONTAINS_SLASH"
        if (word.contains(",")) feats += "CONTAINS_COMMA"
        if (word.contains("-")) feats += "CONTAINS_DASH"
        if (word matches BFTSgml2Owpl.PUNC) feats += "CONTAINS_PUNC"
    })
    // add feature conjunctions for texts
    //    if (!isRecord(mention))
    //      LbledTokSeq.addFeatureConjunctions(features, conjunctions)
    features.map(_.toSeq)
  }
}

object BFTSgml2Owpl extends Sgml2Owpl {
  val Lexer = List(
    "\\d+(\\.\\d+)?\\*", // star ratings
    "\\d+/\\d+(/\\d+)?", // date with or without year
    "\\$\\d+(\\.\\d+)?", // prices
    "[A-Za-z]+",
    "[()\"'\\-\\.,]+",
    "\\S+")
    .mkString("(", "|", ")").r

  // Regex patterns
  // XXX: no "mar" in month as localarea "del mar"
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"
  val ALPHANUM = "[a-z0-9]"
  val NUM = "[0-9]"
  val PUNC = "[,\\.;:?!()\"\\-'`]"

  def simplify(s: String): String = {
    if (s.matches("\\d+/\\d+(/\\d+)?"))
      "$DATE$"
    else if (s.matches("\\$\\d+(\\.\\d+)?"))
      "$PRICE$"
    else if (s.matches("\\d+"))
      "$DIGITS$"
    else if (s.matches(DOTW))
      "$DAY$"
    else if (s.matches(MONTH))
      "$MONTH$"
    else s
  }
}

object PreprocessBFT extends BFTEnv {
  def newMention(mention: DBObject, isRecord: Boolean): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> mention._id.get
    builder += "isRecord" -> isRecord
    builder += "source" -> mention.as[String]("sourceId")
    if (mention.isDefinedAt("clusterId"))
      builder += "trueCluster" -> mention("clusterId")
    // process the sgml to words and labels
    val sgmlStr = mention.as[String]("sgml")
    val tokSeq = new LbledTokSeq(BFTSgml2Owpl(sgmlStr, "\t", true), "\t")
    println("sgml[" + mention._id.get + "]: " + sgmlStr)
    builder += "sgml" -> sgmlStr
    builder += "words" -> tokSeq.column(1)
    builder += "trueLabels" -> tokSeq.columnToBIO(0)
    // return db object
    builder.result
  }

  def main(args: Array[String]) {
    mentionColl.drop
    for (mention <- bftRecords) mentionColl += newMention(mention, true)
    for (mention <- bftPosts) mentionColl += newMention(mention, false)
  }
}