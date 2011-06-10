package cc.refectorie.user.kedarb.dbalign.expts.rexa

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.fst.LbledTokSeq
import cc.refectorie.user.kedarb.dbalign.misc.KnowledgeBase
import cc.refectorie.user.kedarb.dbalign.{GlobalConfig, ACorefSegmentationEnv}
import collection.mutable.HashSet
import cc.refectorie.user.kedarb.dynprog.data.CoraSgml2Owpl
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.dynprog.utils.Utils._

/**
 * @author kedarb
 * @since 3/25/11
 */

trait DBLPRexaEnv extends ACorefSegmentationEnv {
  val mongoHostname = GlobalConfig.get[String]("mongoHost", "localhost")
  val mongoPort = GlobalConfig.get[Int]("mongoPort", 27017)

  // raw data
  val bibRaw = new KnowledgeBase("dblp-rexa-align-raw", mongoHostname, mongoPort)
  val bibRecords = bibRaw.getRawColl("citation_records")
  val bibTexts = bibRaw.getRawColl("citation_texts")
  bibRecords.ensureIndex(Map("bibtype" -> 1))
  bibTexts.ensureIndex(Map("bibtype" -> 1))

  // pre-processed
  val processedKB = new KnowledgeBase("dblp-rexa-align-processed", mongoHostname, mongoPort)
  val mentionColl = processedKB.getMentionColl("citation_mentions")
  mentionColl.ensureIndex(Map("bibtype" -> 1))

  // stopwords
  val titleStopWords = Set("a", "about", "above", "across", "after", "afterwards", "again", "against", "all",
    "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
    "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
    "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
    "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom",
    "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail",
    "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty",
    "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen",
    "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
    "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
    "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
    "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep",
    "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
    "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither",
    "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere",
    "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
    "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see",
    "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere",
    "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still",
    "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
    "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this",
    "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward",
    "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we",
    "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby",
    "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom",
    "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself",
    "yourselves", "the")

  def titleHash(phrase: Seq[String]): HashSet[String] = {
    val keys = new HashSet[String]
    val validWords = phrase.map(_.toLowerCase).filter(s => s.length() > 1 && !titleStopWords(s))
    if (validWords.size == 0) keys += "$EMPTY$"
    else if (validWords.size == 1) keys += "$SINGLE$=" + validWords.mkString("#")
    else if (validWords.size == 2) keys += "$DOUBLE$=" + validWords.mkString("#")
    else {
      val buff = new StringBuffer
      forIndex(validWords.size - 2, (i: Int) => {
        buff.setLength(0)
        buff.append("$TRIPLE$=")
        buff.append(validWords(i))
        buff.append("#")
        buff.append(validWords(i + 1))
        buff.append("#")
        buff.append(validWords(i + 2))
        keys += buff.toString
      })
    }
    keys
  }

  def authorHash(phrase: Seq[String]): HashSet[String] = {
    val keys = new HashSet[String]
    phrase.map(_.toLowerCase.replaceAll("[^A-Za-z0-9]+", "")).filter(_.length > 1).foreach(w => keys += w)
    keys
  }

  def getStringFeatures(mention: DBObject): Array[String] = {
    val words = getStringArray(mention)
    words.map(RexaCitationFeatures.simplify(_))
  }

  def getStringVectorFeatures(mention: DBObject): Array[Seq[String]] = {
    val words = getStringArray(mention)
    val trueLabels = getStringArray(mention, "trueLabels")
    val lines = new Array[String](words.size)
    var i = 0
    while (i < words.size) {
      lines(i) = trueLabels(i) + "\t" + words(i)
      i += 1
    }
    val conjunctions: Array[Array[Int]] =
      if (isRecord(mention)) null
      else Array(Array(1), Array(-1), Array(2), Array(-2))
    RexaCitationFeatures.processLines(lines, "\t", conjunctions, 0, 1).features.map(_.toSeq)
  }

  def isPossibleEnd(j: Int, words: Array[String]): Boolean = {
    val endsOnPunc = "^.*[^A-Za-z0-9\\-]$"
    val startsWithPunc = "^[^A-Za-z0-9\\-].*$"
    val endsWithAlpha = "^.*[A-Za-z]$"
    val endsWithNum = "^.*[0-9]$"
    val startsWithAlpha = "^[A-Za-z].*$"
    val startsWithNum = "^[0-9].*$"
    val endsOnSpecial = "^(and|et\\.?|vol\\.?|no\\.?|pp\\.?|pages)$"
    // info("calling isEnd('" + words(j - 1) + "'): " + words.mkString(" "))
    if (j == 0) false
    else j == words.length ||
      words(j - 1).matches(endsOnPunc) || // word ends on punctuation
      words(j).matches(startsWithPunc) || // words begins with punctuation
      words(j - 1).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      words(j).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      (words(j - 1).matches(endsWithAlpha) && words(j).matches(startsWithNum)) || // alpha -> num
      (words(j - 1).matches(endsWithNum) && words(j).matches(startsWithAlpha)) // num -> alpha
  }

  def newMentionFromCitationString(line: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> new ObjectId()
    builder += "isRecord" -> false
    builder += "source" -> "testRexa"
    // process the sgml to words and labels
    val sgmlStr = PreprocessDBLPRexa.normalizeSgml(line)
    val tokSeq = new LbledTokSeq(CoraSgml2Owpl(sgmlStr, "\t", true), "\t")
    // println("sgml[" + mention._id.get + "]: " + sgmlStr)
    val wordList = new java.util.ArrayList[String]()
    val labelList = new java.util.ArrayList[String]()
    tokSeq.column(1).foreach(wordList.add(_))
    tokSeq.columnToBIO(0).foreach(labelList.add(_))
    builder += "sgml" -> sgmlStr
    builder += "words" -> wordList
    builder += "trueLabels" -> labelList
    // return db object
    builder.result()
  }

  def rexaAdjustSegmentationForPunctuation(otherLabelIndex: Int, words: Array[String],
                                           segmentation: Segmentation): Segmentation = {
    // do additional post processing
    segmentation
    //    val adjSegmentation = new Segmentation(segmentation.length)
    //    forIndex(segmentation.numSegments, (s: Int) => {
    //      val segment = segmentation.segment(s)
    //      if (segment.end - segment.begin == 1 && words(segment.begin).toLowerCase == "and") {
    //        adjSegmentation.append(Segment(segment.begin, segment.end, otherLabelIndex))
    //      } else {
    //        adjSegmentation.append(Segment(segment.begin, segment.end, segment.label))
    //      }
    //    })
    //    adjSegmentation
  }
}

object PreprocessDBLPRexa extends DBLPRexaEnv {
  def normalizeSgml(s: String): String = {
    // removes: ref-marker, author-{first, middle, last}, authors, reference-hlabeled
    // removes font markers: i, b, tt, sup, sub
    // removes fields: isbn, crossref
    // convert: conference -> booktitle, number -> volume, web, note -> O, address -> location
    // convert? thesis -> tech
    // small formatting change &amp; -> and
    s.replaceAll("</?ref-marker>", " ")
      .replaceAll("</?authors>", " ")
      .replaceAll("</?author-(first|middle|last)>", " ")
      .replaceAll("</?reference-hlabeled>", " ")
      .replaceAll("<(/?)conference>", "<$1booktitle>")
      .replaceAll("<(/?)thesis>", "<$1tech>")
      .replaceAll("</?(su[pb]|i|tt|b)>", " ")
      .replaceAll("<(/?)number>", "<$1volume>")
      .replaceAll("<(/?)address>", "<$1location>")
      .replaceAll("<(/?)web>", " ")
      .replaceAll("<(/?)note>", " ")
      .replaceAll("<isbn>[^<]*</isbn>", " ")
      .replaceAll("<crossref>[^<]*</crossref>", " ")
      .replaceAll("&amp;", " and ")
      .replaceAll("^\\s+", "").trim
  }

  def newMention(mention: DBObject, isRecord: Boolean): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> mention._id.get
    builder += "isRecord" -> isRecord
    builder += "source" -> mention.as[String]("sourceId")
    if (mention.isDefinedAt("clusterId"))
      builder += "trueCluster" -> mention("clusterId")
    if (mention.isDefinedAt("bibtype"))
      builder += "bibtype" -> mention("bibtype")
    // process the sgml to words and labels
    val sgmlStr = normalizeSgml(mention.as[String]("sgml"))
    val tokSeq = new LbledTokSeq(CoraSgml2Owpl(sgmlStr, "\t", true), "\t")
    // println("sgml[" + mention._id.get + "]: " + sgmlStr)
    builder += "sgml" -> sgmlStr
    builder += "words" -> tokSeq.column(1)
    builder += "trueLabels" -> tokSeq.columnToBIO(0)
    // return db object
    builder.result
  }

  def main(args: Array[String]) {
    mentionColl.drop
    val dblpCount = bibRecords.count(Map("sourceId" -> "dblp")).toInt
    val rexaCount = bibTexts.count(Map("sourceId" -> "rexa")).toInt
    var count = 0
    for (mention <- bibTexts.find(Map("sourceId" -> "rexa"))) {
      try {
        val processed = newMention(mention, false)
        mentionColl += processed
        count += 1
        if (count % 1000 == 0) println("Loaded %d/%d rexa citations.".format(count, rexaCount))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    count = 0
    for (mention <- bibRecords.find(Map("sourceId" -> "dblp"))) {
      try {
        val processed = newMention(mention, true)
        mentionColl += processed
        count += 1
        if (count % 1000 == 0) println("Loaded %d/%d dblp records.".format(count, dblpCount))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }
}