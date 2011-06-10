package cc.refectorie.user.kedarb.dbalign.expts.rexa

import cc.refectorie.user.kedarb.dynprog.fst.{LbledTokSeq, TokFtrFns}
import collection.mutable.{HashSet, HashMap}
import org.apache.log4j.Logger
import io.Source
import java.io.{File, InputStream}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.data.CoraSgml2Owpl
import cc.refectorie.user.kedarb.dynprog.types.TrieDict
import collection.mutable.ArrayBuffer._

/**
 * @author kedarb
 * @since Dec 16, 2010
 */

class RexaDict(val name: String, val toLC: Boolean = true) {
  val set = new HashSet[String]

  def add(s: String): Unit = set += {
    if (toLC) s.toLowerCase else s
  }

  def contains(s: String): Boolean = set.contains(if (toLC) s.toLowerCase else s)

  override def toString = name + " :: " + set.mkString("\n")
}

object RexaDict {
  val logger = Logger.getLogger(getClass.getSimpleName)

  def fromFile(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromFile(filename), toLC)
  }

  def fromResource(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)), toLC)
  }

  def fromResourceOrFile(filename: String, toLC: Boolean = true): RexaDict = {
    try {
      val is: InputStream = getClass.getClassLoader.getResourceAsStream(filename)
      if (is != null) fromSource(filename, Source.fromInputStream(is), toLC)
      else if (new File(filename).exists) {
        logger.warn("Couldn't find file %s in classpath! Using relative path instead.".format(filename))
        fromSource(filename, Source.fromFile(filename), toLC)
      } else {
        logger.warn("Couldn't find file %s in classpath or relative path! Returning empty dict.".format(filename))
        return new RexaDict(new File(filename).getName, toLC)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        return new RexaDict(new File(filename).getName, toLC)
    }
  }

  def fromSource(filename: String, source: Source, toLC: Boolean = true): RexaDict = {
    val name = new File(filename).getName
    val dict = new RexaDict(name, toLC)
    for (line <- source.getLines) dict.add(line)
    dict
  }
}

class RexaTrieDict(val toLC: Boolean = true) {
  val map = new HashMap[String, RexaTrieDict] {
    override def default(key: String) = {
      val trie = new RexaTrieDict(toLC);
      this(key) = trie;
      trie
    }
  }
  var isDone = false

  def add(a: Seq[String]): Unit = {
    var t = this
    forIndex(a.length, {
      k: Int =>
        val ak = if (toLC) a(k).toLowerCase else a(k)
        t = t.map(ak)
    })
    t.isDone = true
  }

  def str2seq(s: String): Seq[String] = CoraSgml2Owpl.Lexer.findAllIn(s).toSeq

  def add(s: String): Unit = add(str2seq(s))

  def contains(a: Seq[String]): Boolean = {
    var t = this
    forIndex(a.length, {
      k: Int =>
        val ak = if (toLC) a(k).toLowerCase else a(k)
        if (!t.map.contains(ak)) return false
        else t = t.map(ak)
    })
    t.isDone
  }

  def contains(s: String): Boolean = contains(str2seq(s))

  /**
   * Returns the end index
   */
  def endIndexOf(a: Seq[String], begin: Int): Int = {
    var t = this
    var end = begin
    while (end < a.length) {
      val ak = if (toLC) a(end).toLowerCase else a(end)
      if (!t.map.contains(ak)) return -1
      else {
        t = t.map(ak)
        if (t.isDone) return end
      }
      end += 1
    }
    if (t.isDone) a.length - 1
    else -1
  }
}

object RexaTrieDict {
  val logger = Logger.getLogger(getClass.getSimpleName)

  def fromFile(filename: String, toLC: Boolean = true): RexaTrieDict = {
    fromSource(Source.fromFile(filename), toLC)
  }

  def fromResource(filename: String, toLC: Boolean = true): RexaTrieDict = {
    fromSource(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)), toLC)
  }

  def fromResourceOrFile(filename: String, toLC: Boolean = true): RexaTrieDict = {
    try {
      val is: InputStream = getClass.getClassLoader.getResourceAsStream(filename)
      if (is != null) fromSource(Source.fromInputStream(is), toLC)
      else if (new File(filename).exists) {
        logger.warn("Couldn't find file %s in classpath! Using relative path instead.".format(filename))
        fromSource(Source.fromFile(filename), toLC)
      } else {
        logger.warn("Couldn't find file %s in classpath or relative path! Returning empty dict.".format(filename))
        return new RexaTrieDict(toLC)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        return new RexaTrieDict(toLC)
    }
  }

  def fromSource(source: Source, toLC: Boolean = true): RexaTrieDict = {
    val dict = new RexaTrieDict(toLC)
    for (line <- source.getLines) dict.add(line)
    dict
  }
}

object RexaCitationFeatures {
  var debugfeatures = false
  var debuglabels = false

  // flags for features
  var usetriefeatures = true

  // feature types to use
  val useshapefeatures = true
  val usenerfeatures = true
  val usedblpfeatures = true
  val useregexfeatures = true

  val prefixToFtrFns = new HashMap[String, String => Option[String]]
  val prefixToTrieLexicon = new HashMap[String, RexaTrieDict]

  val LEX_ROOT = "citations/lexicons/"

  def LexiconResource(name: String, filename: String, toLC: Boolean = true): Unit = {
    val dict = RexaDict.fromResourceOrFile(LEX_ROOT + filename, toLC)
    prefixToFtrFns("LEXICON=" + name) = {
      s: String =>
        val smod = (if (toLC) s.toLowerCase else s).replaceAll("[^A-Za-z0-9]+", " ").trim()
        if (smod.length > 3 && dict.contains(smod)) Some("") else None
    }
  }

  def trieLex(filename: String, toLC: Boolean = false): RexaTrieDict = {
    RexaTrieDict.fromResourceOrFile(LEX_ROOT + filename, toLC)
  }

  def regex(name: String, pattern: String): Unit = {
    prefixToFtrFns("REGEX=" + name) = {
      s: String => if (s.matches(pattern)) Some("") else None
    }
  }

  def RegexMatcher(name: String, pattern: String) = regex(name, pattern)

  def RegexMatchOrNot(name: String, pattern: String) = {
    prefixToFtrFns("REGEX_PRESENT_" + name + "=") = {
      s: String => if (s.matches(pattern)) Some("true") else Some("false")
    }
  }

  def tokenText(name: String, fn: String => String = identity(_)): Unit = {
    prefixToFtrFns(name) = {
      s: String => Some(fn(s))
    }
  }

  val CAPS = "[A-Z]"
  val ALPHA = "[A-Za-z]"
  val ALPHANUM = "[A-Za-z0-9]"
  val NUM = "[0-9]"
  val PUNC = "[,\\.;:?!()\"'`]"

  val Month = "(?:January|February|March|April|May|June|July|August|September|October|November|December|" +
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
  val YearNotInParens = "(19|20|'|`)\\d\\d[a-z]?"
  val YearInParens = "\\(\\s*(19|20)\\d\\d[a-z]?\\s*\\)"
  val Pages = "(\\d{3,}\\s*[\\-{#]+\\s*\\d{3,}|\\d+\\s*[\\-{#]{2,}\\d+)"
  val VolumeNumber = "[0-9][0-9]?\\s*\\([0-9]?\\)"
  val Volume = "\\(\\s*[0-9][0-9]?\\s*\\)"
  val Numeric = "[0-9][0-9,]+\\.?[0-9]*"
  val DoTW = "(?:Mon|Tues?|Wed(?:nes)?|Thurs?|Fri|Satu?r?|Sun)(?:day)"
  val CardinalDirection = "(?ii)\\b(?:north|south|east|west)\\s*?(?:east|west|)\\b"

  // add token text fn
  // tokenText("WORD=")

  // add simplified word fn
  def simplify(word: String): String = {
    if (word matches YearNotInParens) "<YEARNOTINPARENS>"
    else if (word matches YearInParens) "<YEARINPARENS>"
    else if (word matches Pages) "<PAGES>"
    else if (word matches VolumeNumber) "<VOLUME_NUMBER>"
    else if (word matches Volume) "<VOLUME>"
    else if (word matches Numeric) "<NUMERIC>"
    else if (word matches DoTW) "<DOFTW>"
    else if (word matches Month) "<MONTH>"
    else if (word matches CardinalDirection) "<CARDINAL-DIRECTION>"
    else {
      val s = word.toLowerCase.replaceAll("\\s+", " ").replaceAll("\\d", "1").
        replaceAll("[^a-z0-9\\s\\(\\)\\[\\]]+", "")
      if (s.length() == 0) word else s
    }
  }

  // tokenText("SIMPLIFIED=", simplify(_))

  // add shape features
  if (useshapefeatures) {
    tokenText("SHAPE=", TokFtrFns.wordShape(_, 5))
  }

  // add lexicon feature functions
  if (usedblpfeatures) {
    LexiconResource("DBLPTITLESTARTHIGH", "title.start.high", false)
    LexiconResource("DBLPTITLESTARTMED", "title.start.med", false)
    LexiconResource("DBLPTITLEHIGH", "title.high")
    LexiconResource("DBLPTITLEMED", "title.med")
    LexiconResource("DBLPTITLELOW", "title.low")
    LexiconResource("DBLPAUTHORFIRST", "author-first", false)
    LexiconResource("DBLPAUTHORMIDDLE", "author-middle", false)
    LexiconResource("DBLPAUTHORLAST", "author-last", false)
    LexiconResource("CONFABBR", "conferences.abbr", false)
    LexiconResource("PLACES", "places")
    // add trie lexicons
    prefixToTrieLexicon += "TECH" -> trieLex("tech.txt")
    prefixToTrieLexicon += "JOURNAL" -> trieLex("journals")
    prefixToTrieLexicon += "CONFFULL" -> trieLex("conferences.full")
    prefixToTrieLexicon += "NOTEWORDS" -> trieLex("note-words.txt")
    prefixToTrieLexicon += "DBLPPUBLISHER" -> trieLex("publisher")
  }

  if (usenerfeatures) {
    LexiconResource("INSTITUTELEX", "institute-words.txt", false)
    LexiconResource("FirstHighest", "personname/ssdi.prfirsthighest")
    LexiconResource("FirstHigh", "personname/ssdi.prfirsthigh")
    LexiconResource("FirstMed", "personname/ssdi.prfirstmed")
    LexiconResource("FirstLow", "personname/ssdi.prfirstlow")
    LexiconResource("LastHigest", "personname/ssdi.prlasthighest")
    LexiconResource("LastHigh", "personname/ssdi.prlasthigh")
    LexiconResource("LastMed", "personname/ssdi.prlastmed")
    LexiconResource("LastLow", "personname/ssdi.prlastlow")
    LexiconResource("Honorific", "personname/honorifics")
    LexiconResource("NameSuffix", "personname/namesuffixes")
    LexiconResource("NameParicle", "personname/name-particles")
    LexiconResource("Nickname", "personname/nicknames")
    LexiconResource("Day", "days")
    LexiconResource("Month", "months")
    LexiconResource("StateAbbrev", "state_abbreviations")
    LexiconResource("Stopword", "stopwords")
    prefixToTrieLexicon += "University" -> trieLex("utexas/UNIVERSITIES")
    prefixToTrieLexicon += "State" -> trieLex("US-states")
    prefixToTrieLexicon += "Country" -> trieLex("countries")
    prefixToTrieLexicon += "CapitalCity" -> trieLex("country-capitals")
  }

  if (useregexfeatures) {
    RegexMatcher("CONTAINSDOTS", "[^\\.]*\\..*")
    RegexMatcher("CONTAINSCOMMA", ".*,.*")
    RegexMatcher("CONTAINSDASH", ALPHANUM + "+-" + ALPHANUM + "*")
    RegexMatcher("ACRO", "[A-Z][A-Z\\.]*\\.[A-Z\\.]*")

    RegexMatcher("URL1", "www\\..*|https?://.*|ftp\\..*|.*\\.edu/?.*")

    // patterns involving numbers
    RegexMatcher("PossiblePage", "[0-9]+\\s*[\\-{#]+\\s*[0-9]+")
    RegexMatcher("PossibleVol", "[0-9][0-9]?\\s*\\([0-9]+\\)")
    RegexMatcher("5+digit", "[0-9][0-9][0-9][0-9][0-9]+")
    RegexMatcher("HasDigit", ".*[0-9].*")
    RegexMatcher("AllDigits", "[0-9]+")

    // RegexMatcher("PAGEWORDS", "(?:pp\\.|[Pp]ages?|[\\-,\\.#}]|[0-9]\\s+)+")

    // Ordinals
    RegexMatcher("ORDINAL1", "(?ii)[0-9]+(?:st|nd|rd|th)")
    RegexMatcher("ORDINAL2", ("(?ii)(?:"
      + "[Ff]irst|[Ss]econd|[Tt]hird|[Ff]ourth|[Ff]ifth|[Ss]ixth|[Ss]eventh|[Ee]ighth|[Nn]inth|[Tt]enth"
      + "|[Ee]leventh|[Tt]welfth|[Tt]hirteenth|[Ff]ourteenth|[Ff]ifteenth|[Ss]ixteenth"
      + "|[Ss]eventeenth|[Ee]ighteenth|[Nn]ineteenth"
      + "|[Tt]wentieth|[Tt]hirtieth|[Ff]ou?rtieth|[Ff]iftieth|[Ss]ixtieth|[Ss]eventieth"
      + "|[Ee]ightieth|[Nn]ine?tieth|[Tt]wentieth|[Hh]undredth"
      + ")"))

    // Punctuation
    RegexMatcher("Punc", PUNC)
    RegexMatcher("LeadQuote", "[\"'`]")
    RegexMatcher("EndQuote", "[\"'`][^s]?")
    RegexMatcher("MultiHyphen", "\\S*-\\S*-\\S*")
    RegexMatcher("ContainsPunc", "[\\-,\\:\\;]")
    RegexMatcher("StopPunc", "[\\!\\?\\.\"\']")

    // Character-based
    RegexMatcher("LONELYINITIAL", CAPS + "\\.")
    RegexMatcher("CAPLETTER", CAPS)
    RegexMatcher("ALLCAPS", CAPS + "+")
    RegexMatcher("INITCAP", CAPS + ".*")
  }

  RegexMatcher("HASEDITOR", "(ed\\.|editor|editors|eds\\.)")
  RegexMatcher("HASINSTITUTE", "(University|Universite|Universiteit|Univ\\.?|Dept\\.?|Institute|Corporation|Department|Laboratory|Laboratories|Labs)")

  def addTrieFeatures(ts: LbledTokSeq, j: Int, ftrName: String, trie: RexaTrieDict) = {
    var begin = 0
    val words = ts.column(j)
    while (begin < words.length) {
      val end = trie.endIndexOf(words, begin)
      if (end >= begin) {
        forIndex(begin, end + 1, {
          i: Int => ts.features(i) += ftrName
        })
        begin = end + 1
      } else {
        begin += 1
      }
    }
  }

  def processLines(lines: Array[String], splitRegex: String = "\\s+", conjunctions: Array[Array[Int]] = null,
                   labelIndex: Int = 0, wordIndex: Int = 1): LbledTokSeq = {
    val ts = new LbledTokSeq(lines, splitRegex)
    processTokSeq(ts, conjunctions, labelIndex, wordIndex)
    ts
  }

  def processTokSeq(ts: LbledTokSeq, conjunctions: Array[Array[Int]] = null,
                    labelIndex: Int = 0, wordIndex: Int = 1): LbledTokSeq = {
    // apply normal feature functions
    ts.addFeaturesUsingFunctions(wordIndex, prefixToFtrFns)
    // add trie features
    if (usetriefeatures) prefixToTrieLexicon.keys.foreach {
      prefix: String => addTrieFeatures(ts, wordIndex, prefix, prefixToTrieLexicon(prefix))
    }
    // add offset conjuctions
    if (conjunctions != null && conjunctions.size > 0)
      ts.addFeatureConjuctions(conjunctions)
    // print labels
    if (debuglabels) {
      println(ts.column(labelIndex).mkString("\n"))
      println
    }
    // print features
    if (debugfeatures) {
      println(ts.features.map(_.mkString("\t")).mkString("\n"))
      println
    }
    ts
  }
}
