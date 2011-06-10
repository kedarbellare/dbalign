package cc.refectorie.user.kedarb.dbalign.fields

import collection.mutable.HashSet

/**
 * @author kedarb
 * @since 3/25/11
 */

object PhraseHash {
  val _noHash = new HashSet[String]

  def validWord(word: String) = !word.matches("^[^A-Za-z0-9]*$")

  def noHash(phrase: Seq[String]): HashSet[String] = _noHash

  def unigramWordHash(phrase: Seq[String]): HashSet[String] = {
    var keys = new HashSet[String]
    phrase.filter(validWord(_)).foreach(w => {
      keys += w.toLowerCase
    })
    keys
  }

  def bigramWordHash(phrase: Seq[String]): HashSet[String] = {
    var keys = new HashSet[String]
    var prev = "$begin$"
    val buff = new StringBuffer
    (phrase ++ Seq("$end$")).filter(validWord(_)).foreach(w => {
      val curr = w.toLowerCase
      buff.setLength(0)
      buff.append(prev)
      buff.append('#')
      buff.append(curr)
      keys += buff.toString
      prev = curr
    })
    keys
  }

  def trigramWordHash(phrase: Seq[String]): HashSet[String] = {
    var keys = new HashSet[String]
    var pprev = "$begin2$"
    var prev = "$begin$"
    val buff = new StringBuffer
    (phrase ++ Seq("$end$", "$end2$")).filter(validWord(_)).foreach(w => {
      val curr = w.toLowerCase
      buff.setLength(0)
      buff.append(pprev)
      buff.append('#')
      buff.append(prev)
      buff.append('#')
      buff.append(curr)
      keys += buff.toString
      pprev = prev
      prev = curr
    })
    keys
  }

  def bigramCharHash(phrase: Seq[String]): HashSet[String] = {
    var keys = new HashSet[String]
    phrase.filter(validWord(_)).foreach(w => {
      keys ++= bigramWordHash(w.toLowerCase.map(_.toString).toSeq)
    })
    keys
  }

  def trigramCharHash(phrase: Seq[String]): HashSet[String] = {
    var keys = new HashSet[String]
    phrase.filter(validWord(_)).foreach(w => {
      keys ++= trigramWordHash(w.toLowerCase.map(_.toString).toSeq)
    })
    keys
  }
}

