package cc.refectorie.user.kedarb.dbalign

import uk.ac.shef.wit.simmetrics.similaritymetrics._

/**
 * @author kedarb
 * @since 5/17/11
 */

object EditApp {
  val jaccard = new JaccardSimilarity
  val jaroWinkler = new JaroWinkler
  val cosine = new CosineSimilarity
  val dice = new DiceSimilarity
  val levenshtein = new Levenshtein

  def main(args: Array[String]) {
    println("lev(larkspur, landspur)=" + levenshtein.getSimilarity("larkspur", "landspur"))
    println("jw(intl, international)=" + levenshtein.getSimilarity("intl", "international"))
  }
}