package cc.refectorie.user.kedarb.dbalign

import com.mongodb.casbah.Imports._
import misc.KnowledgeBase

/**
 * @author kedarb
 * @since 3/28/11
 */

trait ACorefSegmentationEnv {
  /**
   * The processed knowledge base. May be used to store other information.
   */
  def processedKB: KnowledgeBase

  /**
   * The collection of mentions
   */
  def mentionColl: MongoCollection

  /**
   * Mention value converters
   */
  def getStringArray(mention: DBObject, name: String = "words"): Array[String] = {
    mention.as[BasicDBList](name).toArray.map(_.toString)
  }

  def isRecord(mention: DBObject): Boolean = mention.as[Boolean]("isRecord")

  def getStringFeatures(mention: DBObject): Array[String]

  def getStringVectorFeatures(mention: DBObject): Array[Seq[String]]
}