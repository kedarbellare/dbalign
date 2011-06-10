package cc.refectorie.user.kedarb.dbalign

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.AExample
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import misc.CorefSegmentation

/**
 * @author kedarb
 * @since 5/10/11
 */

trait ACorefSegmentationExample[Widget] extends AExample[Widget] {
  def isRecord: Boolean

  def _id: ObjectId

  def words: Array[String]

  def features: Array[(Int, FtrVec)]

  def numTokens: Int = words.length

  lazy val featSeq: Array[Int] = features.map(_._1)

  lazy val featVecSeq: Array[FtrVec] = features.map(_._2)

  def trueSegmentation: Segmentation

  def isPossibleEnd(j: Int): Boolean = true
}

class SegmentationOnlyExample(val _id: ObjectId, val isRecord: Boolean, val words: Array[String],
                              val features: Array[(Int, FtrVec)], val trueSegmentation: Segmentation)
  extends ACorefSegmentationExample[Segmentation] {
  val trueWidget: Segmentation = trueSegmentation
}

class CorefSegmentationExample(val _id: ObjectId, val isRecord: Boolean, val words: Array[String],
                               val features: Array[(Int, FtrVec)], val trueSegmentation: Segmentation)
  extends ACorefSegmentationExample[CorefSegmentation] {
  val trueWidget: CorefSegmentation = null
}