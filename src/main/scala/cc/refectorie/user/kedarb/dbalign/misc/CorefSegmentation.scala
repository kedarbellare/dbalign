package cc.refectorie.user.kedarb.dbalign.misc

import com.mongodb.casbah.Imports._
import collection.mutable.ArrayBuffer
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment.{Segmentation, Segment}

/**
 * A segmentation that also holds the record and field clusters within it.
 *
 * @author kedarb
 * @since 3/27/11
 */

case class CorefSegment(recordClusterId: Option[ObjectId], fieldClusterId: Option[ObjectId],
                        label: Int, begin: Int, end: Int)

class CorefSegmentation(val length: Int) {
  val indexToCorefSegment = new Array[CorefSegment](length)
  val corefSegments = new ArrayBuffer[CorefSegment]
  val indexToSegment = new Array[Segment](length)
  val segments = new ArrayBuffer[Segment]

  def corefSegmentAt(i: Int): Option[CorefSegment] = {
    if (indexToCorefSegment(i) == null) None
    else Some(indexToCorefSegment(i))
  }

  def segmentAt(i: Int): Option[Segment] = {
    if (indexToSegment(i) == null) None
    else Some(indexToSegment(i))
  }

  def labelAt(i: Int): Option[Int] = {
    val s = corefSegmentAt(i)
    if (s == None) None
    else Some(s.get.label)
  }

  def numSegments = corefSegments.size

  def corefSegment(i: Int): CorefSegment = corefSegments(i)

  def segment(i: Int): Segment = segments(i)

  def isValid(s: CorefSegment): Boolean = {
    if (s.begin < 0 || s.begin >= length) false
    else if (s.end < 1 || s.end > length) false
    else if (s.end <= s.begin) false
    else {
      forIndex(s.begin, s.end, {
        i => if (indexToCorefSegment(i) != null) return false
      })
      true
    }
  }

  private def addIndex2SegmentIsValid(s: CorefSegment): Boolean = {
    if (isValid(s)) {
      forIndex(s.begin, s.end, {
        i =>
          indexToCorefSegment(i) = s
          indexToSegment(i) = Segment(s.begin, s.end, s.label)
      })
      true
    } else {
      false
    }
  }

  def append(s: CorefSegment): Boolean = {
    val success = addIndex2SegmentIsValid(s)
    if (success) {
      corefSegments += s
      segments += Segment(s.begin, s.end, s.label)
    }
    success
  }

  def prepend(s: CorefSegment): Boolean = {
    val success = addIndex2SegmentIsValid(s)
    if (success) {
      corefSegments.insert(0, s)
      segments.insert(0, Segment(s.begin, s.end, s.label))
    }
    success
  }

  def getSegmentation: Segmentation = {
    val segmentation = new Segmentation(length)
    segments.foreach(seg => segmentation.append(seg))
    segmentation
  }

  def contains(s: CorefSegment): Boolean = corefSegments.contains(s)

  def contains(s: Segment): Boolean = segments.contains(s)

  override def toString = corefSegments.mkString("[", ", ", "]")
}