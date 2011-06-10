package cc.refectorie.user.kedarb.dbalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.types.Hypergraph
import com.mongodb.casbah.Imports._
import collection.mutable.HashSet
import misc.{CorefSegmentation, CorefSegment}
import fields.Field
import params.Params

/**
 * @author kedarb
 * @since 5/10/11
 */

class CorefSegmentationInferencer(val fields: Seq[Field], val joinFields: Boolean, val ex: CorefSegmentationExample,
                                  val params: Params, val counts: Params, val ispec: InferSpec)
  extends ASegmentationBasedInferencer[CorefSegmentation, CorefSegmentationExample] {
  type Widget = CorefSegmentation

  def newWidget: Widget = new Widget(N)

  /**
   * Returns possible record cluster ids.
   * If join then take records that match on all key fields else take union.
   */

  def getPossibleRecordClusterIds(join: Boolean): HashSet[ObjectId] = {
    new RecordBlocker(fields, words, cachedPossibleEnds, trueSegmentation, trueInfer).getPossibleRecordClusterIds(join)
  }

  /**
   * Hypergraph creation
   */
  def allowedTransitionCluster(recordClusterId: Option[ObjectId],
                               prev_l: Int, prevFieldClusterId: Option[ObjectId],
                               l: Int, fieldClusterId: Option[ObjectId]): Boolean = {
    if (!recordClusterId.isDefined) {
      // allow all possible transitions
      true
    } else {
      // only allow other field or non-empty field cluster
      fields(l).name == "O" || fieldClusterId.isDefined
    }
  }

  def scoreRecordCluster(recordClusterId: Option[ObjectId]): Double = {
    // TODO: replace with posterior regularization
    if (!recordClusterId.isDefined) -10.0 else 0.0
  }

  def updateRecordCluster(recordClusterId: Option[ObjectId], prob: Double): Unit = {}

  def scoreFieldClusterOverlap(recordClusterId: ObjectId, field: Field,
                               fieldClusterId: ObjectId, i: Int, j: Int): Double = 0.0

  def updateFieldClusterOverlap(recordClusterId: ObjectId, field: Field,
                                fieldClusterId: ObjectId, i: Int, j: Int, prob: Double): Unit = {}

  def scoreFieldClusterTransition(recordClusterId: ObjectId, prev_l: Int, prevFieldClusterId: Option[ObjectId],
                                  l: Int, fieldClusterId: Option[ObjectId]): Double = 0.0

  def updateFieldClusterTransition(recordClusterId: ObjectId, prev_l: Int, prevFieldClusterId: Option[ObjectId],
                                   l: Int, fieldClusterId: Option[ObjectId], prob: Double): Unit = {}

  def createHypergraph(H: Hypergraph[Widget]): Unit = {
    def genField(recordClusterId: Option[ObjectId], prev_l: Int, i: Int, prevFieldClusterId: Option[ObjectId]): Object = {
      val isStart = prev_l == -1 || i == 0
      if (i == N) H.endNode
      else {
        val node = {
          if (isStart) ('startRecord, recordClusterId, i)
          else ('continueRecord, recordClusterId, prev_l, i, prevFieldClusterId)
        }
        if (H.addSumNode(node)) {
          // println("added node: " + node)
          // only use similarity scores for key fields
          forIndex(L, (l: Int) => {
            val fld = fields(l)
            forIndex(i + 1, math.min(i + fld.maxSegmentLength, N) + 1, (j: Int) => {
              if (allowedSegment(l, i, j) && ((isStart && allowedStart(l)) || (!isStart && allowedTransition(prev_l, l)))) {
                // calculate emission score
                val emissionScore = scoreEmission(l, i, j)
                val transitionScore = if (isStart) scoreStart(l, j) else scoreTransition(prev_l, l, i, j)
                // add label without corresponding field
                if (allowedTransitionCluster(recordClusterId, prev_l, prevFieldClusterId, l, None))
                  H.addEdge(node, genField(recordClusterId, l, j, None), new Info {
                    def getWeight: Double = emissionScore + transitionScore

                    def choose(widget: Widget): Widget = {
                      val segment = CorefSegment(recordClusterId, None, l, i, j)
                      require(widget.append(segment), "Could not add segment!")
                      widget
                    }

                    def setPosterior(prob: Double): Unit = {
                      // update emission and transition counts
                      if (isStart) updateStart(l, j, prob) else updateTransition(prev_l, l, i, j, prob)
                      updateEmission(l, i, j, prob)
                    }
                  })
                if (recordClusterId.isDefined) {
                  // add field alignment
                  fld.getValueIdForRecordClusterId(recordClusterId.get).foreach {
                    fieldClusterId: ObjectId =>
                      if (allowedTransitionCluster(recordClusterId, prev_l, prevFieldClusterId, l, Some(fieldClusterId)))
                        H.addEdge(node, genField(recordClusterId, l, j, Some(fieldClusterId)), new Info {
                          def getWeight: Double = {
                            emissionScore + transitionScore +
                              scoreFieldClusterTransition(recordClusterId.get, prev_l, prevFieldClusterId, l, Some(fieldClusterId)) +
                              scoreFieldClusterOverlap(recordClusterId.get, fld, fieldClusterId, i, j)
                          }

                          def choose(widget: Widget): Widget = {
                            val segment = CorefSegment(recordClusterId, Some(fieldClusterId), l, i, j)
                            require(widget.append(segment), "Could not add segment!")
                            widget
                          }

                          def setPosterior(prob: Double): Unit = {
                            // update emission and transition counts
                            if (isStart) updateStart(l, j, prob) else updateTransition(prev_l, l, i, j, prob)
                            updateEmission(l, i, j, prob)
                            updateFieldClusterTransition(recordClusterId.get, prev_l, prevFieldClusterId, l, Some(fieldClusterId), prob)
                            updateFieldClusterOverlap(recordClusterId.get, fld, fieldClusterId, i, j, prob)
                          }
                        })
                  }
                }
              }
            })
          })
        }
        node
      }
    }

    /**
     * Only allow a single record per text
     */
    val possibleRecordClusterIds = getPossibleRecordClusterIds(joinFields)
    // outputPossibleRecordClusterIds(possibleRecordClusterIds)
    H.addEdge(H.sumStartNode, genField(None, -1, 0, None), new Info {
      def getWeight: Double = scoreRecordCluster(None)

      def setPosterior(prob: Double): Unit = {
        updateRecordCluster(None, prob)
      }

      def choose(widget: Widget): Widget = widget
    })
    possibleRecordClusterIds.foreach {
      recordClusterId: ObjectId =>
        H.addEdge(H.sumStartNode, genField(Some(recordClusterId), -1, 0, None), new Info {
          def getWeight: Double = scoreRecordCluster(Some(recordClusterId))

          def setPosterior(prob: Double): Unit = {
            updateRecordCluster(Some(recordClusterId), prob)
          }

          def choose(widget: Widget): Widget = widget
        })
    }
  }
}