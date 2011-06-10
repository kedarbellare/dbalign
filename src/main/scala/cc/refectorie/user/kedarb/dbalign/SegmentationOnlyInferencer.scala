package cc.refectorie.user.kedarb.dbalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment.{Segmentation, Segment}
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.types.Hypergraph
import fields.Field
import params.Params

/**
 * @author kedarb
 * @since 5/10/11
 */

class SegmentationOnlyInferencer(val fields: Seq[Field], val ex: SegmentationOnlyExample,
                                 val params: Params, val counts: Params, val ispec: InferSpec)
  extends ASegmentationBasedInferencer[Segmentation, SegmentationOnlyExample] {
  type Widget = Segmentation

  def newWidget = new Widget(N)

  def createHypergraph(H: Hypergraph[Widget]): Unit = {
    def gen(a: Int, i: Int): Object = {
      if (i == N) H.endNode
      else {
        val node = (a, i)
        if (H.addSumNode(node)) {
          forIndex(L, (b: Int) => {
            forIndex(i + 1, math.min(i + fields(b).maxSegmentLength, N) + 1, (j: Int) => {
              if (allowedTransition(a, b) && allowedSegment(b, i, j)) {
                H.addEdge(node, gen(b, j), new Info {
                  def getWeight = scoreTransition(a, b, i, j) + scoreEmission(b, i, j)

                  def setPosterior(v: Double) = {
                    updateTransition(a, b, i, j, v)
                    updateEmission(b, i, j, v)
                  }

                  def choose(widget: Widget) = {
                    val seg = Segment(i, j, b)
                    require(widget.append(seg), "Could not add segment: " + seg)
                    widget
                  }
                })
              }
            })
          })
        }
        node
      }
    }

    forIndex(L, (a: Int) => {
      forIndex(1, math.min(fields(a).maxSegmentLength, N) + 1, (i: Int) => {
        if (allowedStart(a) && allowedSegment(a, 0, i)) {
          H.addEdge(H.sumStartNode, gen(a, i), new Info {
            def getWeight = scoreStart(a, i) + scoreEmission(a, 0, i)

            def setPosterior(v: Double) = {
              updateStart(a, i, v)
              updateEmission(a, 0, i, v)
            }

            def choose(widget: Widget) = {
              val seg = Segment(0, i, a)
              require(widget.append(seg), "Could not add segment: " + seg)
              widget
            }
          })
        }
      })
    })
  }
}
