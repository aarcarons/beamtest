package beamtest.transforrms

import com.spotify.scio.extra.sorter._
import com.spotify.scio.values.SCollection

object SorterByKey {
  def sort(s: SCollection[(String, String)]) = {
    s.map(t => ("sort", (t._1, t._2)))
      .groupByKey
      .sortValues(100)
  }
}
