package beamtest

import com.spotify.scio.testing.PipelineSpec
import beamtest.transforrms.SorterByKey

class SortByKeyTest extends PipelineSpec {
  "Sorter" should "sort by term name" in {
    // We receive an SCollection that contains pairs of String -> String
    // representing term -> (already sorted docIds)
    // Now we want to sort this SCollection lexicographically by the term

    val data = Seq(
      ("unbalance", "3"),
      ("zip", "1"),
      ("abyss", "1, 2, 3")
    )

    val expected = Seq(
      ("sort",Iterable(("abyss", "1, 2, 3"), ("unbalance", "3"), ("zip", "1")))
    )

    runWithContext { sc =>
      val d = SorterByKey.sort(sc.parallelize(data))
      d should containInAnyOrder(expected)
    }
  }
}
