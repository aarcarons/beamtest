package example.transforrms

import com.spotify.scio.values.SCollection

import scala.collection.immutable.TreeSet

object Indexer {
  def index(input: SCollection[(String, String)]): SCollection[(String, TreeSet[String])] =
    // Group words by term and aggregate doc ids in a TreeSet
    // TreeSet is implemented with a RB tree
    // TreeSet insert is O(logN)
    // TreeSet merge is O(NlogN)
    input.combineByKey(TreeSet(_))(_ + _)(_ ++ _) // O(N log N)
}
