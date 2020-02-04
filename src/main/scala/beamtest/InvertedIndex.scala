package beamtest

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import beamtest.transforrms.{Indexer, LineParser, SorterByKey}
import org.apache.beam.sdk.io.FileSystems

import scala.collection.JavaConverters._

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object InvertedIndex {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")

    // Collect all the files that match the input pattern
    val fileNames = FileSystems
      .`match`(input)
      .metadata()
      .asScala
      .map(_.resourceId())

    // Read all the files in parallel, and generate (docId - line) tuples
    val allLines = SCollection.unionAll(fileNames.map(uri => sc.textFile(uri.toString).keyBy(_ => uri.getFilename)))

    // Parse the lines and convert them into words.
    // Invert the tuple format so its (word -> docId)
    val wordsToDocId = allLines.flatMap {
      case (uri, line) =>
        LineParser.parse(line)
          // Build word -> doc tuples
          .map(w => (w,  uri))
    }


    wordsToDocId
      // Index results by term. As an output we get pairs like
      // (word1, TreeSet("doc1", "doc2",...))
      // Storing the document numbers in a TreeSet allows us to:
      // (1) deduplicate docs
      // (2) order docs lexicographically
      .transform(Indexer.index(_))

      // ASSUMPTION: The data from this stage of the pipeline until the end
      // can fit in the same machine, since we've reduced the keys to distinct words

      // Transform the values so we convert the treeset into the desired comma-separated string "doc1,doc2,..."
      .mapValues(_.mkString(","))

      // Sort the pairs lexicographically by the term
      .transform(SorterByKey.sort(_))

      // To sort we had to use some sort of a "trick". The Beam sorter can't sort
      // by the key, since that would involve a serial operation.
      // Instead, it can only sort values. It does that by grouping all the values with the same key.
      // Then keys and their values are distributed on the workers and sorted locally.
      // To trick the sorter, we have to make our original scollection as the value of an artificial
      // key, and then sort. This key is ignored here
      //         (*)
      .map(t => t._2.map(kv => f"${kv._1}: ${kv._2}").mkString("\n"))

      .saveAsTextFile(output)

    // Run the pipeline
    sc.run().waitUntilFinish()
  }
}
