package example

import example.transforrms.LineParser
import org.scalatest._
import org.scalatest.matchers.should._

class LineParserTest extends FlatSpec with Matchers {

  "LineParserTest" should "filter duplicates" in {
      val p = LineParser.parse("dupe dupe")
      assert(p.size == 1)
    }

  it should "trim leading and trailing line spaces" in {
      val p = LineParser.parse(f" word1 word2 ")
      p should contain ("word1", "word2")
  }

  it should "trim single spaces between words" in {
      val p = LineParser.parse("word1 word2")
      p should contain ("word1", "word2")
  }

  it should "treat apostrophes as the same word unit" in {
      val p = LineParser.parse("word1's word's")
      p should contain ("word1's", "word's")
    }

  it should "trim multiple spaces between words" in {
      val p = LineParser.parse("word1            word2")
      p should contain("word1", "word2")
    }
}