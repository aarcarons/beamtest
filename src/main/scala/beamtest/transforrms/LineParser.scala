package beamtest.transforrms

object LineParser {
  def parse(line: String): Iterable[String] =
    line
      .trim
      .split("\\s+")
      .filter(_.nonEmpty)
}
