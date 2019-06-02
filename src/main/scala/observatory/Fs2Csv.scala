package observatory

import fs2.{Pipe, RaiseThrowable, text}

object Fs2Csv {
  def parse[F[_]: RaiseThrowable](sep: String): Pipe[F, String, Map[String, String]] =
    parse(sep, identity)

  def parseLenient[F[_], I](sep: String): Pipe[F, String, Either[Throwable, Map[String, String]]] =
    parseLenient(sep, identity)

  def parse[F[_]: RaiseThrowable, I](sep: String, convert: I => String): Pipe[F, I, Map[String, String]] =
    csvBytes => parseLenient(sep, convert)(csvBytes).rethrow

  def parseLenient[F[_], I](sep: String, convert: I => String): Pipe[F, I, Either[Throwable, Map[String, String]]] =
    csvBytes =>
      csvBytes
        .map(convert(_))
        .through(text.lines)
        .filter(_.trim.nonEmpty)
        .through(splitLine(sep))
        .through(zipWithHeader)

  def splitLine[F[_]](sep: String): Pipe[F, String, Vector[String]] =
    _.map(_.split(sep, -1).toVector.map(_.trim))

  def zipWithHeader[F[_]]: Pipe[F, Vector[String], Either[Throwable, Map[String, String]]] =
    csvRows =>
      csvRows.zipWithIndex
        .mapAccumulate(Option.empty[Vector[String]]) {
          case (None, (headerRow, _)) =>
            (Some(headerRow), Right(Map.empty[String, String]))
          case (h @ Some(header), (row, rowIndex)) =>
            if (header.length == row.length)
              h -> Right(header.zip(row).toMap)
            else
              h -> Left(HeaderSizeMismatch(rowIndex, header.length, row))
        }
        .drop(1)
        .map(_._2)

  case class HeaderSizeMismatch(rowIndex: Long,
                                headerLength: Int,
                                row: Vector[String]) extends Throwable (
      s"CSV row at index $rowIndex has ${row.length} items, header has $headerLength. Row: $row"
  )
}
