package observatory

import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.util.concurrent.Executors

import cats.implicits._
import cats.effect.{ContextShift, IO}
import fs2.{Pipe, Stream, io, text}

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * 1st milestone: data extraction
  * https://github.com/syhan/coursera/blob/master/scala-capstone/observatory/src/main/scala/observatory/Extraction.scala
  */
object Extraction {
  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  /**
    * This method should return the list of all the temperature records converted in degrees Celsius along with their date and location
    * (ignore data coming from stations that have no GPS coordinates).
    * You should not round the temperature values.
    *
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    (for {
      stations <- readFile(stationsFile)
        .map(parseStationsLine)
        .through(optionToValue).compile.toList
      stationsMap = stations.toMap
      temperatures <- readFile(temperaturesFile)
        .map(parseTempLine)
        .through(optionToValue)
        .map { case (sk, tempAtDate) => stationsMap.get(sk).map(l => (LocalDate.of(year, tempAtDate.month, tempAtDate.day), l, tempAtDate.temp)) }
        .through(optionToValue)
        .compile
        .toList
    } yield temperatures).unsafeRunSync()
  }

  private[observatory] val fahrenheitToCelsius: Double => Double =
    fahrenheit => (fahrenheit - 32.0) * (5.0 / 9.0)

  private[observatory] def readFile(fileName: String): Stream[IO, String] =
    io.file.readAll[IO](Paths.get(getClass.getResource(fileName).toURI), blockingEc, 4096)
      .through(csvParser)

  private[observatory] def csvParser[F[_]]: Pipe[F, Byte, String] = _.through(text.utf8Decode andThen text.lines)

  private[observatory] def optionToValue[F[_], A]: Pipe[F, Option[A], A] =
    _.through(_.filter(_.isDefined).map(_.get))

  private[observatory] val parseStationKey: (String, String) => StationKey = (stn, wban) =>
    StationKey(Try(stn.toInt).toOption, Try(wban.toInt).toOption)

  private[observatory] val parseStationsLine: String => Option[(StationKey, Location)] =
    _.split(",") match {
      case Array(stn, wban, lat, lon) => Some((parseStationKey(stn, wban), Location(lat.toDouble, lon.toDouble)))
      case _ => None
    }

  private[observatory] val parseTempLine: String => Option[(StationKey, TempsLine)] =
    _.split(",") match {
      case Array(stn, wban, month, day, temp) if temp != "9999.9" =>
        Try((parseStationKey(stn, wban), TempsLine(month.toInt, day.toInt, fahrenheitToCelsius(temp.toDouble)))).toOption
      case _ => None
    }


  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
