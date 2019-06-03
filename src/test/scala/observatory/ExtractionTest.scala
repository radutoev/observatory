package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {
  test("locateTemperature should work with given year") {
    val temp = Extraction.locateTemperatures(1986, "/stations.csv", "/1986.csv")
    assert(temp.size == 2429828)
  }
}