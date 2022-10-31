package playground

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import akka.util.ByteString
import akka.util.ByteString.empty.utf8String
import kantan.csv._
import kantan.csv.ops._

import java.nio.file.Paths
import scala.concurrent.Future

object CSVReaderTest extends App {

  case class TaxiTripEntry(vendorID: Int, tpepPickupDatetime: String, tpepDropoffDatetime: String, passengerCount: Int,
                           tripDistance: Double, pickupLongitude: Double, pickupLatitude: Double, rateCodeID: Int,
                           storeAndFwdFlag: String, dropoffLongitude: Double, dropoffLatitude: Double,
                           paymentType: Int, fareAmount: Double, extra: Double, mtaTax: Double,
                           tipAmount: Double, tollsAmount: Double, improvementSurcharge: Double, totalAmount: Double)

  def fromCsvEntryToCaseClass(csvEntry : String): TaxiTripEntry ={
    val arrayString = csvEntry.split(",")
    println(s"Trying to parse $csvEntry")
    TaxiTripEntry(
      arrayString(0).toInt,
      arrayString(1).toString,
      arrayString(2).toString,
      arrayString(3).toInt,
      arrayString(4).toDouble,
      arrayString(5).toDouble,
      arrayString(6).toDouble,
      arrayString(7).toInt,
      arrayString(8).toString,
      arrayString(9).toDouble,
      arrayString(10).toDouble,
      arrayString(11).toInt,
      arrayString(12).toDouble,
      arrayString(13).toDouble,
      arrayString(14).toDouble,
      arrayString(15).toDouble,
      arrayString(16).toDouble,
      arrayString(17).toDouble,
      arrayString(18).toDouble
    )
  }

  val file = Paths.get("src/main/resources/1ksample.csv")

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()


  //sources -> produce stuff
  val source = FileIO
    .fromPath(file)

  //flow transform
//  val toStringFlow = Flow[ByteString].map(byteString => {
//    println("Converting to taxi entry")
//    val stringVal = byteString.utf8String
//    fromCsvEntryToCaseClass(stringVal)
//  })
  val toStringFlow = Framing.delimiter(ByteString("\n"), 256, true).map(_.utf8String)
  val toTaxiEntryFlow = Flow[String].map(fromCsvEntryToCaseClass)

  //sinks -> consume stuff
  val sink = Sink.foreach[TaxiTripEntry](x => println(x.toString()))

  val runnableGraph = source.via(toStringFlow).via(toTaxiEntryFlow).to(sink)
  runnableGraph.run()

  //val graph = source.to(sink)
  //graph.run()
}
