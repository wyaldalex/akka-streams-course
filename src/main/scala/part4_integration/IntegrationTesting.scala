package part4_integration

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration._

object IntegrationTesting extends App {



  implicit val actorSystem = ActorSystem("integrationStreamsSys")
  implicit val  timeout = Timeout(3 seconds)
  implicit val materializer = ActorMaterializer()

  object SimpleActor {
    def props : Props = Props(new SimpleActor)
  }
  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s: String =>
        log.info(s"Just received a string: $s")
        sender() ! s"$s$s"
      case n: Int =>
        log.info(s"Just received a number: $n")
        sender() ! (2 * n)
      case _ =>
    }
  }

  val simpleActor = actorSystem.actorOf(SimpleActor.props, "simpleActor")

  val numbersSource = Source(1 to 10)
  val actorBasedFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

  //numbersSource.via(actorBasedFlow).to(Sink.ignore).run()
  numbersSource.via(actorBasedFlow).to(Sink.foreach(x =>
    {
      println(s"Receving response from actor with value: ${x}")
    })).run()

  val actorPoweredSource = Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
  val someSink = Sink.foreach[Int](x => {
    println(s"Receving actor graph response: ${x}")
  })
  val materilizedActorRef = actorPoweredSource.to(someSink).run()
  materilizedActorRef ! 1000


  object DestinationActor {
    case object StreamInit
    case object StreamAck
    case object StreamComplete
    case class StreamFail(ex: Throwable)
    def props : Props = Props(new DestinationActor)
  }

  class DestinationActor extends Actor with ActorLogging {
    import DestinationActor._
    override def receive: Receive = {
      case StreamInit =>
        log.info("Stream initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.warning(s"Stream failed: $ex")
      case message =>
        log.info(s"Message $message has come to its final resting point.")
        sender() ! StreamAck
    }
  }
  val destinationActor = actorSystem.actorOf(DestinationActor.props, "destinationActor")
  val actorPoweredSink = Sink.actorRefWithAck[Int](
    destinationActor,
    onInitMessage = DestinationActor.StreamInit,
    onCompleteMessage = DestinationActor.StreamComplete,
    ackMessage = DestinationActor.StreamAck,
    onFailureMessage = throwable => DestinationActor.StreamFail(throwable) // optional
  )
  Source(1 to 100).to(actorPoweredSink).run()

}
