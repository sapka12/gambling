import spray.json.RootJsonFormat
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._
import akka.actor.ActorSystem
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.kafka.scaladsl.Producer
import akka.serialization.ByteArraySerializer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.JavaFlowSupport.Sink
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val serializer = new StringSerializer

  val producerSettings = ProducerSettings[String, String](system, serializer, serializer)
    .withBootstrapServers("localhost:9092")

  def send[T](state: T)(implicit format: RootJsonFormat[T]) = {

    Source(List(state))
      .map(_.toJson.compactPrint)
      .map { json =>
        new ProducerRecord[String, String]("gambling", json)
      }
      .runWith(Producer.plainSink[String, String](producerSettings))

  }

}
