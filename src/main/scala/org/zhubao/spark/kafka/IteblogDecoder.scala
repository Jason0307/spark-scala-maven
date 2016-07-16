package org.zhubao.spark.kafka

import java.io.ObjectInputStream
import java.io.ByteArrayInputStream
import kafka.utils.VerifiableProperties
import kafka.serializer.Decoder

class IteblogDecoder[T](props: VerifiableProperties = null) extends Decoder[T] {
 
    def fromBytes(bytes: Array[Byte]): T = {
      var t: T = null.asInstanceOf[T]
      var bi: ByteArrayInputStream = null
      var oi: ObjectInputStream = null
      try {
        bi = new ByteArrayInputStream(bytes)
        oi = new ObjectInputStream(bi)
        t = oi.readObject().asInstanceOf[T]
      }
      catch {
        case e: Exception => {
          e.printStackTrace(); null
        }
      } finally {
        bi.close()
        oi.close()
      }
      t
    }
}