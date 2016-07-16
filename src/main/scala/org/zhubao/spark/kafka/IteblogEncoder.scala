package org.zhubao.spark.kafka

import kafka.utils.VerifiableProperties
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import kafka.serializer.Encoder

class IteblogEncoder[T](props: VerifiableProperties = null) extends Encoder[T] {
 
    override def toBytes(t: T): Array[Byte] = {
      if (t == null)
        null
      else {
        var bo: ByteArrayOutputStream = null
        var oo: ObjectOutputStream = null
        var byte: Array[Byte] = null
        try {
          bo = new ByteArrayOutputStream()
          oo = new ObjectOutputStream(bo)
          oo.writeObject(t)
          byte = bo.toByteArray
        } catch {
          case ex: Exception => return byte
        } finally {
          bo.close()
          oo.close()
        }
        byte
      }
    }
}