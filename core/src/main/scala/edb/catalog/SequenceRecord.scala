/* Record: we will implement Writable  interface. 
 This is the value of SequenceInputFormat. 
 The key of SequenceInputFormat is the byteArrayWriable of expression list 

 - schema ID: int
 - schema Version: byte
 - data : array of genericValue

 - readFields: StreamIn
 - write: StreamOut

 - equals

 - toString

 */
package edb.catalog

import scala._
import scala.io._
import scala.reflect.BeanProperty
import scala.util.control.Breaks._

import org.apache.hadoop.io._

import java.util._
import java.io.DataInput
import java.io.DataOutput

class SequenceRecord extends  Writable with Serializable {

  @BeanProperty  var schId: Int = _
  @BeanProperty  var schVersion: Byte = _
  @transient @BeanProperty var sch: Schema = _
  @BeanProperty  var data: Array[genericValue]= _

  /* auxiliary constructor */
  def this(in: Array[genericValue]){
    //must call the default constructor
    this()
    setData(in)
  }

  /* deserialize data */
  override def readFields(in: DataInput){

    //1. read in schema id and version
    setSchId(in.readInt())
    setSchVersion(in.readByte())

    setSch(new Schema(schId, schVersion))


    val atts = sch.getAtts()
    val numAtts = sch.getNumAtts()

    //create data array
    setData(new Array[genericValue](numAtts))

    //2. read data in (deserialize data)
    for (i <- 0 to numAtts -1){
      data(i) = atts(i)  match {
        case a1: IntAtt => new IntVal(a1.getName(),in.readInt)
        case a2: LongAtt => new LongVal(a2.getName(),in.readLong)
        case a3: DoubleAtt => new DoubleVal(a3.getName(),in.readDouble)
        case a4: FloatAtt => new FloatVal(a4.getName(),in.readFloat)
        case a5: StringAtt => new StringVal(a5.getName(), in.readUTF)
        case a6: DateAtt => new DateVal(a6.getName(), in.readLong)
        case _ => {
          Console.err.println("Unsupported attribute type "+ atts(i).toString)
          exit(100)
        }
      }//end match
    }//end for
  }

  /* serialize data */ 
  override def write(out: DataOutput){

    if (data != null && sch != null) {

      //output id and version of schema first
      out.writeInt(sch.getId())
      out.writeByte(sch.getVersion())

      val numAtts = sch.getNumAtts()
      //serialize data
      for (i <- 0 to numAtts -1){
        data(i)  match {
          case a1: IntVal => out.writeInt(a1.getValue())
          case a2: LongVal => out.writeLong(a2.getValue())
          case a3: DoubleVal => out.writeDouble(a3.getValue())
          case a4: FloatVal => out.writeFloat(a4.getValue())
          case a5: StringVal => out.writeUTF(a5.getValue())
          case a6: DateVal => out.writeLong(a6.getValue())
          case _ => {
            Console.err.println("Unsupported attribute type "+ data(i).toString)
            exit(100)
          }
        }//end match
      }//end for
    }
  }

  /**
    * Two records are equal only when they hold same values 
    * @param other SequenceRecord to compare with
    * @return ture when current record is equal to other
    */
  override def equals(other: Any): Boolean = other.isInstanceOf[SequenceRecord] && {
    val o: SequenceRecord = other.asInstanceOf[SequenceRecord]
    o.getSchId == this.getSchId && o.getSchVersion == this.getSchVersion && 
    o.getSch == this.getSch && o.getData.corresponds(this.getData){_ == _}
  }

  //we use data.toList since array.toHash is not deterministic
  override def hashCode: Int = data.toList.hashCode

  override def toString = {
    var str = ""
    if (data == null || data != null && data.length == 0)
      str = "null"
    else {

      for (i<-0 to data.length -1 ){
        val v = data(i)
        v match {
          case o1: IntVal => str += o1.toString + ","
          case o2: DoubleVal => str += o2.toString + ","
          case o3: FloatVal=> str += o3.toString + ","
          case o4: LongVal => str += o4.toString + ","
          case o5: BooleanVal => str += o5.toString + ","
          case o6: StringVal => str += o6.toString + ","
          case o7: DateVal => str += o7.toString + ","
          case _ => str += "[unsupported type],"
        }
      }
      //get rid off the last comma
      str = str.substring(0, str.length-1)
    }//end else 
    str
  }//end toString


  /**
    * This function make current SequenceRecord copy
    * the fromMe record
    * @param fromMe the src SequenceRecord instance
    * @return nothing
    *
    */
  def copy(): SequenceRecord = {

    val copy = new SequenceRecord()

    copy.setSchId(getSchId)
    copy.setSchVersion(getSchVersion)
    copy.setSch(new Schema(schId, schVersion))

    val atts = sch.getAtts
    val numAtts = sch.getNumAtts

    //create data array
    copy.setData(new Array[genericValue](numAtts))
    val copyData = copy.getData

    //2. read data in (deserialize data)
    for (i <- 0 to numAtts -1){
      copyData(i) = data(i)  match {
        case a1: IntVal => a1.copy()
        case a2: LongVal => a2.copy()
        case a3: DoubleVal =>a3.copy()
        case a4: FloatVal => a4.copy()
        case a5: StringVal => a5.copy()
        case a6: DateVal => a6.copy()
        case _ => {
          Console.err.println("Unsupported attribute type "+ atts(i).toString)
          exit(100)
        }
      }//end match
    }//end for

    copy
  }
  /** 
    * clone from the current record to a new record 
    * with a projection list 
    * @param attList projection list, an atts index
    */
  def copy(attList: Array[Int]): SequenceRecord = {

    //a new schema consists of projection list
    val sch = new Schema() 
    sch.setSchema(attList,getSch())

    val atts = sch.getAtts

    val copy = new SequenceRecord()
    copy.setSchId(sch.getId)
    copy.setSchVersion(sch.getVersion)

    //create data array
    copy.setData(new Array[genericValue](sch.getNumAtts))
    val copyData = copy.getData
    
    val numAtts = sch.getNumAtts

    //2. project data
    for (i <- 0 to numAtts -1){
      copyData(i) = data(attList(i))  match {
        case a1: IntVal => a1.copy()
        case a2: LongVal => a2.copy()
        case a3: DoubleVal =>a3.copy()
        case a4: FloatVal => a4.copy()
        case a5: StringVal => a5.copy()
        case a6: DateVal => a6.copy()
        case _ => {
          Console.err.println("Unsupported attribute type "+ atts(i).toString)
          exit(100)
        }
      }//end match
    }//end for

    copy
  }

}//end class


