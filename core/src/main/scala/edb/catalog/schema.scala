package edb.catalog
import scala._
import scala.io._
import scala.reflect.BeanProperty
import java.util._
import scala.util.control.Breaks._
import com.google.common.base._

/* define the supported attribute type */
sealed abstract class Attribute extends Serializable  {
  @BeanProperty var name: String = _
  def copy(): Attribute 
}

/* scala will generate equals and hashCode for 
 * case class. So, no need to override it
 */
case class IntAtt (_name: String) extends Attribute 
{
  setName(_name)
  override def toString = "int " + name
  override def copy(): Attribute = new IntAtt(getName())
}

case class FloatAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "float " + name
  override def copy(): Attribute = new FloatAtt(getName())
}

case class DoubleAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "double " + name
  override def copy(): Attribute = new DoubleAtt(getName())
}

case class LongAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "long " + name
  override def copy(): Attribute = new LongAtt(getName())
}

case class BooleanAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "boolean " + name
  override def copy(): Attribute = new BooleanAtt(getName())
}

case class StringAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "string " + name
  override def copy(): Attribute = new StringAtt(getName())
}


case class DateAtt (_name: String) extends Attribute
{
  setName(_name)
  override def toString = "date" + name
  override def copy(): Attribute = new DateAtt(getName())
}


/* define the supported value type, we use case class so that
 * scala generatate HashCode and comparison method 
 */
sealed abstract class genericValue extends Serializable {
  @BeanProperty var name: String = _

  def +(that: genericValue): genericValue
  def -(that: genericValue): genericValue
  def *(that: genericValue): genericValue
  def /(that: genericValue): genericValue

  def >(that: genericValue): Boolean 
  def <(that: genericValue): Boolean 
  def >=(that: genericValue): Boolean 
  def <=(that: genericValue): Boolean 
  //eq is reserved
  def eqs(that: genericValue): Boolean 
  //neq is probably reserved too
  def neqs(that: genericValue): Boolean 
}

case class IntVal (val _name: String, val _value: Int)
extends genericValue {

  @BeanProperty var value: Int = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":int " + value + "]"
  def copy(): IntVal = new IntVal(getName(),getValue())


  def +(that: genericValue): genericValue = that match {
    case a1: IntVal => new IntVal("", this.getValue + a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue + a2.getValue)
    case a3: LongVal => new LongVal ("", this.getValue + a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue + a4.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case a1: IntVal => new IntVal("", this.getValue - a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue - a2.getValue)
    case a3: LongVal => new LongVal ("", this.getValue - a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue - a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case a1: IntVal => new IntVal("", this.getValue * a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue * a2.getValue)
    case a3: LongVal => new LongVal ("", this.getValue * a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue * a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {
    case a1: IntVal => 
    {
      assert(a1.getValue !=0)
      new DoubleVal("", this.getValue / a1.getValue)
    }
    case a2: DoubleVal =>
    {
      assert(a2.getValue !=0)
      new DoubleVal("", this.getValue / a2.getValue)
    }
    case a3: LongVal => 
    {
      assert(a3.getValue !=0)
      new DoubleVal ("", this.getValue / a3.getValue) 
    }
    case a4: FloatVal => 
    {
      assert(a4.getValue !=0)
      new FloatVal("", this.getValue / a4.getValue)
    }
    case _ => throw new EdbException("not supported op " + that)
  }

  //end arithmatic operator
  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: IntVal => this.getValue > a1.getValue
    case a2: FloatVal => this.getValue > a2.getValue
    case a3: DoubleVal => this.getValue > a3.getValue
    case a4: LongVal => this.getValue > a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue < a1.getValue
    case a2: FloatVal => this.getValue < a2.getValue
    case a3: DoubleVal => this.getValue < a3.getValue
    case a4: LongVal => this.getValue < a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue >= a1.getValue
    case a2: FloatVal => this.getValue >= a2.getValue
    case a3: DoubleVal => this.getValue >= a3.getValue
    case a4: LongVal => this.getValue >= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue <= a1.getValue
    case a2: FloatVal => this.getValue <= a2.getValue
    case a3: DoubleVal => this.getValue <= a3.getValue
    case a4: LongVal => this.getValue <= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue == a1.getValue
    case a2: FloatVal => this.getValue == a2.getValue
    case a3: DoubleVal => this.getValue == a3.getValue
    case a4: LongVal => this.getValue == a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: IntVal => this.getValue != a1.getValue
    case a2: FloatVal => this.getValue != a2.getValue
    case a3: DoubleVal => this.getValue != a3.getValue
    case a4: LongVal => this.getValue != a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
}
case class DoubleVal (val _name: String, _value: Double) 
extends genericValue{

  @BeanProperty var value: Double = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":double " + value + "]"
  def copy(): DoubleVal = new DoubleVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case a1: IntVal => new DoubleVal("", this.getValue + a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue + a2.getValue)
    case a3: LongVal => new DoubleVal ("", this.getValue + a3.getValue) 
    case a4: FloatVal => new DoubleVal("", this.getValue + a4.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case a1: IntVal => new DoubleVal("", this.getValue - a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue - a2.getValue)
    case a3: LongVal => new DoubleVal ("", this.getValue - a3.getValue) 
    case a4: FloatVal => new DoubleVal("", this.getValue - a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case a1: IntVal => new DoubleVal("", this.getValue * a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue * a2.getValue)
    case a3: LongVal => new DoubleVal ("", this.getValue * a3.getValue) 
    case a4: FloatVal => new DoubleVal("", this.getValue * a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {

    case a1: IntVal => 
    {
      assert(a1.getValue !=0)
      new DoubleVal("", this.getValue / a1.getValue)
    }
    case a2: DoubleVal => 
    {
      assert(a2.getValue !=0)
      new DoubleVal("", this.getValue / a2.getValue)
    }
    case a3: LongVal => 
    {
      assert(a3.getValue !=0)
      new DoubleVal ("", this.getValue / a3.getValue) 
    }
    case a4: FloatVal => 
    {

      assert(a4.getValue !=0)
      new DoubleVal("", this.getValue / a4.getValue)
    }
    case _ => throw new EdbException("not supported op " + that)
  }

  //end arithmatic operator
  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: IntVal => this.getValue > a1.getValue
    case a2: FloatVal => this.getValue > a2.getValue
    case a3: DoubleVal => this.getValue > a3.getValue
    case a4: LongVal => this.getValue > a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
  def <(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue < a1.getValue
    case a2: FloatVal => this.getValue < a2.getValue
    case a3: DoubleVal => this.getValue < a3.getValue
    case a4: LongVal => this.getValue < a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue >= a1.getValue
    case a2: FloatVal => this.getValue >= a2.getValue
    case a3: DoubleVal => this.getValue >= a3.getValue
    case a4: LongVal => this.getValue >= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue <= a1.getValue
    case a2: FloatVal => this.getValue <= a2.getValue
    case a3: DoubleVal => this.getValue <= a3.getValue
    case a4: LongVal => this.getValue <= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue == a1.getValue
    case a2: FloatVal => this.getValue == a2.getValue
    case a3: DoubleVal => this.getValue == a3.getValue
    case a4: LongVal => this.getValue == a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: IntVal => this.getValue != a1.getValue
    case a2: FloatVal => this.getValue != a2.getValue
    case a3: DoubleVal => this.getValue != a3.getValue
    case a4: LongVal => this.getValue != a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
}

case class FloatVal (val _name: String, _value: Float)
extends genericValue {

  @BeanProperty var value: Float = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":float " + value + "]"
  def copy(): FloatVal = new FloatVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case a1: IntVal => new FloatVal("", this.getValue + a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue + a2.getValue)
    case a3: LongVal => new FloatVal ("", this.getValue + a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue + a4.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case a1: IntVal => new FloatVal("", this.getValue - a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue - a2.getValue)
    case a3: LongVal => new FloatVal ("", this.getValue - a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue - a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case a1: IntVal => new FloatVal("", this.getValue * a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue * a2.getValue)
    case a3: LongVal => new  FloatVal ("", this.getValue * a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue * a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {
    case a1: IntVal => 
    {
      assert(a1.getValue !=0)
      new FloatVal("", this.getValue / a1.getValue)
    }
    case a2: DoubleVal => 
    {
      assert(a2.getValue !=0)
      new DoubleVal("", this.getValue / a2.getValue)
    }
    case a3: LongVal => 
    {
      assert(a3.getValue !=0)
      new FloatVal ("", this.getValue / a3.getValue) 
    }
    case a4: FloatVal => 
    {
      assert(a4.getValue !=0)
      new FloatVal("", this.getValue / a4.getValue)
    }
    case _ => throw new EdbException("not supported op " + that)
  }

  //end arithmatic operator
  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: IntVal => this.getValue > a1.getValue
    case a2: FloatVal => this.getValue > a2.getValue
    case a3: DoubleVal => this.getValue > a3.getValue
    case a4: LongVal => this.getValue > a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
  def <(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue < a1.getValue
    case a2: FloatVal => this.getValue < a2.getValue
    case a3: DoubleVal => this.getValue < a3.getValue
    case a4: LongVal => this.getValue < a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue >= a1.getValue
    case a2: FloatVal => this.getValue >= a2.getValue
    case a3: DoubleVal => this.getValue >= a3.getValue
    case a4: LongVal => this.getValue >= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue <= a1.getValue
    case a2: FloatVal => this.getValue <= a2.getValue
    case a3: DoubleVal => this.getValue <= a3.getValue
    case a4: LongVal => this.getValue <= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue == a1.getValue
    case a2: FloatVal => this.getValue == a2.getValue
    case a3: DoubleVal => this.getValue == a3.getValue
    case a4: LongVal => this.getValue == a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: IntVal => this.getValue != a1.getValue
    case a2: FloatVal => this.getValue != a2.getValue
    case a3: DoubleVal => this.getValue != a3.getValue
    case a4: LongVal => this.getValue != a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
}
case class LongVal (val _name: String, _value: Long) 
extends genericValue {

  @BeanProperty var value: Long = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":long " + value + "]"
  def copy(): LongVal = new LongVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case a1: IntVal => new LongVal("", this.getValue + a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue + a2.getValue)
    case a3: LongVal => new LongVal ("", this.getValue + a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue + a4.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case a1: IntVal => new LongVal("", this.getValue - a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue - a2.getValue)
    case a3: LongVal => new LongVal ("", this.getValue - a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue - a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case a1: IntVal => new LongVal("", this.getValue * a1.getValue)
    case a2: DoubleVal => new DoubleVal("", this.getValue * a2.getValue)
    case a3: LongVal => new  LongVal ("", this.getValue * a3.getValue) 
    case a4: FloatVal => new FloatVal("", this.getValue * a4.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {

    case a1: IntVal => 
    {
      assert(a1.getValue !=0)
      new DoubleVal("", this.getValue / a1.getValue)
    }
    case a2: DoubleVal => 
    {
      assert(a2.getValue !=0)
      new DoubleVal("", this.getValue / a2.getValue)
    }
    case a3: LongVal => 
    {
      assert(a3.getValue !=0)
      new DoubleVal ("", this.getValue / a3.getValue) 
    }
    case a4: FloatVal => 
    {
      assert(a4.getValue !=0)
      new FloatVal("", this.getValue / a4.getValue)
    }
    case _ => throw new EdbException("not supported op " + that)
  }

  //end arithmatic operator
  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: IntVal => this.getValue > a1.getValue
    case a2: FloatVal => this.getValue > a2.getValue
    case a3: DoubleVal => this.getValue > a3.getValue
    case a4: LongVal => this.getValue > a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
  def <(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue < a1.getValue
    case a2: FloatVal => this.getValue < a2.getValue
    case a3: DoubleVal => this.getValue < a3.getValue
    case a4: LongVal => this.getValue < a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue >= a1.getValue
    case a2: FloatVal => this.getValue >= a2.getValue
    case a3: DoubleVal => this.getValue >= a3.getValue
    case a4: LongVal => this.getValue >= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue <= a1.getValue
    case a2: FloatVal => this.getValue <= a2.getValue
    case a3: DoubleVal => this.getValue <= a3.getValue
    case a4: LongVal => this.getValue <= a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: IntVal => this.getValue == a1.getValue
    case a2: FloatVal => this.getValue == a2.getValue
    case a3: DoubleVal => this.getValue == a3.getValue
    case a4: LongVal => this.getValue == a4.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: IntVal => this.getValue != a1.getValue
    case a2: FloatVal => this.getValue != a2.getValue
    case a3: DoubleVal => this.getValue != a3.getValue
    case a4: LongVal => this.getValue != a4.getValue
    case _ => throw new EdbException("cannot compute")
  }
}

case class BooleanVal (val _name: String, _value: Boolean) 
extends genericValue {

  @BeanProperty var value: Boolean = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":boolean " + value + "]"
  def copy(): BooleanVal = new BooleanVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }


  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case _ => throw new EdbException("cannot compute")
  }
  def <(that: genericValue): Boolean = that match {
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: BooleanVal => this.getValue == a1.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: BooleanVal => this.getValue != a1.getValue
    case _ => throw new EdbException("cannot compute")
  }
}
case class StringVal (val _name: String, _value: String) 
extends genericValue {

  @BeanProperty var value: String = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":string " + value + "]"
  def copy(): StringVal = new StringVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case a1: StringVal=> new StringVal("", this.getValue + a1.getValue)
    case a2: IntVal=> new StringVal("", this.getValue + a2.getValue)
    case a3: FloatVal=> new StringVal("", this.getValue + a3.getValue)
    case a4: DoubleVal=> new StringVal("", this.getValue + a4.getValue)
    case a5: LongVal=> new StringVal("", this.getValue + a5.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }



  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: StringVal => this.getValue.compareTo(a1.getValue) >0
    case _ => throw new EdbException("cannot compute")
  }

  def <(that: genericValue): Boolean = that match {
    case a1: StringVal => this.getValue.compareTo(a1.getValue)<0
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: StringVal => this.getValue.compareTo(a1.getValue)>=0
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: StringVal => this.getValue.compareTo(a1.getValue)<=0
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: StringVal => this.getValue.compareTo(a1.getValue) == 0
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: StringVal => this.getValue.compareTo(a1.getValue) != 0
    case _ => throw new EdbException("cannot compute")
  }
}

case class DateVal (val _name: String, _value: Long) 
extends genericValue{

  @BeanProperty var value: Long = _
  setValue(_value)
  setName(_name)
  override def toString =  "[" + name+ ":date " + value + "]"
  def copy(): DateVal = new DateVal(getName(),getValue())

  def +(that: genericValue): genericValue = that match {
    case a: DateVal => new DateVal("", this.getValue + a.getValue)
    case _ => throw new EdbException("not supported type" + that)
  }

  def -(that: genericValue): genericValue = that match {
    case a: DateVal => new DateVal("", this.getValue - a.getValue)
    case _ => throw new EdbException("not supported op " + that)
  }

  def *(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  def /(that: genericValue): genericValue = that match {
    case _ => throw new EdbException("not supported op " + that)
  }

  //start of compare operator
  def >(that: genericValue): Boolean = that match { 
    case a1: DateVal => this.getValue > a1.getValue
    case _ => throw new EdbException("cannot compute")
  }
  def <(that: genericValue): Boolean = that match {
    case a1: DateVal => this.getValue < a1.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def >=(that: genericValue): Boolean = that match {
    case a1: DateVal => this.getValue >= a1.getValue
    case _ => throw new EdbException("cannot compute")
  }

  def <=(that: genericValue): Boolean = that match {
    case a1: DateVal => this.getValue <= a1.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //eq is reserved
  def eqs(that: genericValue): Boolean = that match {
    case a1: DateVal => this.getValue == a1.getValue
    case _ => throw new EdbException("cannot compute")
  }

  //neq is probably reserved too
  def neqs(that:genericValue): Boolean = that match {
    case a1: DateVal => this.getValue != a1.getValue
    case _ => throw new EdbException("cannot compute")
  }
}

import java.util.StringTokenizer
/** 
  * schema class. default has id=0 and version=0
  * which represents a schema created on the fly 
  */
class Schema (_id: Int=0, _version: Byte=0) extends Serializable {

  /* BeanProperty add getname and setname  methods */
  /* "_" just mean default value */

  @BeanProperty var  id= _id 
  @BeanProperty var version = _version
  @BeanProperty var latestVersion: Int = 0
  @BeanProperty var numAtts: Int = 0 
  @BeanProperty var atts: Array[Attribute] = _ 
  @BeanProperty var numKeyAtts: Int = 0
  //store the key att index
  @BeanProperty var keys: Array[Int] = _

  //see hashCode recipe in programming in scala 2nd ed
  override def hashCode: Int = 
  41*(
    41*(
      41*(
        41*(
          41 *(
            41 * (
              41 + id.hashCode
            ) + version.hashCode
        ) + latestVersion.hashCode
    ) + numAtts.hashCode
) + atts.hashCode
    ) + numKeyAtts.hashCode
) + keys.hashCode


  //creating schema
  {
    val sch = Catalog.getTableSchema(id, version)
    //foud the schema from catalog
    if (!Option(sch).isEmpty) {
      var st = new StringTokenizer(sch, ";")
      val attList = st.nextToken()
      val keyList = st.nextToken()

      //validity check. attList cannot be empty
      //keyList can be empty
      if(Option(attList).isEmpty) {
        Console.err.println("attribute list from catalog is empty")
        exit(100)
      }

      //creating schema
      //1. parse attribute list
      st = new StringTokenizer(attList,",")
      this.setNumAtts(st.countTokens())
      this.setAtts(new Array[Attribute](this.getNumAtts())) 

      for (i<- 0 to this.getNumAtts()-1){
        val column = st.nextToken().trim()
        val parts: Array[String] = column.split(" ")

        this.atts(i)= parts(0).trim().toLowerCase() match{
          case "int" =>  new IntAtt(parts(1).trim())
          case "long" =>  new LongAtt(parts(1).trim())
          case "double" =>  new DoubleAtt(parts(1).trim())
          case "float" => new FloatAtt(parts(1).trim())
          case "boolean" => new BooleanAtt(parts(1).trim())
          case "string" => new StringAtt(parts(1).trim())
          case "date" => new DateAtt(parts(1).trim())
          case _ => {
            Console.err.println("Unsupported attribute type "+ parts.toString)
            exit(100)
          }
        }
      }
      //2. parse key attribute list

      val attName: Array[String] = new Array[String](this.numAtts)

      for(i <- 0 to this.numAtts-1){
        attName(i)=atts(i).getName()
      }

      if(!Option(keyList).isEmpty){ 
        st = new StringTokenizer(keyList,",")
        this.setNumKeyAtts(st.countTokens())
        this.setKeys(new Array[Int](this.getNumKeyAtts())) 

        var j = 0
        for (i<- 0 to this.getNumKeyAtts()-1){
          val keyName = st.nextToken().trim()
          val idx = attName.indexOf(keyName)
          this.keys(j) = if (idx > -1)  idx else {
            Console.err.println("cannot find key "+ keyName)
            exit(100)
          }
          j= j+1
        }

        Arrays.sort(this.keys)
      }
    }
    //sort the keys low to high
  }

  /**
    * instantiate a new schema based on the 
    * projection list of a parent schema
    */
  def setSchema(attList: Array[Int], parentSch: Schema) = {

    this.setNumAtts(attList.length)
    this.setAtts(new Array[Attribute](this.getNumAtts())) 
    for (i<- 0 to this.getNumAtts()-1){
      this.atts(i)= (parentSch.getAtts()(attList(i))).copy()
    }
  }

  override def equals (other: Any): Boolean = other.isInstanceOf[Schema] && {
    val o: Schema =  other.asInstanceOf[Schema]
    o.getId == this.getId && o.getVersion == this.getVersion && 
    o.getNumAtts == this.getNumAtts && 
    o.getNumKeyAtts == this.getNumKeyAtts && 
    o.getLatestVersion == this.getLatestVersion &&
    o.getAtts.corresponds(this.getAtts) {_ == _}
  }

  override def toString = {
    val str1 = Catalog.getTableName(id)+ "(" 
    var str2 = ""
    for (att <-atts) {
      str2 += att.toString + ","
    }

    //get rid of additional comma
    str2 = str2.substring(0, str2.length-1)

    var str3 =""

    if(!Option(keys).isEmpty){ 
      for(i<-keys){
        str3 += atts(i).getName()+ ","
      }
      //get rid of additional comma
      str3 = str3.substring(0, str3.length-1)
    }

    str1+ str2 +";" + str3 + ")"
  }

  def copy(): Schema = {

    val sch = new Schema()
    sch.setId(this.getId)
    sch.setVersion(this.getVersion)
    sch.setLatestVersion(this.getLatestVersion)
    sch.setNumAtts(this.getNumAtts)
    //clone att array
    sch.setAtts(new Array[Attribute](this.getNumAtts())) 
    for (i<- 0 to this.getNumAtts()-1){
      sch.atts(i)= (this.getAtts()(i)).copy()
    }
    sch.setNumKeyAtts(this.getNumKeyAtts)

    //clone keys array
    sch.setKeys(new Array[Int](this.getNumKeyAtts))
    for (i<- 0 to this.getNumKeyAtts-1){
      sch.getKeys(i)= this.getKeys()(i)
    }
    sch
  }
}
