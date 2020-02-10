// Databricks notebook source
// val, var
val variable1: String = "hello world!"

// COMMAND ----------

var variable1_var : String = "shivani"
variable1_var


// COMMAND ----------

var value1 = "hello"

// COMMAND ----------

val variable1 = "hey"
variable1 = variable1 + "world!"

// COMMAND ----------

var value2 = "hey"
value2 = "hey" + "world!"

// COMMAND ----------

val var_byte : Byte = 126
val val_int : Int = 2

// COMMAND ----------

print(f"hello,$val_int")

// COMMAND ----------

//switch stmt
var n : Int = 3
n match{
  case 1 => print("1")
  case 2 => print("2")
  case 3 => print("Holla found value 3")
  case _ => print("N")
}

// COMMAND ----------

for(i<-1 to 4){
  println(i)
}

// COMMAND ----------

var wh = 4
while(wh < 4)
{
  print(f"wh,$wh")
  wh = wh-1
}

// COMMAND ----------

def noDivisor(x : Int, y : Int): Float = {
  //dividing the numbers
  x/y
}
//calling the function
noDivisor(23,5)

// COMMAND ----------

def prime(x:Int): Unit = {
  for(i<- 1 to 10){
    println(x*i)
  }
}
prime(2)

// COMMAND ----------

def third(y:Int, func : Int => Unit): Unit ={
  func(y)
}
third(4, prime)

// COMMAND ----------

def parameter(x:Double, y:Double): Double = {
  x+y
}
parameter(2.5,3.4)

// COMMAND ----------

def addNumber(a:Double,b:Double,func : (Double, Double) => Double): Double = {
  func(a,b)
}
addNumber(2.3,4.5,parameter)

// COMMAND ----------

val tup = ("hello","goyal",4)
tup._2

// COMMAND ----------

var list1 = List("tushar","goyal")
list1.head

// COMMAND ----------

var dict = 1 -> "hello"
dict._2

// COMMAND ----------

var list1 = List(1,2,3)
var list2 = List(4,5)
list1 ++ list2

// COMMAND ----------

var newlist = List(1,2,3,4,5)
newlist.filter((x:Int) => x!=3 )

// COMMAND ----------

var liststring = List("hey","hello")
liststring.map((x:String) => x.length )

// COMMAND ----------

var newlist = List(1,2,3,4,5)
newlist.reduce((x:Int, y:Int) => x+y)

// COMMAND ----------

var list6 = List("hey","tushar","class","is","boring")
list6.map((x:String) => (x,1))
