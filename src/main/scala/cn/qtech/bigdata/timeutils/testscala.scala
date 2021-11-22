package cn.qtech.bigdata.timeutils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ListBuffer

object testscala {

  def main(args: Array[String]): Unit = {
    var result = new ListBuffer[String]()

    result += "123"+","+"456"+","+""+","+"789"

    for (s <- result){
      val data: Array[String] = s.split(",")

      print(data(0),data(1),data(2),data(3))


    }
  }

  def startTime(date: String): String = {
    var str: String = ""
    if (date.isEmpty || date.contains("NULL") || date.contains("null")) {
      str = "2020-08-01 00:00:00"

    } else {
      val newtime: Date = new SimpleDateFormat("yyyy-MM-dd HH").parse(date)
      var cal = Calendar.getInstance()
      cal.setTime(newtime)
      cal.add(Calendar.MONTH, 1)
      cal.set(Calendar.DATE, 1)
      str = new SimpleDateFormat("yyyy-MM-dd HH").format(cal.getTime)
      //      print()
    }

    str+":00:00"
  }

  def endtime(date: String): String = {
    var cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, 1)
    cal.set(Calendar.DATE, 0)
    val targetTime: String = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    targetTime +" 00:00:00"
  }

}
