package cn.qtech.bigdata.timeutils

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries","500")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val kuduContext = new KuduContext(kuduMaster, sc)
//ADS_SPCMESRESULTLIST
    val insertTable = "ADS_GLODGYL2_T"

    val inserttable = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> insertTable
    )

    sparkSession.read.options(inserttable).format("kudu").load().show()
//      .createOrReplaceTempView("inserttable")

  }

}
