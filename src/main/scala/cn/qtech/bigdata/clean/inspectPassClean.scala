package cn.qtech.bigdata

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.UUID
import java.util.regex.Pattern

import cn.qtech.bigdata.common.filterData.newWork_code
import org.apache.spark.rdd.RDD
object inspectPassClean {
  private final val res1 = ".*[1-9].*"
  private final val pattern2 = Pattern.compile(res1)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu").set("spark.debug.maxToStringFields", "100").set("spark.port.maxRetries","500")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).config("spark.sql.crossJoin.enabled","true").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "10.170.3.11,10.170.3.12,10.170.3.13"

    val tableName1 = "MESC_B_FIRST_ITEM_TATA"
    val tableName2 = "MESC_B_FIRST2HM"

    val resultTable = "ADS_INSPECTIONPASSRATE"

    processData(sparkSession, kuduMaster, tableName1, tableName2, resultTable)

    sc.stop()
  }

  private def processData(sparkSession: SparkSession,  kuduMaster: String, tableName1: String,
                          tableName2: String, resultTable:String): Unit = {
    val sql = sparkSession.sqlContext
    import sql.implicits._
    val option1 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName1
    )
    val option2 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName2
    )
    val optionResult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> resultTable
    )

    sparkSession.read.options(option1).format("kudu").load
      .createOrReplaceTempView("temp1notfiltertime")
    sparkSession.read.options(option2).format("kudu").load
      .createOrReplaceTempView("temp2")
    sparkSession.read.options(optionResult).format("kudu").load
      .createOrReplaceTempView("resultTable")

    // 获取kudu中结果表最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(mdate) from resultTable").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from temp1notfiltertime").collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes{0}
    var sourcetimemax = sourcetimes{1}
    var timemin = "2020-06-30 00:00:00"

    if (kudutimes != "null") {
      timemin = kudutimes
    }

    sparkSession.sql("select * from temp1notfiltertime")
      .where(s"mdate>'${timemin}' and mdate<='${sourcetimemax}'")
//      .where(s"mdate>'2021-06-30 00:00:00' and mdate<='2021-10-14 00:00:00'")
      .createOrReplaceTempView("temp1")

    // 对巡检合格率相关指标所在的表进行关联
    val datas = sparkSession.sql("SELECT WORKSHOP_CODE,temp1.MDATE,FIRST_TIME,RCARD,PART_SPEC,PIXEL,OP_CODE,STA_CODE,TESTITEM,temp1.eattribute1 as " +
      "eattribute1_1,temp1.RESULT,ID_NO,LOWER,UPPER,TDATA FROM temp1 JOIN temp2 ON temp2.ID_NO=temp1.ID")

    // 遍历数据
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = datas.rdd.repartition(8).map(item => {
      val dataStr = item.toString()
      val dataSubStr = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = dataSubStr.split("\\,")

      val workshopcodefield = splitArr {0};
      val mdatefield = splitArr {1};
      val firsttimefield = splitArr {2};
      val rcardfield = splitArr {3};
      val partspecfield = splitArr {4};
      val pixelfield = splitArr {5};
      val opcodefield = splitArr {6};
      val stacodefield = splitArr {7};
      val testitemfield = splitArr {8};
      val eattribute1_1field = splitArr {9};
      val resultfield = splitArr {10};
      val idnofield = splitArr {11};
      val lowerfield = splitArr {12};
      val upperfield = splitArr {13};
      val tdatafield = splitArr {14}.substring(0, splitArr {14}.length - 1);

      var uuid = UUID.randomUUID
      var id = uuid.toString

      // 处理厂区字段
      var factoryfield = ""
      val tuple: (String, String) = newWork_code(factoryfield, workshopcodefield)
      var factoryfieldnew = tuple._1
      var workcodeNew = tuple._2

      (id, workcodeNew, mdatefield, firsttimefield, rcardfield, partspecfield, pixelfield, opcodefield, stacodefield, testitemfield, eattribute1_1field, resultfield, idnofield, lowerfield, upperfield, tdatafield, factoryfieldnew)
    })

    //RDD转换DF
    resultRDD.toDF().createOrReplaceTempView("tmp1")

    sparkSession.sql(s"insert into resultTable select * from tmp1")

  }
}
