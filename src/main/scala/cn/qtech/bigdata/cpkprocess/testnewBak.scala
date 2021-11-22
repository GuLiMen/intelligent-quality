package cn.qtech.bigdata.cpkprocess

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import java.util.{Calendar, Date, UUID}

import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object testnewBak {

  // 匹配浮点数
  private final val res = "[-+]?[0-9]*\\.?[0-9]+"
  private final val pattern = Pattern.compile(res)

  private final val res1 = ".*[1-9].*"
  private final val pattern2 = Pattern.compile(res1)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
      //      .set("spark.executor.heartbeatInterval","10000s")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"

    val tableName = "ADS_INSPECTIONPASSRATE"

    val theTime = "MONTHTIME"
    val dateTime = "MONTH"
    val insertTable = "ADS_CPKMONTHLISTBYMES"

    CpkCalculate(sparkSession, sc, kuduMaster, tableName, insertTable)

    sc.stop()
  }

  private def CpkCalculate(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, tableName: String,
                           insertTable: String): Unit = {
    val option1 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    val optionsResult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> insertTable
    )

    sparkSession.read.options(option1).format("kudu").load.createOrReplaceTempView("temp1")
    sparkSession.read.options(optionsResult).format("kudu").load
      .createOrReplaceTempView("getresulttemp")

    val str: String = sparkSession.sql("select max(monthtime)  from getresulttemp").collect().toList.toString()

    val createTime: String = startTime(str)

    sparkSession.sql("SELECT * FROM temp1")
      .where(s"MDATE>'${createTime}' and FIRST_TIME='巡检'")
      //      .where(s"MDATE >= '2021-01-01 00:00:00' and MDATE <= '2021-12-31 00:00:00' and FIRST_TIME='巡检'")
      .createOrReplaceTempView("jointemp")

    // 分组查询
    val groupData = sparkSession.sql(
      """
        |select * from (
        |select a.lower,a.upper,a.tdata,a.rcard,a.id_no,a.mdate,a.FIRST_TIME,a.WORKSHOP_CODE,
        |a.PART_SPEC,a.OP_CODE,a.STA_CODE,a.eattribute1_1,a.testitem, a.FACTORY,
        |row_number() over(partition by a.FIRST_TIME,
        |a.WORKSHOP_CODE,
        |a.PART_SPEC,
        |a.OP_CODE,
        |a.STA_CODE,
        |a.eattribute1_1,
        |a.testitem,
        |a.FACTORY,substr(a.mdate,1,7)
        |order by a.mdate desc) as num
        |from jointemp a
        |)y where tdata like '%1%' or
        |tdata like '%2%' or
        |tdata like '%3%' or
        |tdata like '%4%' or
        |tdata like '%5%' or
        |tdata like '%6%' or
        |tdata like '%7%' or
        |tdata like '%8%' or
        |tdata like '%9%'
      """.stripMargin)

    val resultList = new ListBuffer[String]()

    val dataList = groupData.collect().toIterator

    val integer = new AtomicInteger(0)

    // 创建List保存分组数据
    var tDatasList = new ListBuffer[Float]()
    var groupDataList = new ListBuffer[String]()

    // 遍历并查询数据
    while (dataList.hasNext) {
      val rowData = dataList.next()
      val dataStr: String = rowData.toString()
      val transfData: String = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = transfData.split("\\,")

      // 上下限
      val lslfield = splitArr {
        0
      }
      val uslfield = splitArr {
        1
      }

      val tdataField = rowData.getAs[String]("tdata")
      val numField = rowData.getAs[Int]("num")
      val mdate = rowData.getAs[String]("mdate")
      // 分组数据
      val firstTimeField = rowData.getAs[String]("FIRST_TIME")
      val workShopCodeField = rowData.getAs[String]("WORKSHOP_CODE")
      val partSpecField = rowData.getAs[String]("PART_SPEC")
      val opCodeField = rowData.getAs[String]("OP_CODE")
      val staCodeField = rowData.getAs[String]("STA_CODE")
      val eattribute1_1Field = rowData.getAs[String]("eattribute1_1")
      val testItemField = rowData.getAs[String]("testitem")
      val factoryField = rowData.getAs[String]("FACTORY")

      // 自增加1
      integer.getAndIncrement()
      if (integer.get() == numField.toInt) {
        // 不断把分组数据加入到集合中
        groupDataList += firstTimeField + ";" + workShopCodeField + ";" + partSpecField + ";" + opCodeField + ";" + staCodeField +
          ";" + eattribute1_1Field + ";" + testItemField + ";" + factoryField + ";" + uslfield + ";" + lslfield

        // 将tdata中的数值不断加入到集合中
        if (tdataField.contains(";")) {
          val tdatas = tdataField.split(";").toIterator
          while (tdatas.hasNext) {
            // 将数据加入到集合中
            val tdataDatas = tdatas.next()
            if (pattern.matcher(s"$tdataDatas").matches()) {
              tDatasList += tdataDatas.toFloat
            }
          }
        } else if (pattern.matcher(s"$tdataField").matches()) {
          // 如果tdata匹配的值为浮点数，直接将数据插入到集合中
          tDatasList += tdataField.toFloat
        }

      } else if (integer.get() > numField.toInt) {

        // 不断把分组数据加入到集合中
        groupDataList += firstTimeField + ";" + workShopCodeField + ";" + partSpecField + ";" + opCodeField + ";" + staCodeField +
          ";" + eattribute1_1Field + ";" + testItemField + ";" + factoryField + ";" + uslfield + ";" + lslfield
        if (tdataField.contains(";")) {
          //              println(tdataField)
          val tdatas = tdataField.split(";").toIterator
          while (tdatas.hasNext) {
            // 将数据加入到集合中
            val tdataDatas = tdatas.next()
            if (pattern.matcher(s"$tdataDatas").matches()) {
              tDatasList += tdataDatas.toFloat
            }
          }
        } else if (pattern.matcher(s"$tdataField").matches()) {
          // 如果tdata匹配的值为浮点数，直接将数据插入到集合中
          tDatasList += tdataField.toFloat
        }
        //          }
        // 不断把分组数据加入到集合中

        //主键id
        var uuid = UUID.randomUUID
        var id = uuid.toString

        // 获取pcs的个数
        val count = tDatasList.size
        // 获取testValue的平均值
        val avgTestValue = tDatasList.sum / count
        // 获取testValue的标准差
        val stdevTestValue = sc.parallelize(tDatasList).stdev()
        // 获取第1条分组条件
        val groupDataOne = groupDataList.subList(0, 1)
        val groupDataStr = groupDataOne.toString
        val groupDatasArr = groupDataStr.substring(1).split("\\;")

        val firstTime = groupDatasArr {
          0
        }
        val workShopCode = groupDatasArr {
          1
        }
        val partSpec = groupDatasArr {
          2
        }
        val opCode = groupDatasArr {
          3
        }
        val staCode = groupDatasArr {
          4
        }
        val eattribute1_1 = groupDatasArr {
          5
        }
        val testItem = groupDatasArr {
          6
        }
        val factory = groupDatasArr {
          7
        }
        // 规格上限
        val usl = groupDatasArr {
          8
        }
        // 规格下限
        val lsl = groupDatasArr {
          9
        }.substring(0, groupDatasArr {
          9
        }.length - 1)

        // pcs需大于等于32
        if (count >= 32) {
          //            print("test")
          if (usl != "null" && lsl != "null" && lsl != "" && usl != "") {
            val uslavgdata = usl.toDouble
            val lslavgdata = lsl.toDouble
            if (uslavgdata > lslavgdata) {

              print(uslavgdata + "==" + lslavgdata)
              val valueavgdata = avgTestValue.toDouble
              val stddevdata = stdevTestValue.toDouble

              val uslcpk = (uslavgdata - valueavgdata).abs / 3 / stddevdata
              val lslcpk = (valueavgdata - lslavgdata).abs / 3 / stddevdata
              val cpu = uslcpk.formatted("%.3f")

              val cpl = lslcpk.formatted("%.3f")

              if (cpu.toFloat >= cpl.toFloat) {

                resultList += id + "," + dateYear(mdate) + "," + Year(mdate) + "," + yearMonth(mdate) + "," + firstTime + "," + workShopCode + "," + partSpec + "," + opCode + "," + staCode + "," + eattribute1_1 + "," + testItem + "," + factory + "," + cpu + "," + cpl + "," + cpl + "," + "1.000"


              } else if (cpu.toFloat < cpl.toFloat) {

                resultList += id + "," + dateYear(mdate) + "," + Year(mdate) + "," + yearMonth(mdate) + "," + firstTime + "," + workShopCode + "," + partSpec + "," + opCode + "," + staCode + "," + eattribute1_1 + "," + testItem + "," + factory + "," +
                  cpu + "," + cpl + "," + cpu + "," + "1.000"

              }
            }
          } else if (usl != "null" & lsl == "null") {
            val uslavgdata = usl.toDouble
            val valueavgdata = avgTestValue.toDouble
            val stddevdata = stdevTestValue.toDouble
            val uslcpk = (uslavgdata - valueavgdata).abs / 3 / stddevdata
            val cpu = uslcpk.formatted("%.3f")

            resultList += id + "," + dateYear(mdate) + "," + Year(mdate) + "," + yearMonth(mdate) + "," + firstTime + "," + workShopCode + "," + partSpec + "," + opCode + "," + staCode + "," +
              eattribute1_1 + "," + testItem + "," + factory + "," +
              cpu + "," + "" + "," + cpu + "," + "1.000"

          } else if (usl == "null" & lsl != "null") {
            val lslavgdata = lsl.toDouble
            val valueavgdata = avgTestValue.toDouble
            val stddevdata = stdevTestValue.toDouble
            val lslcpk = (valueavgdata - lslavgdata).abs / 3 / stddevdata
            val cpl = lslcpk.formatted("%.3f")
            resultList += id + "," + dateYear(mdate) + "," + Year(mdate) + "," + yearMonth(mdate) + "," + firstTime + "," + workShopCode + "," +
              partSpec + "," + opCode + "," + staCode + "," +
              eattribute1_1 + "," + testItem + "," + factory + "," +
              "" + "," + cpl + "," + cpl + "," + "1.000"
          }

        }

        println("一个分组结束，触发计算")
        // 清空当前集合
        groupDataList.clear()
        tDatasList.clear()

        // 将自增数为1时的数据插入到集合中
        if (tdataField.contains(";")) {
          val tdatas = tdataField.split(";").toIterator
          while (tdatas.hasNext) {
            // 将数据加入到集合中
            val tdataDatas = tdatas.next()
            if (pattern.matcher(s"$tdataDatas").matches()) {
              tDatasList += tdataDatas.toFloat
            }
          }
        } else if (pattern.matcher(s"$tdataField").matches()) {
          // 如果tdata匹配的值为浮点数，直接将数据插入到集合中
          tDatasList += tdataField.toFloat
        }

        // 将自增数为1时的分组数据加入到集合中

        groupDataList += firstTimeField + ";" + workShopCodeField + ";" + partSpecField + ";" + opCodeField + ";" + staCodeField +
          ";" + eattribute1_1Field + ";" + testItemField + ";" + factoryField + ";" + uslfield + ";" + lslfield

        // 将数字初始值置为1
        integer.set(1)
      }

    }

    val resultRDD: RDD[String] = sc.makeRDD(resultList)

    val resRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = resultRDD.map(res => {
      val s: Array[String] = res.split(",")

      (s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13), s(14), s(15))
    })
    val sql = sparkSession.sqlContext
    import sql.implicits._
    resRDD.toDF().createOrReplaceTempView("resultDF")

    sparkSession.sql("insert into getresulttemp select * from resultDF")

  }


  def insertData(kuduMaster: String, insertTable: String, id: String, mDate: String,
                 year: String, date: String, firstTime: String, workShopCode: String, partSpec: String, opCode: String,
                 staCode: String, eattribute1_1: String, testItem: String, factory: String, cpuValue: String,
                 cplValue: String, cpkValue: String, cpkWarnLine: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(insertTable)

    val session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val insert = kuduTable.newInsert()
    val rowUpsert = insert.getRow()

    rowUpsert.addString("ID", id)
    rowUpsert.addString("MONTHTIME", s"${mDate}")
    rowUpsert.addString("YEAR", s"${year}")
    rowUpsert.addString("MONTH", s"${date}")
    rowUpsert.addString("FIRST_TIME", s"${firstTime}")
    rowUpsert.addString("WORKSHOP_CODE", s"${workShopCode}")
    rowUpsert.addString("PART_SPEC", s"${partSpec}")
    rowUpsert.addString("OP_CODE", s"${opCode}")
    rowUpsert.addString("STA_CODE", s"${staCode}")
    rowUpsert.addString("EATTRIBUTE1_1", s"${eattribute1_1}")
    rowUpsert.addString("TESTITEM", s"${testItem}")
    rowUpsert.addString("FACTORY", s"${factory}")
    rowUpsert.addString("CPUVALUE", s"${cpuValue}")
    rowUpsert.addString("CPLVALUE", s"${cplValue}")
    rowUpsert.addString("CPKVALUE", s"${cpkValue}")
    rowUpsert.addString("CPKWARNLINE", s"${cpkWarnLine}")

    // 执行upsert操作
    session.apply(insert)

    session.flush()
    session.close()
    kuduClient.close()

  }


  def dateYear(date: String): String = {
    val newtime: Date = new SimpleDateFormat("yyyy-MM-dd HH").parse(date)
    var cal = Calendar.getInstance()
    cal.setTime(newtime)
    cal.add(Calendar.MONTH, 1)
    cal.set(Calendar.DATE, 0)
    val sourceTime: String = new SimpleDateFormat("yyyy-MM").format(newtime)

    val targetTime: String = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val timeslot = sourceTime.toString + "-01" + "至" + targetTime
    timeslot
  }

  def Year(date: String): String = {
    val newtime: Date = new SimpleDateFormat("yyyy").parse(date)
    var cal = Calendar.getInstance()
    val sourceTime: String = new SimpleDateFormat("yyyy").format(newtime)

    val timeslot = sourceTime.toString
    timeslot
  }


  def yearMonth(date: String): String = {
    val newtime: Date = new SimpleDateFormat("yyyy-MM").parse(date)
    var cal = Calendar.getInstance()
    val sourceTime: String = new SimpleDateFormat("yyyy-MM").format(newtime)

    val timeslot = sourceTime.toString
    timeslot
  }

  def startTime(date: String): String = {
    var str: String = ""
    if (date.isEmpty || date.contains("NULL") || date.contains("null")) {
      str = "2020-08-01 00:00:00"

    } else {
      str = date.substring(11, 21) + " 00:00:00"
    }

    str
  }


}


