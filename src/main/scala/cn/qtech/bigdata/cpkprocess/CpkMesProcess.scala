package cn.qtech.bigdata.cpkprocess

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, UUID}
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import cn.qtech.bigdata.common.filterData.newWork_code
import cn.qtech.bigdata.timeutils.TimeUtils
import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object CpkMesProcess {

  // 匹配浮点数
  private final val res = "[-+]?[0-9]*\\.?[0-9]+"
  private final val pattern = Pattern.compile(res)

  private final val res1 = ".*[1-9].*"
  private final val pattern2 = Pattern.compile(res1)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    //val kuduContext = new KuduContext(kuduMaster,sc)

    val tableName = "ADS_INSPECTIONPASSRATE"

    //val thetime = sparkConf.get("spark.thetime.field")
    val theTime = "WEEKTIME"
    //    val theTime = "MONTHTIME"
    //        val theTime = "QUARTERTIME"

    val dateTime = "WEEK"
    //    val dateTime = "MONTH"
    //        val dateTime = "QUARTER"

    val insertTable = "ADS_CPKWEEKLISTBYMES"
    //    val insertTable = "ADS_CPKMONTHLISTBYMES"
    //        val insertTable = "ADS_CPKQUARTERLISTBYMES"

    CpkCalculate(sparkSession, sc, kuduMaster, tableName, insertTable, theTime, dateTime)

    sc.stop()
  }

  private def CpkCalculate(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, tableName: String,
                           insertTable: String, theTime: String, dateTime: String): Unit = {

    var result = new ListBuffer[String]()

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

    // 获取计算结果表里 周、月、季 最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(${theTime}) from getresulttemp").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    // 获取源表最小和最大的时间
    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from temp1").collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes {
      0
    }
//    var sourcetimemax = sourcetimes {1}
    var sourcetimemax = "2021-11-15 00:00:00"
//   var timemin = sourcetimemin
    var timemin = "2020-06-30 00:03:09"

    if (kudutimes != "null" && kudutimes.contains("至")) {
      timemin = kudutimes.split("至") {1}

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var sDate = sdf.parse(timemin)

      // timemin的值理论上应该是周日，把周日加上一天，即周一作为起始日期
      val calendar = Calendar.getInstance()
      calendar.setTime(sDate);
      calendar.add(Calendar.DATE, 1) // 把日期往后增加一天,整数

      timemin = sdf.format(calendar.getTime())

      //本地测试最小时间
      timemin = "2021-10-04 00:00:00"
    }

    // 按周、月、季维度划分时间
    var timeMap: util.Map[String, String] = null
    if (theTime == "WEEKTIME") {
      timeMap = TimeUtils.getWeeks(timemin, sourcetimemax)
    } else if (theTime == "MONTHTIME") {
      timeMap = TimeUtils.getMonths(timemin, sourcetimemax)
    } else if (theTime == "QUARTERTIME") {
      timeMap = TimeUtils.getQuarter(timemin, sourcetimemax)
    }

    var date = ""
    var yeartime = ""

    for ((timesmin, timesmax) <- timeMap) {
      var timefield = timesmin + "至" + timesmax
      if (theTime == "WEEKTIME") {
        val week = sparkSession.sql(s"select weekofyear('${timesmax}')").collect()
          .toList.toString.substring(6).split("]") {
          0
        }
        yeartime = timesmin.split("\\-") {
          0
        }
        date = "WEEK" + week
      } else if (theTime == "MONTHTIME") {
        yeartime = timesmax.split("\\-") {
          0
        }
        date = timesmax.split("\\-") {
          0
        } + "-" + timesmax.split("\\-") {
          1
        }
      } else if (theTime == "QUARTERTIME") {
        yeartime = timesmax.split("\\-") {
          0
        }
        val getmonth = timesmax.split("\\-") {
          1
        }.toInt
        date = TimeUtils.quarterOfYear(getmonth)
      }

//      timesmin = ''


      sparkSession.sql("SELECT * FROM temp1")
        .where(s"MDATE>='${timesmin}' and MDATE<='${timesmax}' and FIRST_TIME='巡检'")
//                .where(s"MDATE >= '2020-10-01 00:00:00' and MDATE < '2020-11-15 00:00:00' and FIRST_TIME='巡检'")
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
          |a.FACTORY
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

      val dataList = groupData.collect().toIterator
      val list: List[Row] = groupData.collect().toList
      val integer = new AtomicInteger(0)

      // 创建List保存分组数据
      var tDatasList = new ListBuffer[Float]()
      var groupDataList = new ListBuffer[String]()

      val last: Row = list.last;

      //      println(last)
      // 遍历并查询数据
      while (dataList.hasNext) {
        val rowData = dataList.next()
        //        println(rowData.toString())
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
        //        if (integer.get() > numField.toInt) {
        if (integer.get() == numField.toInt && !last.toString().equalsIgnoreCase(dataStr)) {

          // 不断把分组数据加入到集合中
          groupDataList += firstTimeField + ";" + workShopCodeField + ";" + partSpecField + ";" + opCodeField + ";" + staCodeField +
            ";" + eattribute1_1Field + ";" + testItemField + ";" + factoryField + ";" + uslfield + ";" + lslfield

          // 将tdata中的数值不断加入到集合中
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
        } else if (integer.get() > numField.toInt || last.toString().equalsIgnoreCase(dataStr)) {
          if (last.toString().equalsIgnoreCase(dataStr)) {
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
          }
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

                  insertData(kuduMaster, insertTable, id, theTime, dateTime, s"${timefield}", yeartime,
                    s"${date}", s"${firstTime}", s"${workShopCode}",
                    s"${partSpec}", s"${opCode}", s"${staCode}",
                    s"${eattribute1_1}", s"${testItem}", s"${factory}",
                    cpu, cpl, cpl, "1.000")
                } else if (cpu.toFloat < cpl.toFloat) {


                  insertData(kuduMaster, insertTable, id, theTime, dateTime, s"${timefield}",
                    yeartime, s"${date}", s"${firstTime}", s"${workShopCode}",
                    s"${partSpec}", s"${opCode}", s"${staCode}",
                    s"${eattribute1_1}", s"${testItem}", s"${factory}",
                    cpu, cpl, cpu, "1.000")
                }
              }
            } else if (usl != "null" & lsl == "null") {
              val uslavgdata = usl.toDouble
              val valueavgdata = avgTestValue.toDouble
              val stddevdata = stdevTestValue.toDouble
              val uslcpk = (uslavgdata - valueavgdata).abs / 3 / stddevdata
              val cpu = uslcpk.formatted("%.3f")

              insertData(kuduMaster, insertTable, id, theTime, dateTime, s"${timefield}", yeartime,
                s"${date}", s"${firstTime}", s"${workShopCode}",
                s"${partSpec}", s"${opCode}", s"${staCode}",
                s"${eattribute1_1}", s"${testItem}", s"${factory}",
                cpu, "", cpu, "1.000")

            } else if (usl == "null" & lsl != "null") {
              val lslavgdata = lsl.toDouble
              val valueavgdata = avgTestValue.toDouble
              val stddevdata = stdevTestValue.toDouble
              val lslcpk = (valueavgdata - lslavgdata).abs / 3 / stddevdata
              val cpl = lslcpk.formatted("%.3f")

              insertData(kuduMaster, insertTable, id, theTime, dateTime, s"${timefield}", yeartime,
                s"${date}", s"${firstTime}", s"${workShopCode}",
                s"${partSpec}", s"${opCode}", s"${staCode}",
                s"${eattribute1_1}", s"${testItem}", s"${factory}",
                "", cpl, cpl, "1.000")

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

    }
  }

  case class BasicFeatures(id: String, theTime: String, dateTime: String, mDate: String,
                           year: String, date: String, firstTime: String, workShopCode: String, partSpec: String, opCode: String,
                           staCode: String, eattribute1_1: String, testItem: String, factory: String, cpuValue: String,
                           cplValue: String, cpkValue: String, cpkWarnLine: String)

  def insertData(kuduMaster: String, insertTable: String, id: String, theTime: String, dateTime: String, mDate: String,
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
    rowUpsert.addString(s"${theTime}", s"${mDate}")
    rowUpsert.addString("YEAR", s"${year}")
    rowUpsert.addString(s"${dateTime}", s"${date}")
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


}


