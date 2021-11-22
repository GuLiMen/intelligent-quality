package cn.qtech.bigdata.reversecontrol

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

object SpcCheckLimitForHuaWei {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
    //.setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val kuduContext = new KuduContext(kuduMaster, sc)

    val spccontrollimit = "ADS_SPCCONTROLLIMITFORHUAWEI"
    val spcresultlist = "ADS_SPCREVERSECONTROL"

    SpcCalculate(sparkSession: SparkSession, sc:
      SparkContext, kuduMaster: String, spccontrollimit: String, spcresultlist: String)

    sc.stop()
  }

  private def SpcCalculate (sparkSession: SparkSession, sc: SparkContext,
                            kuduMaster: String, spccontrollimit: String, spcresultlist: String): Unit = {
    val optioncl = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spccontrollimit
    )
    val optionresult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcresultlist
    )

    sparkSession.read.options(optioncl).format("kudu").load.createOrReplaceTempView("tempcl")
    sparkSession.read.options(optionresult).format("kudu").load
      .where("datasource='HUAWEI'")
      .createOrReplaceTempView("tempresult")

    // 使用spc结果表，分组后根据时间取每组最新的25条数据(25个点)
    val groupdata = sparkSession.sql(
      """
        |    select
        |    row_number() over(partition by a.FIRST_TIME,
        |    a.WORKSHOP_CODE,
        |    a.PART_SPEC,
        |    a.OP_CODE,
        |    a.STA_CODE,
        |    a.EATTRIBUTE1_1,
        |    a.TESTITEM,
        |    a.FACTORY
        |    order by a.TIME desc) as num,
        |    a.FIRST_TIME,a.WORKSHOP_CODE,a.PART_SPEC,
        |    a.OP_CODE,a.STA_CODE,a.EATTRIBUTE1_1,
        |    a.TESTITEM, a.FACTORY,
        |    a.SAMPLENUM,
        |    a.TESTVALUEAVG ,a.TESTVALUERANGE ,a.TESTVALUESTDEV ,a.SPCWARN ,a.USL,a.LSL,
        |    a.TIME
        |    from tempresult a
        |
      """.stripMargin)

    val datalist = groupdata.collect().toIterator

    val integer = new AtomicInteger(0)

    // 创建List保存分组数据
    var groupDataList = new ListBuffer[String]()

    // 遍历并查询数据
    while (datalist.hasNext) {
      val rowData = datalist.next()
      val datastr: String = rowData.toString()
      val transfdata: String = datastr.substring(1, datastr.length - 1)
      val splitArr: Array[String] = transfdata.split("\\,")

      val numfield = splitArr {0};

      val firstTime = rowData.getAs[String]("FIRST_TIME");
      val workShopCode= rowData.getAs[String]("WORKSHOP_CODE");
      val partSpec = rowData.getAs[String]("PART_SPEC");
      val opCode = rowData.getAs[String]("OP_CODE");
      val staCode = rowData.getAs[String]("STA_CODE");
      val eattribute1_1 = rowData.getAs[String]("EATTRIBUTE1_1");
      val testItem = rowData.getAs[String]("TESTITEM");
      val factory = rowData.getAs[String]("FACTORY");
      val samplenum = rowData.getAs[Int]("SAMPLENUM").toString;
      val avgvalue = rowData.getAs[String]("TESTVALUEAVG");
      val rangevalue = rowData.getAs[String]("TESTVALUERANGE");
      val stddevvalue = rowData.getAs[String]("TESTVALUESTDEV");
      val spcwarn = rowData.getAs[String]("SPCWARN");
      val usl = rowData.getAs[String]("USL");
      val lsl = rowData.getAs[String]("LSL");
      val mdate = rowData.getAs[String]("TIME");

      val transfData: String = firstTime+";"+workShopCode+";"+partSpec+";"+opCode+";"+staCode+";"+
        eattribute1_1+";"+ testItem+";"+factory+";"+samplenum+";"+avgvalue+";"+rangevalue+";"+stddevvalue+
        ";"+spcwarn+ ";"+usl+";"+ lsl+";"+ mdate

      // 自增加1
      integer.getAndIncrement()

      if (integer.get() == numfield.toInt) {

        // 不断把数据加入到集合中
        groupDataList += transfData

      } else if (integer.get() > numfield.toInt) {

        if (groupDataList.size >= 25){

          // 保存非告警的数量
          var spcNonWarnList = new ListBuffer[Int]()
          // 保存单值或均值
          var avgValueList = new ListBuffer[Float]()
          // 保存极差值
          var rangeValueList = new ListBuffer[Float]()
          // 保存标准差值
          var stdevValueList = new ListBuffer[Float]()

          // 当同一个分组的数据都加入集合后，遍历集合
          groupDataList.subList(0, 25).foreach (item => {

            // 遍历获取到一个点的数据
            val pointDatasStr = item.toString
            var pointArrs = pointDatasStr.split("\\;")
            val avgValue= pointArrs {9};
            val rangeValue = pointArrs {10};
            val stdevValue = pointArrs {11};
            val spcWarnNum = pointArrs {12};

            // 判断25个点是否失控(有告警)
            if (spcWarnNum == "") {
              // 累加非告警数量
              spcNonWarnList += 1
              // 累加单值或均值
              if (avgValue != "") {
                avgValueList += avgValue.toFloat
              }
              // 累加极差值
              if (rangeValue != "") {
                rangeValueList += rangeValue.toFloat
              }
              // 累加标准差值
              if (stdevValue != "") {
                stdevValueList += stdevValue.toFloat
              }

            }

          })

          // 获取第1个点的分组数据
          val groupDataFirst = groupDataList.subList(0, 1)
          val groupDataFirstStr = groupDataFirst.toString
          val groupDatasFirstArr = groupDataFirstStr.substring(1).split("\\;")

          val firstTimeField = groupDatasFirstArr{0}
          val workShopCodeField = groupDatasFirstArr{1}
          val partSpecField = groupDatasFirstArr{2}
          val opCodeField = groupDatasFirstArr{3}
          val staCodeField = groupDatasFirstArr{4}
          val eattribute1_1Field = groupDatasFirstArr{5}
          val testItemField = groupDatasFirstArr{6}
          val factoryField = groupDatasFirstArr{7}
          val samplenumField = groupDatasFirstArr{8}.toInt

          // 减去失控的点，如果不等于25(即告警数大于0),利用其余的点，重新计算控制限
          if (spcNonWarnList.size < 25 && spcNonWarnList.size != 0){

            // 计算去除告警后其余送检单号的xbar
            val xbarsum = avgValueList.sum / avgValueList.size
            // 计算去除告警后其余送检单号的极差均值
            val rangeAll = rangeValueList.sum / rangeValueList.size
            // 计算去除告警后其余送检单号的标准差均值
            val stdevAll = stdevValueList.sum / stdevValueList.size

            // 抽样数为1个点，计算单值和极差图的上下限
            if (samplenumField == 1){

              val value1Ucl = (xbarsum + 2.660 * rangeAll).formatted("%.3f")
              val value1Lcl = (xbarsum - 2.660 * rangeAll).formatted("%.3f")
              val range1Ucl = (3.267 * rangeAll).formatted("%.3f")
              val range1Lcl = (0 * rangeAll).formatted("%.3f")

              updateData(kuduMaster, spccontrollimit, s"${firstTimeField}",
                s"${workShopCodeField}", s"${partSpecField}",s"${opCodeField}" ,
                s"${staCodeField}" , s"${eattribute1_1Field}" ,
                s"${testItemField}" , s"${factoryField}", value1Ucl, value1Lcl, range1Ucl,
                range1Lcl, "", "")

            }

          } else if (spcNonWarnList.size == 0) {
            // 如果25个点都失控，删除控制限

            deleteData(kuduMaster, spccontrollimit, s"${firstTimeField}",
              s"${workShopCodeField}", s"${partSpecField}",
              s"${opCodeField}" , s"${staCodeField}" ,
              s"${eattribute1_1Field}" ,s"${testItemField}" , s"${factoryField}")

          }

        }
        // 清空当前集合
        groupDataList.clear()
        // 将自增数为1时的数据插入到集合中
        groupDataList+=transfData
        // 将数字初始值置为1
        integer.set(1)
      }
    }

  }

  def updateData(kuduMaster: String, spccontrollimit: String, firsttime: String, workshopcode: String,
                 partspec: String, opcode: String, stacode: String, eattribute11: String, testitem: String,
                 factory: String, avgucl: String, avglcl: String, rangeucl: String, rangelcl: String,
                 stddevucl: String, stddevlcl: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spccontrollimit)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val update = kuduTable.newUpdate()
    val rowUpdate = update.getRow()

    rowUpdate.addString("FIRST_TIME", s"${firsttime}")
    rowUpdate.addString("WORKSHOP_CODE", s"${workshopcode}")
    rowUpdate.addString("PART_SPEC", s"${partspec}")
    rowUpdate.addString("OP_CODE", s"${opcode}")
    rowUpdate.addString("STA_CODE", s"${stacode}")
    rowUpdate.addString("EATTRIBUTE1_1", s"${eattribute11}")
    rowUpdate.addString("TESTITEM", s"${testitem}")
    rowUpdate.addString("FACTORY", s"${factory}")
    rowUpdate.addString("AVGUCL", s"${avgucl}")
    rowUpdate.addString("AVGLCL", s"${avglcl}")
    rowUpdate.addString("RANGEUCL", s"${rangeucl}")
    rowUpdate.addString("RANGELCL", s"${rangelcl}")
    rowUpdate.addString("STDDEVUCL", s"${stddevucl}")
    rowUpdate.addString("STDDEVLCL", s"${stddevlcl}")

    // 执行upsert操作
    session.apply(update)

    session.flush()
    session.close()
    kuduClient.close()

  }


  def deleteData(kuduMaster: String, spccontrollimit: String, firsttime: String, workshopcode: String, partspec: String,
                 opcode: String, stacode: String, eattribute11: String, testitem: String, factory: String): Unit ={
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spccontrollimit)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val delete = kuduTable.newDelete()
    val rowDelete = delete.getRow()

    rowDelete.addString("FIRST_TIME", s"${firsttime}")
    rowDelete.addString("WORKSHOP_CODE", s"${workshopcode}")
    rowDelete.addString("PART_SPEC", s"${partspec}")
    rowDelete.addString("OP_CODE", s"${opcode}")
    rowDelete.addString("STA_CODE", s"${stacode}")
    rowDelete.addString("EATTRIBUTE1_1", s"${eattribute11}")
    rowDelete.addString("TESTITEM", s"${testitem}")
    rowDelete.addString("FACTORY", s"${factory}")

    // 执行upsert操作
    session.apply(delete)

    session.flush()
    session.close()
    kuduClient.close()

  }


}
