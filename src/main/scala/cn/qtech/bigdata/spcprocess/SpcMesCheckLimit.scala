package cn.qtech.bigdata.spcprocess

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.ListBuffer

object SpcMesCheckLimit {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
    .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val kuduContext = new KuduContext(kuduMaster, sc)

    val spccontrollimit = "ADS_SPCMESCONTROLLIMIT_TEST"
    val spcresultlist = "ADS_SPCMESRESULTLIST"

    SpcCalculate(sparkSession: SparkSession, sc:
      SparkContext, kuduMaster: String, spccontrollimit: String, spcresultlist: String)

    sc.stop()
  }

  private def SpcCalculate(sparkSession: SparkSession, sc:
  SparkContext, kuduMaster: String, spccontrollimit: String, spcresultlist: String): Unit = {
    val optioncl = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spccontrollimit
    )
    val optionresult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcresultlist
    )

    sparkSession.read.options(optioncl).format("kudu").load
      .createOrReplaceTempView("tempcl")
    sparkSession.read.options(optionresult).format("kudu").load
      .createOrReplaceTempView("tempresult")

    // 使用spc结果表，分组后根据时间取每组最新的25条数据(25个点)
    val groupdata = sparkSession.sql(
      """
        |select
        |row_number() over(partition by a.FIRST_TIME,
        |a.WORKSHOP_CODE,
        |a.PART_SPEC,
        |a.OP_CODE,
        |a.STA_CODE,
        |a.EATTRIBUTE1_1,
        |a.TESTITEM,
        |a.FACTORY
        |order by a.TIME desc) as num,
        |a.FIRST_TIME, a.WORKSHOP_CODE, a.PART_SPEC,
        |a.OP_CODE, a.STA_CODE, a.EATTRIBUTE1_1,
        |a.TESTITEM, a.FACTORY,
        |a.SAMPLENUM,
        |a.TESTVALUEAVG, a.TESTVALUERANGE, a.TESTVALUESTDEV, a.SPCWARN, a.USL, a.LSL,
        |a.TIME
        |from tempresult a where FACTORY not in ('台虹厂','古城一厂')
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

      val numfield = splitArr {
        0
      };

      val firstTime = rowData.getAs[String]("FIRST_TIME");
      val workShopCode = rowData.getAs[String]("WORKSHOP_CODE");
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

      val transfData: String = firstTime + ";" + workShopCode + ";" + partSpec + ";" + opCode + ";" + staCode + ";" +
        eattribute1_1 + ";" + testItem + ";" + factory + ";" + samplenum + ";" + avgvalue + ";" + rangevalue + ";" + stddevvalue +
        ";" + spcwarn + ";" + usl + ";" + lsl + ";" + mdate

      // 自增加1
      integer.getAndIncrement()

      if (integer.get() == numfield.toInt) {

        // 不断把数据加入到集合中
        groupDataList += transfData

      } else if (integer.get() > numfield.toInt) {

        if (groupDataList.size >= 25) {

          // 保存非告警的数量
          var spcNonWarnList = new ListBuffer[Int]()
          // 保存单值或均值
          var avgValueList = new ListBuffer[Float]()
          // 保存极差值
          var rangeValueList = new ListBuffer[Float]()
          // 保存标准差值
          var stdevValueList = new ListBuffer[Float]()

          // 当同一个分组的数据都加入集合后，遍历集合
          groupDataList.subList(0, 25).foreach(item => {

            // 遍历获取到一个点的数据
            val pointDatasStr = item.toString
            var pointArrs = pointDatasStr.split("\\;")
            val avgValue = pointArrs {
              9
            };
            val rangeValue = pointArrs {
              10
            };
            val stdevValue = pointArrs {
              11
            };
            val spcWarnNum = pointArrs {
              12
            };

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

          val firstTimeField = groupDatasFirstArr {
            0
          }
          val workShopCodeField = groupDatasFirstArr {
            1
          }
          val partSpecField = groupDatasFirstArr {
            2
          }
          val opCodeField = groupDatasFirstArr {
            3
          }
          val staCodeField = groupDatasFirstArr {
            4
          }
          val eattribute1_1Field = groupDatasFirstArr {
            5
          }
          val testItemField = groupDatasFirstArr {
            6
          }
          val factoryField = groupDatasFirstArr {
            7
          }
          val samplenumField = groupDatasFirstArr {
            8
          }.toInt

          // 减去失控的点，如果不等于25(即告警数大于0),利用其余的点，重新计算控制限
          if (spcNonWarnList.size < 25 && spcNonWarnList.size != 0) {

            // 计算去除告警后其余送检单号的xbar
            val xbarsum = avgValueList.sum / avgValueList.size
            // 计算去除告警后其余送检单号的极差均值
            val rangeAll = rangeValueList.sum / rangeValueList.size
            // 计算去除告警后其余送检单号的标准差均值
            val stdevAll = stdevValueList.sum / stdevValueList.size

            // 抽样数为1个点，计算单值和极差图的上下限
            if (samplenumField == 1) {

              val value1Ucl = (xbarsum + 2.660 * rangeAll).formatted("%.3f")
              val value1Lcl = (xbarsum - 2.660 * rangeAll).formatted("%.3f")
              val range1Ucl = (3.267 * rangeAll).formatted("%.3f")
              val range1Lcl = (0 * rangeAll).formatted("%.3f")

              updateData(kuduMaster, spccontrollimit, s"${firstTimeField}", s"${workShopCodeField}", s"${partSpecField}", s"${opCodeField}",
                s"${staCodeField}", s"${eattribute1_1Field}",
                s"${testItemField}", s"${factoryField}", value1Ucl, value1Lcl, range1Ucl,
                range1Lcl, "", "")

              // 抽样数为2到5个点，计算平均值图和极差图的上下限
            } else if (samplenumField >= 2 && samplenumField <= 5) {

              var a2constant: Double = 0
              if (samplenumField == 2) {
                a2constant = 1.880
              } else if (samplenumField == 3) {
                a2constant = 1.023
              } else if (samplenumField == 4) {
                a2constant = 0.729
              } else if (samplenumField == 5) {
                a2constant = 0.577
              }

              var d4constant: Double = 0
              if (samplenumField == 2) {
                d4constant = 3.267
              } else if (samplenumField == 3) {
                d4constant = 2.574
              } else if (samplenumField == 4) {
                d4constant = 2.282
              } else if (samplenumField == 5) {
                d4constant = 2.114
              }

              val value2to5Ucl = (xbarsum + a2constant * rangeAll).formatted("%.3f")
              val value2to5Lcl = (xbarsum - a2constant * rangeAll).formatted("%.3f")
              val range2to5Ucl = (d4constant * rangeAll).formatted("%.3f")
              val range2to5Lcl = (0 * rangeAll).formatted("%.3f")

              updateData(kuduMaster, spccontrollimit, s"${firstTimeField}", s"${workShopCodeField}", s"${partSpecField}", s"${opCodeField}",
                s"${staCodeField}", s"${eattribute1_1Field}",
                s"${testItemField}", s"${factoryField}", value2to5Ucl, value2to5Lcl,
                range2to5Ucl, range2to5Lcl, "", "")

              // 抽样数为6个点到25个点，计算平均值图和标准差图的上下限
            } else if (samplenumField >= 6 && samplenumField <= 25) {

              var a3constant: Double = 0
              if (samplenumField == 6) {
                a3constant = 1.287
              } else if (samplenumField == 7) {
                a3constant = 1.182
              } else if (samplenumField == 8) {
                a3constant = 1.099
              } else if (samplenumField == 9) {
                a3constant = 1.032
              } else if (samplenumField == 10) {
                a3constant = 0.975
              } else if (samplenumField == 11) {
                a3constant = 0.927
              } else if (samplenumField == 12) {
                a3constant = 0.886
              } else if (samplenumField == 13) {
                a3constant = 0.850
              } else if (samplenumField == 14) {
                a3constant = 0.817
              } else if (samplenumField == 15) {
                a3constant = 0.789
              } else if (samplenumField == 16) {
                a3constant = 0.763
              } else if (samplenumField == 17) {
                a3constant = 0.739
              } else if (samplenumField == 18) {
                a3constant = 0.718
              } else if (samplenumField == 19) {
                a3constant = 0.698
              } else if (samplenumField == 20) {
                a3constant = 0.680
              } else if (samplenumField == 21) {
                a3constant = 0.663
              } else if (samplenumField == 22) {
                a3constant = 0.647
              } else if (samplenumField == 23) {
                a3constant = 0.633
              } else if (samplenumField == 24) {
                a3constant = 0.619
              } else if (samplenumField == 25) {
                a3constant = 0.606
              }

              var b4constant: Double = 0
              if (samplenumField == 6) {
                b4constant = 1.970
              } else if (samplenumField == 7) {
                b4constant = 1.882
              } else if (samplenumField == 8) {
                b4constant = 1.815
              } else if (samplenumField == 9) {
                b4constant = 1.761
              } else if (samplenumField == 10) {
                b4constant = 1.716
              } else if (samplenumField == 11) {
                b4constant = 1.679
              } else if (samplenumField == 12) {
                b4constant = 1.646
              } else if (samplenumField == 13) {
                b4constant = 1.618
              } else if (samplenumField == 14) {
                b4constant = 1.594
              } else if (samplenumField == 15) {
                b4constant = 1.572
              } else if (samplenumField == 16) {
                b4constant = 1.552
              } else if (samplenumField == 17) {
                b4constant = 1.534
              } else if (samplenumField == 18) {
                b4constant = 1.518
              } else if (samplenumField == 19) {
                b4constant = 1.503
              } else if (samplenumField == 20) {
                b4constant = 1.490
              } else if (samplenumField == 21) {
                b4constant = 1.477
              } else if (samplenumField == 22) {
                b4constant = 1.466
              } else if (samplenumField == 23) {
                b4constant = 1.455
              } else if (samplenumField == 24) {
                b4constant = 1.445
              } else if (samplenumField == 25) {
                b4constant = 1.435
              }

              var b3constant: Double = 0
              if (samplenumField == 6) {
                b3constant = 0.030
              } else if (samplenumField == 7) {
                b3constant = 0.118
              } else if (samplenumField == 8) {
                b3constant = 0.185
              } else if (samplenumField == 9) {
                b3constant = 0.239
              } else if (samplenumField == 10) {
                b3constant = 0.284
              } else if (samplenumField == 11) {
                b3constant = 0.321
              } else if (samplenumField == 12) {
                b3constant = 0.354
              } else if (samplenumField == 13) {
                b3constant = 0.382
              } else if (samplenumField == 14) {
                b3constant = 0.406
              } else if (samplenumField == 15) {
                b3constant = 0.428
              } else if (samplenumField == 16) {
                b3constant = 0.448
              } else if (samplenumField == 17) {
                b3constant = 0.466
              } else if (samplenumField == 18) {
                b3constant = 0.482
              } else if (samplenumField == 19) {
                b3constant = 0.497
              } else if (samplenumField == 20) {
                b3constant = 0.510
              } else if (samplenumField == 21) {
                b3constant = 0.523
              } else if (samplenumField == 22) {
                b3constant = 0.534
              } else if (samplenumField == 23) {
                b3constant = 0.545
              } else if (samplenumField == 24) {
                b3constant = 0.555
              } else if (samplenumField == 25) {
                b3constant = 0.565
              }

              val value6Ucl = (xbarsum + a3constant * stdevAll).formatted("%.3f")
              val value6Lcl = (xbarsum - a3constant * stdevAll).formatted("%.3f")
              val stddev6Ucl = (b4constant * stdevAll).formatted("%.3f")
              val stddev6Lcl = (b3constant * stdevAll).formatted("%.3f")

              updateData(kuduMaster, spccontrollimit, s"${firstTimeField}", s"${workShopCodeField}", s"${partSpecField}", s"${opCodeField}",
                s"${staCodeField}", s"${eattribute1_1Field}",
                s"${testItemField}", s"${factoryField}", value6Ucl, value6Lcl,
                "", "", stddev6Ucl, stddev6Lcl)

            }

          } else if (spcNonWarnList.size == 0) {
            // 如果25个点都失控，删除控制限

            deleteData(kuduMaster, spccontrollimit, s"${firstTimeField}", s"${workShopCodeField}",
              s"${partSpecField}", s"${opCodeField}", s"${staCodeField}",
              s"${eattribute1_1Field}", s"${testItemField}", s"${factoryField}")

          }

        }
        // 清空当前集合
        groupDataList.clear()
        // 将自增数为1时的数据插入到集合中
        groupDataList += transfData
        // 将数字初始值置为1
        integer.set(1)
      }
    }

  }

  def updateData(kuduMaster: String, spccontrollimit: String, firsttime: String, workshopcode: String, partspec: String,
                 opcode: String, stacode: String, eattribute11: String, testitem: String, factory: String,
                 avgucl: String, avglcl: String, rangeucl: String, rangelcl: String, stddevucl: String,
                 stddevlcl: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spccontrollimit)

    val session = kuduClient.newSession()
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


  def deleteData(kuduMaster: String, spccontrollimit: String, firsttime: String, workshopcode: String,
                 partspec: String, opcode: String, stacode: String, eattribute11: String, testitem: String,
                 factory: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spccontrollimit)

    val session = kuduClient.newSession()
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

    // 执行delete操作
    session.apply(delete)

    session.flush()
    session.close()
    kuduClient.close()

  }


}
