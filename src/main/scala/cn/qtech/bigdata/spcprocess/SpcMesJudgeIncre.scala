package cn.qtech.bigdata.spcprocess

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SpcMesJudgeIncre {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries","500")
//     .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val kuduContext = new KuduContext(kuduMaster, sc)

    val spcclresultlist = "ADS_SPCMESCONTROLLIMIT"
    val spcpointlist = "ADS_SPCMESPOINTLIST"
    val insertTable = "ADS_SPCMESRESULTLIST"

    judgeRule(sparkSession, sc, kuduMaster, spcclresultlist, spcpointlist, insertTable)

    sc.stop()
  }

  def judgeRule(sparkSession: SparkSession, sc:
  SparkContext,kuduMaster: String,spcclresultlist: String,spcpointlist: String,insertTable: String): Unit = {

    val optionsclresult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcclresultlist
    )
    val optionspoint = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcpointlist
    )
    val inserttable = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> insertTable
    )

    sparkSession.read.options(optionsclresult).format("kudu").load()
      .createOrReplaceTempView("getclresult")
    sparkSession.read.options(optionspoint).format("kudu").load()
      .createOrReplaceTempView("pointtablenotfiltertime")
    sparkSession.read.options(inserttable).format("kudu").load()
      .createOrReplaceTempView("inserttable")

      // 把结果表的最大时间作为程序运行起始时间，如果结果表为空，则把存量数据的最小时间作为起始时间
      // 把point表的最大时间作为程序运行的最大时间
      val kudutimeLists = sparkSession.sql(s"select max(time) from inserttable").collect().toList
      var kudutimeList = kudutimeLists.toString()
      var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

      var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from pointtablenotfiltertime")
        .collect()
        .toList
      var sourcetimelist = sourcetimelists.toString()
      var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
      var sourcetimemin = sourcetimes{0}
      var sourcetimemax = sourcetimes{1}

      var timemin = "2020-06-30 00:00:01"
      if (kudutimes != "null") {
        timemin = kudutimes
      }

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var sDate = sdf.parse(timemin)

      val calendar = Calendar.getInstance()
      calendar.setTime(sDate);
      calendar.add(Calendar.DATE,-90);  // 把日期往前推九十天,整数

      var minTime = sdf.format(calendar.getTime())

    sparkSession.sql("select * from pointtablenotfiltertime")
      .where(s"mdate>'${minTime}' and mdate<='${sourcetimemax}'")
//      .where(s"mdate>'2021-06-01 00:00:00' and mdate<='2021-09-30 00:00:00'")
      .createOrReplaceTempView("pointtable")

    // 查询控制限表中的数据,利用控制限中的一条数据查询并获取point表中多个点的数据
    val groupData = sparkSession.sql(
      """
        |   select a.FIRST_TIME,a.WORKSHOP_CODE,a.PART_SPEC,a.OP_CODE,a.STA_CODE,a.eattribute1_1,a.testitem, a.FACTORY,a.pcs,
        |   a.usl,a.lsl,a.avgucl,a.avglcl,a.rangeucl,a.rangelcl,a.stddevucl,a.stddevlcl,groupdata.ID_NO,groupdata.RCARD,
        |   groupdata.MDATE,groupdata.AVGVALUE,groupdata.RANGEVALUE,groupdata.STDDEVVALUE,
        |    row_number() over(partition by a.FIRST_TIME,
        |    a.WORKSHOP_CODE,
        |    a.PART_SPEC,
        |    a.OP_CODE,
        |    a.STA_CODE,
        |    a.eattribute1_1,
        |    a.testitem,
        |    a.factory
        |    order by groupdata.mdate asc) as num
        |    from getclresult a
        |    join (
        |        select ID_NO,RCARD,MDATE,AVGVALUE,RANGEVALUE,STDDEVVALUE,FIRST_TIME,WORKSHOP_CODE,PART_SPEC,OP_CODE,STA_CODE,eattribute1_1,TESTITEM,FACTORY
        |         from pointtable
        |    )groupdata on a.FIRST_TIME=groupdata.FIRST_TIME and a.WORKSHOP_CODE=groupdata.WORKSHOP_CODE and
        |    a.part_spec=groupdata.part_spec and a.OP_CODE=groupdata.OP_CODE and a.STA_CODE=groupdata.STA_CODE
        |    and a.eattribute1_1=groupdata.eattribute1_1 and a.TESTITEM=groupdata.TESTITEM and a.FACTORY=groupdata.FACTORY
        |
      """.stripMargin)

    val dataList = groupData.collect().toIterator

    val integer = new AtomicInteger(0)

    // 创建List保存送检单号数据和分组数据
    var groupDataList = new ListBuffer[String]()

    // 遍历并查询数据
    while (dataList.hasNext) {
      val rowdata = dataList.next()
      val datastr: String = rowdata.toString()
      val transfdata: String = datastr.substring(1, datastr.length-1)
      val splitArr: Array[String] = transfdata.split("\\,")

      val numfield = splitArr {23};

      // 自增加1
      integer.getAndIncrement()

      if (integer.get() == numfield.toInt) {

        // 不断把数据加入到集合中
        groupDataList += transfdata

      } else if(integer.get() > numfield.toInt) {

        // 当同一个分组的数据都加入集合后，遍历集合
        groupDataList.foreach (item => {

          // 主键id
          var uuid = UUID.randomUUID
          var id = uuid.toString

          // 遍历获取到一个点的数据
          val pointDatasStr = item.toString
          var pointArrs = pointDatasStr.split("\\,")
          val firstTime = pointArrs {0};
          val workShopCode= pointArrs {1};
          val partSpec = pointArrs {2};
          val opCode = pointArrs {3};
          val staCode = pointArrs {4};
          val eattribute1_1 = pointArrs {5};
          val testItem = pointArrs {6};
          val factory = pointArrs {7};
          val pcs = pointArrs {8}.toInt;
          val usl = pointArrs {9};
          val lsl = pointArrs {10};
          val avgUcl = pointArrs {11};
          val avgLcl = pointArrs {12};
          val rangeUcl = pointArrs {13};
          val rangeLcl = pointArrs {14};
          val stddevUcl = pointArrs {15};
          val stddevLcl = pointArrs {16};
          val id_No = pointArrs {17};
          val rCard= pointArrs {18};
          val mDate = pointArrs {19};
          val avgValue = pointArrs {20};
          val rangeValue = pointArrs {21};
          val stddevValue = pointArrs {22};
          val numPoint = pointArrs {23}.toInt;

          // SPC八大原则判断，根据每个单号抽取的检测数不同：
          // PCS为1个点的图类型为单值和移动极差图；2到5个点的图类型为均值极差图；6个点及以上的图类型为均值标准差图
          var spc8RuleList = new ListBuffer[String]()

          // 判断八个原则，同时不判断stddev相关的控制线
          if (avgValue != "" && rangeValue != "" && pcs == 1 && mDate > timemin) {

            val graphtype = "单值和移动极差图"
            // 一、1点在控制线外（1点距离中心线大于UCL-CL）
            if (numPoint >= 1) {
              // 1.判断单值
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "单值一"
              }
              // 2.判断极差
              if (rangeValue.toFloat > rangeUcl.toFloat || rangeValue.toFloat < rangeLcl.toFloat) {
                spc8RuleList += "极差一"
              }
            }

            // 二、3个点中有2个点，距离中心线同侧大于{（UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var rangerule2datalist = new ListBuffer[String]()
//
//              // 1.判断单值
//              val rulename1 = "单值二"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断极差
//              val rulename2 = "极差二"
//              judgeSpc2Rule(spc8RuleList, rangerule2datalist,groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // 三、5个点中有4个点在中心线同侧大于{（UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var rangerule3datalist = new ListBuffer[String]()
//
//              // 1.判断单值
//              val rulename1 = "单值三"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断极差
//              val rulename2 = "极差三"
//              judgeSpc3Rule(spc8RuleList, rangerule3datalist, groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // 四、连续6个点，全部递增或全部递减
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var rangerule4datalist = new ListBuffer[String]()

              // 1.判断单值
              val rulename1 = "单值四"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.判断极差
              val rulename2 = "极差四"
              judgeSpc4Rule(spc8RuleList, rangerule4datalist, groupDataList, rangeValue, rulename2, numPoint)
            }

            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)

            println("结果数据1========"+spc8RuleLists)

            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

          } else if (avgValue != "" && rangeValue != "" && pcs >= 2 && pcs <= 5 && mDate > timemin) {

            // 判断八个原则，同时不判断stddev相关的控制线
            val graphtype = "均值极差图"

            // 一、1点在控制线外（1点距离中心线大于UCL-CL）
            if (numPoint >= 1) {
              // 1.判断均值
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "均值一"
              }
              // 2.判断极差
              if (rangeValue.toFloat > rangeUcl.toFloat || rangeValue.toFloat < rangeLcl.toFloat) {
                spc8RuleList += "极差一"
              }
            }

            // 二、3个点中有2个点，距离中心线同侧大于{（UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var rangerule2datalist = new ListBuffer[String]()
//
//              // 1.判断均值
//              val rulename1 = "均值二"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断极差
//              val rulename2 = "极差二"
//              judgeSpc2Rule(spc8RuleList, rangerule2datalist, groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // 三、5个点中有4个点在中心线同侧大于{（UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var rangerule3datalist = new ListBuffer[String]()
//
//              // 1.判断均值
//              val rulename1 = "均值三"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断极差
//              val rulename2 = "极差三"
//              judgeSpc3Rule(spc8RuleList, rangerule3datalist,  groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // 四、连续6个点，全部递增或全部递减
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var rangerule4datalist = new ListBuffer[String]()

              // 1.判断均值
              val rulename1 = "均值四"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.判断极差
              val rulename2 = "极差四"
              judgeSpc4Rule(spc8RuleList, rangerule4datalist, groupDataList, rangeValue, rulename2, numPoint)
            }

            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)

            println("结果数据2========"+spc8RuleLists)

            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

          } else if (avgValue != "" && stddevValue != "" && pcs >= 6 && mDate > timemin) {
            // 判断八个原则，同时不判断range相关的控制线
            val graphtype = "均值标准差图"

            // 一、1点在控制线外（1点距离中心线大于UCL-CL）
            if (numPoint >= 1) {
              // 1.判断均值
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "均值一"
              }
              // 2.判断标准差
              if (stddevValue.toFloat > stddevUcl.toFloat || stddevValue.toFloat < stddevLcl.toFloat) {
                spc8RuleList += "标准差一"
              }
            }

            // 二、3个点中有2个点，距离中心线同侧大于{（UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var stddevrule2datalist = new ListBuffer[String]()
//
//              // 1.判断均值
//              val rulename1 = "均值二"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断标准差
//              val rulename2 = "标准差二"
//              judgeSpc2Rule(spc8RuleList, stddevrule2datalist, groupDataList, stddevUcl, stddevLcl, stddevValue,
//                rulename2,numPoint)
//            }

            // 三、5个点中有4个点在中心线同侧大于{（UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var stddevrule3datalist = new ListBuffer[String]()
//
//              // 1.判断均值
//              val rulename1 = "均值三"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.判断标准差
//              val rulename2 = "标准差三"
//              judgeSpc3Rule(spc8RuleList,stddevrule3datalist,groupDataList,stddevUcl,stddevLcl,stddevValue,rulename2, numPoint)
//            }

            // 四、连续6个点，全部递增或全部递减
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var stddevrule4datalist = new ListBuffer[String]()

              // 1.判断均值
              val rulename1 = "均值四"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.判断标准差
              val rulename2 = "标准差四"
              judgeSpc4Rule(spc8RuleList, stddevrule4datalist, groupDataList, stddevValue, rulename2, numPoint)
            }

            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)
            println("结果数据3========"+spc8RuleLists)

            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

          }
          spc8RuleList.clear()
        })
        println("一个分组结束，触发计算")
        // 清空当前集合
        groupDataList.clear()
        // 将自增数为1时的数据插入到集合中
        groupDataList += transfdata
        // 将数字初始值置为1
        integer.set(1)
      }

    }
  }


  def insertData(kuduMaster: String, spcclresultlist: String, id:String, time:String, firsttime:String,
                 workshopcode: String, partspec: String, opcode: String, stacode: String, eattribute11:String,
                 testitem: String, factory:String, idno: String, rcard:String, samplenum:Int, usl :String, lsl: String,
                 avgucl: String, avglcl: String, rangeucl: String, rangelcl: String, stddevucl: String,
                 stddevlcl: String, testvalueavg: String, testvaluerange: String, testvaluestdev: String,
                 spcwarn: String, spcgraphtype: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spcclresultlist)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val insert = kuduTable.newInsert()
    val rowUpsert = insert.getRow()

    rowUpsert.addString("ID", id)
    rowUpsert.addString("TIME", s"${time}")
    rowUpsert.addString("FIRST_TIME", s"${firsttime}")
    rowUpsert.addString("WORKSHOP_CODE", s"${workshopcode}")
    rowUpsert.addString("PART_SPEC", s"${partspec}")
    rowUpsert.addString("OP_CODE", s"${opcode}")
    rowUpsert.addString("STA_CODE", s"${stacode}")
    rowUpsert.addString("EATTRIBUTE1_1", s"${eattribute11}")
    rowUpsert.addString("TESTITEM", s"${testitem}")
    rowUpsert.addString("FACTORY", s"${factory}")
    rowUpsert.addString("ID_NO", s"${idno}")
    rowUpsert.addString("RCARD", s"${rcard}")
    rowUpsert.addInt("SAMPLENUM", samplenum)
    rowUpsert.addString("USL", s"${usl}")
    rowUpsert.addString("LSL", s"${lsl}")
    rowUpsert.addString("AVGUCL", s"${avgucl}")
    rowUpsert.addString("AVGLCL", s"${avglcl}")
    rowUpsert.addString("RANGEUCL", s"${rangeucl}")
    rowUpsert.addString("RANGELCL", s"${rangelcl}")
    rowUpsert.addString("STDDEVUCL", s"${stddevucl}")
    rowUpsert.addString("STDDEVLCL", s"${stddevlcl}")
    rowUpsert.addString("TESTVALUEAVG", s"${testvalueavg}")
    rowUpsert.addString("TESTVALUERANGE", s"${testvaluerange}")
    rowUpsert.addString("TESTVALUESTDEV", s"${testvaluestdev}")
    rowUpsert.addString("SPCWARN", s"${spcwarn}")
    rowUpsert.addString("SPCGRAPHTYPE", s"${spcgraphtype}")

    // 执行upsert操作
    session.apply(insert)

    session.flush()
    session.close()
    kuduClient.close()

  }

  // SPC规则二、3个点中有2个点，距离中心线同侧大于{（UCL-CL)2/3}
  def judgeSpc2Rule(spc8RuleList: ListBuffer[String], rule2datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], uclfield: String, lclfield: String, value: String,
                    rulename: String, numpoint:Int): Unit = {

    val rule2dataIterator = groupDataList.subList(numpoint - 3, numpoint - 1).toIterator
    while (rule2dataIterator.hasNext) {
      val rule2dataStr = rule2dataIterator.next().toString()
      val rule2datas = rule2dataStr.substring(0, rule2dataStr.length).split("\\,")

      if (rulename == "单值二" || rulename == "均值二") {
        rule2datalist += rule2datas{20}
      } else if (rulename == "极差二") {
        rule2datalist += rule2datas{21}
      } else if (rulename == "标准差二") {
        rule2datalist += rule2datas{22}
      }
      if (rule2datalist.contains("")) {
        rule2datalist.clear()
      }
    }

    if (rule2datalist.size == 2) {
      val cl = (uclfield.toFloat + lclfield.toFloat) / 2
      val rule2uslcldistance = (uclfield.toFloat - cl) * 2 / 3

      val the1data = rule2datalist{0}.toFloat
      val the2data = rule2datalist{1}.toFloat

      val uslcl1distance = (the1data - cl).abs
      val uslcl2distance = (the2data - cl).abs
      val uslcldistance = (value.toFloat - cl).abs
      var countlist1 = new ListBuffer[Float]()
      var countlist2 = new ListBuffer[Float]()
      // 判断点是在LSL-CL侧还是USL-CL侧
      // USL-CL侧
      if (uslcldistance > rule2uslcldistance && value.toFloat > cl) {
        if (uslcl1distance > rule2uslcldistance && the1data.toFloat > cl) {
          countlist1 += the1data
        }
        if (uslcl2distance > rule2uslcldistance && the2data.toFloat > cl) {
          countlist1 += the2data
        }
      }
      // LSL-CL侧
      if (uslcldistance > rule2uslcldistance && value.toFloat < cl) {
        if (uslcl1distance > rule2uslcldistance && the1data.toFloat < cl) {
          countlist2 += the1data
        }
        if (uslcl2distance > rule2uslcldistance && the2data.toFloat < cl) {
          countlist2 += the2data
        }
      }
      if (countlist1.size >= 1 || countlist2.size >= 1) {
        if (rulename == "单值二") {
          spc8RuleList += "单值二"
        } else if (rulename == "均值二") {
          spc8RuleList += "均值二"
        } else if (rulename == "极差二") {
          spc8RuleList += "极差二"
        } else if (rulename == "标准差二") {
          spc8RuleList += "标准差二"
        }
        countlist1.clear()
        countlist2.clear()
      }
    }
  }


  // SPC规则三、5个点中有4个点在中心线同侧大于{（UCL-CL)1/3}
  def judgeSpc3Rule(spc8RuleList: ListBuffer[String], rule3datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], uclfield: String, lclfield: String, value: String,
                    rulename: String, numpoint:Int): Unit = {

    val rule3dataIterator = groupDataList.subList(numpoint - 5, numpoint - 1).toIterator
    while (rule3dataIterator.hasNext){
      val rule3dataStr = rule3dataIterator.next().toString()
      val rule3datas = rule3dataStr.substring(0, rule3dataStr.length).split("\\,")

      if (rulename == "单值三" || rulename == "均值三") {
        rule3datalist += rule3datas{20}
      } else if (rulename == "极差三") {
        rule3datalist += rule3datas{21}
      } else if (rulename == "标准差三") {
        rule3datalist += rule3datas{22}
      }
      if (rule3datalist.contains("")) {
        rule3datalist.clear()
      }
    }

    if (rule3datalist.size == 4) {
      val cl = (uclfield.toFloat + lclfield.toFloat) / 2
      val rule3uslcldistance = (uclfield.toFloat - cl) / 3

      val the1data = rule3datalist{0}.toFloat
      val the2data = rule3datalist{1}.toFloat
      val the3data = rule3datalist{2}.toFloat
      val the4data = rule3datalist{3}.toFloat

      val uslcl1distance = (the1data - cl).abs
      val uslcl2distance = (the2data - cl).abs
      val uslcl3distance = (the3data - cl).abs
      val uslcl4distance = (the4data - cl).abs
      val uslcldistance = (value.toFloat - cl).abs
      var countlist1 = new ListBuffer[Float]()
      var countlist2 = new ListBuffer[Float]()
      // 判断点是在LSL-CL侧还是USL-CL侧
      // USL-CL侧
      if (uslcldistance > rule3uslcldistance && value.toFloat > cl) {
        if (uslcl1distance > rule3uslcldistance && the1data.toFloat > cl) {
          countlist1 += the1data
        }
        if (uslcl2distance > rule3uslcldistance && the2data.toFloat > cl) {
          countlist1 += the2data
        }
        if (uslcl3distance > rule3uslcldistance && the3data.toFloat > cl) {
          countlist1 += the3data
        }
        if (uslcl4distance > rule3uslcldistance && the4data.toFloat > cl) {
          countlist1 += the4data
        }
      }
      // LSL-CL侧
      if (uslcldistance > rule3uslcldistance && value.toFloat < cl) {
        if (uslcl1distance > rule3uslcldistance && the1data.toFloat < cl) {
          countlist2 += the1data
        }
        if (uslcl2distance > rule3uslcldistance && the2data.toFloat < cl) {
          countlist2 += the2data
        }
        if (uslcl3distance > rule3uslcldistance && the3data.toFloat < cl) {
          countlist2 += the3data
        }
        if (uslcl4distance > rule3uslcldistance && the4data.toFloat < cl) {
          countlist2 += the4data
        }
      }
      if (countlist1.size >= 3 || countlist2.size >= 3) {
        if (rulename == "单值三") {
          spc8RuleList += "单值三"
        } else if (rulename == "均值三") {
          spc8RuleList += "均值三"
        } else if (rulename == "极差三") {
          spc8RuleList += "极差三"
        } else if (rulename == "标准差三") {
          spc8RuleList += "标准差三"
        }
        countlist1.clear()
        countlist2.clear()
      }
    }
  }


  // SPC规则四、连续6个点，全部递增或全部递减
  def judgeSpc4Rule(spc8RuleList: ListBuffer[String], rule4datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], value: String, rulename: String, numpoint:Int): Unit = {

    val rule4dataIterator = groupDataList.subList(numpoint - 6, numpoint - 1).toIterator
    while (rule4dataIterator.hasNext) {
      val rule4dataStr = rule4dataIterator.next().toString()
      val rule4datas = rule4dataStr.substring(0, rule4dataStr.length).split("\\,")

      if (rulename == "单值四" || rulename == "均值四") {
        rule4datalist += rule4datas{20}
      } else if (rulename == "极差四") {
        rule4datalist += rule4datas{21}
      } else if (rulename == "标准差四"){
        rule4datalist += rule4datas{22}
      }

      if (rule4datalist.contains("")) {
        rule4datalist.clear()
      }
    }

    if (rule4datalist.size == 5) {

      val the1data = rule4datalist{0}.toFloat
      val the2data = rule4datalist{1}.toFloat
      val the3data = rule4datalist{2}.toFloat
      val the4data = rule4datalist{3}.toFloat
      val the5data = rule4datalist{4}.toFloat

      if ((value.toFloat > the1data && the1data > the2data && the2data > the3data && the3data > the4data &&
        the4data > the5data) || (value.toFloat < the1data && the1data < the2data && the2data < the3data &&
        the3data < the4data && the4data < the5data)) {
        if (rulename == "单值四") {
          spc8RuleList += "单值四"
        } else if (rulename == "均值四") {
          spc8RuleList += "均值四"
        } else if (rulename == "极差四") {
          spc8RuleList += "极差四"
        } else if (rulename == "标准差四") {
          spc8RuleList += "标准差四"
        }
      }
    }
  }


}
