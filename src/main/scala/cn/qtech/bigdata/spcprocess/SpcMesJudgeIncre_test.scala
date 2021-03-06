package cn.qtech.bigdata.spcprocess

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Calendar, UUID}

import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SpcMesJudgeIncre_test {
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

    var result = new ListBuffer[String]()

    sparkSession.read.options(optionsclresult).format("kudu").load()
      .createOrReplaceTempView("getclresult")
    sparkSession.read.options(optionspoint).format("kudu").load()
      .createOrReplaceTempView("pointtablenotfiltertime")
    sparkSession.read.options(inserttable).format("kudu").load()
      .createOrReplaceTempView("inserttable")

      // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
      // ???point???????????????????????????????????????????????????
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
      calendar.add(Calendar.DATE,-90);  // ???????????????????????????,??????

      var minTime = sdf.format(calendar.getTime())

    sparkSession.sql("select * from pointtablenotfiltertime")
      .where(s"mdate>'${minTime}' and mdate<='${sourcetimemax}'")
//      .where(s"mdate>'2020-06-30 01:12:51' and mdate<='2021-11-02 00:00:00' and workshop_code = '??????COB??????'")
      .createOrReplaceTempView("pointtable")

    // ??????????????????????????????,????????????????????????????????????????????????point????????????????????????
    val groupData = sparkSession.sql(
      """
        |select a.FIRST_TIME,a.WORKSHOP_CODE,a.PART_SPEC,a.OP_CODE,a.STA_CODE,a.eattribute1_1,a.testitem, a.FACTORY,a.pcs,
        |   a.usl,a.lsl,a.avgucl,a.avglcl,a.rangeucl,a.rangelcl,a.stddevucl,a.stddevlcl,groupdata.ID_NO,groupdata.RCARD,
        |   groupdata.MDATE,
        |   case when groupdata.AVGVALUE = 'null' then '' else groupdata.AVGVALUE end as AVGVALUE,
        |   case when groupdata.RANGEVALUE = 'null' then '' else groupdata.RANGEVALUE  end as RANGEVALUE,
        |   case when groupdata.STDDEVVALUE = 'null' then '' else groupdata.STDDEVVALUE  end as STDDEVVALUE,
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
      """.stripMargin)

    val dataList = groupData.collect().toIterator

    val integer = new AtomicInteger(0)

    // ??????List???????????????????????????????????????
    var groupDataList = new ListBuffer[String]()

    // ?????????????????????
    while (dataList.hasNext) {
      val rowdata = dataList.next()
      val datastr: String = rowdata.toString()
      val transfdata: String = datastr.substring(1, datastr.length-1)
      val splitArr: Array[String] = transfdata.split("\\,")

      val numfield = splitArr {23};

      // ?????????1
      integer.getAndIncrement()

      if (integer.get() == numfield.toInt) {

        // ?????????????????????????????????
        groupDataList += transfdata

      } else if(integer.get() > numfield.toInt) {

        // ????????????????????????????????????????????????????????????
        groupDataList.foreach (item => {

          // ??????id
          var uuid = UUID.randomUUID
          var id = uuid.toString

          // ?????????????????????????????????
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

          // SPC??????????????????????????????????????????????????????????????????
          // PCS???1????????????????????????????????????????????????2???5???????????????????????????????????????6????????????????????????????????????????????????
          var spc8RuleList = new ListBuffer[String]()

          // ????????????????????????????????????stddev??????????????????
          if (avgValue != "" && rangeValue != "" && pcs == 1 && mDate > timemin) {

            val graphtype = "????????????????????????"
            // ??????1?????????????????????1????????????????????????UCL-CL???
            if (numPoint >= 1) {
              // 1.????????????
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "?????????"
              }
              // 2.????????????
              if (rangeValue.toFloat > rangeUcl.toFloat || rangeValue.toFloat < rangeLcl.toFloat) {
                spc8RuleList += "?????????"
              }
            }

            // ??????3????????????2????????????????????????????????????{???UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var rangerule2datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.????????????
//              val rulename2 = "?????????"
//              judgeSpc2Rule(spc8RuleList, rangerule2datalist,groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // ??????5????????????4??????????????????????????????{???UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var rangerule3datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.????????????
//              val rulename2 = "?????????"
//              judgeSpc3Rule(spc8RuleList, rangerule3datalist, groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // ????????????6????????????????????????????????????
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var rangerule4datalist = new ListBuffer[String]()

              // 1.????????????
              val rulename1 = "?????????"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.????????????
              val rulename2 = "?????????"
              judgeSpc4Rule(spc8RuleList, rangerule4datalist, groupDataList, rangeValue, rulename2, numPoint)
            }

            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)

            println("????????????1========"+spc8RuleLists)

            result += id+","+ s"${mDate}"+","+ s"${firstTime}"+","+ s"${workShopCode}"+","+ s"${partSpec}"+","+ s"${opCode}"+","+s"${staCode}"+","+
              s"${eattribute1_1}"+","+s"${testItem}"+","+s"${factory}"+","+ s"${id_No}"+","+ s"${rCard}"+","+pcs+","+ s"${usl}"+","+
              s"${lsl}"+","+s"${avgUcl}"+","+ s"${avgLcl}"+","+ s"${rangeUcl}"+","+s"${rangeLcl}"+","+s"${stddevUcl}"+","+s"${stddevLcl}"+","+s"${avgValue}"+","+
              s"${rangeValue}"+","+s"${stddevValue}"+","+spc8RuleLists+","+graphtype

//            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
//              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
//              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
//              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

          } else if (avgValue != "" && rangeValue != "" && pcs >= 2 && pcs <= 5 && mDate > timemin) {

            // ????????????????????????????????????stddev??????????????????
            val graphtype = "???????????????"

            // ??????1?????????????????????1????????????????????????UCL-CL???
            if (numPoint >= 1) {
              // 1.????????????
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "?????????"
              }
              // 2.????????????
              if (rangeValue.toFloat > rangeUcl.toFloat || rangeValue.toFloat < rangeLcl.toFloat) {
                spc8RuleList += "?????????"
              }
            }

            // ??????3????????????2????????????????????????????????????{???UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var rangerule2datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.????????????
//              val rulename2 = "?????????"
//              judgeSpc2Rule(spc8RuleList, rangerule2datalist, groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // ??????5????????????4??????????????????????????????{???UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var rangerule3datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.????????????
//              val rulename2 = "?????????"
//              judgeSpc3Rule(spc8RuleList, rangerule3datalist,  groupDataList, rangeUcl, rangeLcl, rangeValue, rulename2,
//                numPoint)
//            }

            // ????????????6????????????????????????????????????
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var rangerule4datalist = new ListBuffer[String]()

              // 1.????????????
              val rulename1 = "?????????"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.????????????
              val rulename2 = "?????????"
              judgeSpc4Rule(spc8RuleList, rangerule4datalist, groupDataList, rangeValue, rulename2, numPoint)
            }


            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)

            println("????????????2========"+spc8RuleLists)

            result += id+","+ s"${mDate}"+","+ s"${firstTime}"+","+ s"${workShopCode}"+","+ s"${partSpec}"+","+ s"${opCode}"+","+s"${staCode}"+","+
              s"${eattribute1_1}"+","+s"${testItem}"+","+s"${factory}"+","+ s"${id_No}"+","+ s"${rCard}"+","+pcs+","+ s"${usl}"+","+
              s"${lsl}"+","+s"${avgUcl}"+","+ s"${avgLcl}"+","+ s"${rangeUcl}"+","+s"${rangeLcl}"+","+s"${stddevUcl}"+","+s"${stddevLcl}"+","+s"${avgValue}"+","+
              s"${rangeValue}"+","+s"${stddevValue}"+","+spc8RuleLists+","+graphtype

//            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
//              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
//              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
//              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

            //&& stddevUcl != "" && stddevLcl != ""
          } else if (avgValue != "" && stddevValue != "" && stddevUcl != "" && stddevLcl!= ""  && pcs >= 6 && mDate > timemin) {
            // ????????????????????????????????????range??????????????????
            val graphtype = "??????????????????"

            // ??????1?????????????????????1????????????????????????UCL-CL???
            if (numPoint >= 1) {
              // 1.????????????
              if (avgValue.toFloat > avgUcl.toFloat || avgValue.toFloat < avgLcl.toFloat) {
                spc8RuleList += "?????????"
              }
              // 2.???????????????
              if (stddevValue.toFloat > stddevUcl.toFloat || stddevValue.toFloat < stddevLcl.toFloat) {
                spc8RuleList += "????????????"
              }
            }

            // ??????3????????????2????????????????????????????????????{???UCL-CL)2/3}
//            if (numPoint >= 3) {
//              var avgrule2datalist = new ListBuffer[String]()
//              var stddevrule2datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc2Rule(spc8RuleList, avgrule2datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.???????????????
//              val rulename2 = "????????????"
//              judgeSpc2Rule(spc8RuleList, stddevrule2datalist, groupDataList, stddevUcl, stddevLcl, stddevValue,
//                rulename2,numPoint)
//            }

            // ??????5????????????4??????????????????????????????{???UCL-CL)1/3}
//            if (numPoint >= 5) {
//              var avgrule3datalist = new ListBuffer[String]()
//              var stddevrule3datalist = new ListBuffer[String]()
//
//              // 1.????????????
//              val rulename1 = "?????????"
//              judgeSpc3Rule(spc8RuleList, avgrule3datalist, groupDataList, avgUcl, avgLcl, avgValue, rulename1, numPoint)
//              // 2.???????????????
//              val rulename2 = "????????????"
//              judgeSpc3Rule(spc8RuleList,stddevrule3datalist,groupDataList,stddevUcl,stddevLcl,stddevValue,rulename2, numPoint)
//            }

            // ????????????6????????????????????????????????????
            if (numPoint >= 6) {
              var avgrule4datalist = new ListBuffer[String]()
              var stddevrule4datalist = new ListBuffer[String]()

              // 1.????????????
              val rulename1 = "?????????"
              judgeSpc4Rule(spc8RuleList, avgrule4datalist, groupDataList, avgValue, rulename1, numPoint)
              // 2.???????????????
              val rulename2 = "????????????"
              judgeSpc4Rule(spc8RuleList, stddevrule4datalist, groupDataList, stddevValue, rulename2, numPoint)
            }

            val spc8RuleListStr = spc8RuleList.toString()
            val spc8RuleLists = spc8RuleListStr.substring(11, spc8RuleListStr.length - 1)
            println("????????????3========"+spc8RuleLists)

            result += id+","+ s"${mDate}"+","+ s"${firstTime}"+","+ s"${workShopCode}"+","+ s"${partSpec}"+","+ s"${opCode}"+","+s"${staCode}"+","+
              s"${eattribute1_1}"+","+s"${testItem}"+","+s"${factory}"+","+ s"${id_No}"+","+ s"${rCard}"+","+pcs+","+ s"${usl}"+","+
              s"${lsl}"+","+s"${avgUcl}"+","+ s"${avgLcl}"+","+ s"${rangeUcl}"+","+s"${rangeLcl}"+","+s"${stddevUcl}"+","+s"${stddevLcl}"+","+s"${avgValue}"+","+
              s"${rangeValue}"+","+s"${stddevValue}"+","+spc8RuleLists+","+graphtype
//            insertData(kuduMaster, insertTable, id, s"${mDate}", s"${firstTime}", s"${workShopCode}", s"${partSpec}", s"${opCode}",s"${staCode}",
//              s"${eattribute1_1}",s"${testItem}",s"${factory}", s"${id_No}", s"${rCard}",pcs, s"${usl}",
//              s"${lsl}",s"${avgUcl}", s"${avgLcl}", s"${rangeUcl}",s"${rangeLcl}",s"${stddevUcl}",s"${stddevLcl}",s"${avgValue}",
//              s"${rangeValue}",s"${stddevValue}",spc8RuleLists,graphtype)

          }
          spc8RuleList.clear()
        })
        println("?????????????????????????????????")
        // ??????????????????
        groupDataList.clear()
        // ???????????????1??????????????????????????????
        groupDataList += transfdata
        // ????????????????????????1
        integer.set(1)
      }

    }

    print(result.size)

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(insertTable)

    val  session = kuduClient.newSession()
    session.setMutationBufferSpace(20000)
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)

    for(datas <- result){

      val data: Array[String] = datas.split(",")
      val insert = kuduTable.newInsert()
      val rowUpsert = insert.getRow()
      rowUpsert.addString("ID", data(0))
      rowUpsert.addString("TIME", data(1))
      rowUpsert.addString("FIRST_TIME", data(2))
      rowUpsert.addString("WORKSHOP_CODE", data(3))
      rowUpsert.addString("PART_SPEC", data(4))
      rowUpsert.addString("OP_CODE", data(5))
      rowUpsert.addString("STA_CODE", data(6))
      rowUpsert.addString("EATTRIBUTE1_1", data(7))
      rowUpsert.addString("TESTITEM", data(8))
      rowUpsert.addString("FACTORY", data(9))
      rowUpsert.addString("ID_NO", data(10))
      rowUpsert.addString("RCARD", data(11))
      rowUpsert.addInt("SAMPLENUM", data(12).toInt)
      rowUpsert.addString("USL", data(13))
      rowUpsert.addString("LSL", data(14))
      rowUpsert.addString("AVGUCL", data(15))
      rowUpsert.addString("AVGLCL", data(16))
      rowUpsert.addString("RANGEUCL", data(17))
      rowUpsert.addString("RANGELCL", data(18))
      rowUpsert.addString("STDDEVUCL", data(19))
      rowUpsert.addString("STDDEVLCL", data(20))
      rowUpsert.addString("TESTVALUEAVG", data(21))
      rowUpsert.addString("TESTVALUERANGE", data(22))
      rowUpsert.addString("TESTVALUESTDEV", data(23))
      rowUpsert.addString("SPCWARN", data(24))
      rowUpsert.addString("SPCGRAPHTYPE", data(25))
      session.apply(insert)
    }
    session.flush()
    session.close()
    kuduClient.close()

  }

  // SPC????????????3????????????2????????????????????????????????????{???UCL-CL)2/3}
  def judgeSpc2Rule(spc8RuleList: ListBuffer[String], rule2datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], uclfield: String, lclfield: String, value: String,
                    rulename: String, numpoint:Int): Unit = {

    val rule2dataIterator = groupDataList.subList(numpoint - 3, numpoint - 1).toIterator
    while (rule2dataIterator.hasNext) {
      val rule2dataStr = rule2dataIterator.next().toString()
      val rule2datas = rule2dataStr.substring(0, rule2dataStr.length).split("\\,")

      if (rulename == "?????????" || rulename == "?????????") {
        rule2datalist += rule2datas{20}
      } else if (rulename == "?????????") {
        rule2datalist += rule2datas{21}
      } else if (rulename == "????????????") {
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
      // ???????????????LSL-CL?????????USL-CL???
      // USL-CL???
      if (uslcldistance > rule2uslcldistance && value.toFloat > cl) {
        if (uslcl1distance > rule2uslcldistance && the1data.toFloat > cl) {
          countlist1 += the1data
        }
        if (uslcl2distance > rule2uslcldistance && the2data.toFloat > cl) {
          countlist1 += the2data
        }
      }
      // LSL-CL???
      if (uslcldistance > rule2uslcldistance && value.toFloat < cl) {
        if (uslcl1distance > rule2uslcldistance && the1data.toFloat < cl) {
          countlist2 += the1data
        }
        if (uslcl2distance > rule2uslcldistance && the2data.toFloat < cl) {
          countlist2 += the2data
        }
      }
      if (countlist1.size >= 1 || countlist2.size >= 1) {
        if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "????????????") {
          spc8RuleList += "????????????"
        }
        countlist1.clear()
        countlist2.clear()
      }
    }
  }


  // SPC????????????5????????????4??????????????????????????????{???UCL-CL)1/3}
  def judgeSpc3Rule(spc8RuleList: ListBuffer[String], rule3datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], uclfield: String, lclfield: String, value: String,
                    rulename: String, numpoint:Int): Unit = {

    val rule3dataIterator = groupDataList.subList(numpoint - 5, numpoint - 1).toIterator
    while (rule3dataIterator.hasNext){
      val rule3dataStr = rule3dataIterator.next().toString()
      val rule3datas = rule3dataStr.substring(0, rule3dataStr.length).split("\\,")

      if (rulename == "?????????" || rulename == "?????????") {
        rule3datalist += rule3datas{20}
      } else if (rulename == "?????????") {
        rule3datalist += rule3datas{21}
      } else if (rulename == "????????????") {
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
      // ???????????????LSL-CL?????????USL-CL???
      // USL-CL???
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
      // LSL-CL???
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
        if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "????????????") {
          spc8RuleList += "????????????"
        }
        countlist1.clear()
        countlist2.clear()
      }
    }
  }


  // SPC??????????????????6????????????????????????????????????
  def judgeSpc4Rule(spc8RuleList: ListBuffer[String], rule4datalist: ListBuffer[String],
                    groupDataList:ListBuffer[String], value: String, rulename: String, numpoint:Int): Unit = {

    val rule4dataIterator = groupDataList.subList(numpoint - 6, numpoint - 1).toIterator
    while (rule4dataIterator.hasNext) {
      val rule4dataStr = rule4dataIterator.next().toString()
      val rule4datas = rule4dataStr.substring(0, rule4dataStr.length).split("\\,")

      if (rulename == "?????????" || rulename == "?????????") {
        rule4datalist += rule4datas{20}
      } else if (rulename == "?????????") {
        rule4datalist += rule4datas{21}
      } else if (rulename == "????????????"){
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
        if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "?????????") {
          spc8RuleList += "?????????"
        } else if (rulename == "????????????") {
          spc8RuleList += "????????????"
        }
      }
    }
  }


}
