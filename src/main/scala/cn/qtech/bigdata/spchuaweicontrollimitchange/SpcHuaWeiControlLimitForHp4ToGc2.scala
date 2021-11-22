package cn.qtech.bigdata.reversecontrol

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import scala.collection.JavaConversions._
import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

object SpcHuaWeiControlLimitForHp4ToGc2{

  // 匹配浮点数
  private final val res = "[-+]?[0-9]*\\.?[0-9]+"
  private final val pattern = Pattern.compile(res)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
    //.setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "bigdata01,bigdata02,bigdata03"
    val kuduContext = new KuduContext(kuduMaster, sc)

    val tableName = "ADS_DATAJOINHUAWEIWB"
    val spcclresultlist = "ADS_SPCCONTROLLIMITFORHUAWEI"

    SpcCalculate(sparkSession: SparkSession, sc:
      SparkContext, kuduMaster: String, tableName: String, spcclresultlist: String)

    sc.stop()
  }

  private def SpcCalculate (sparkSession: SparkSession, sc:
  SparkContext, kuduMaster: String, tableName: String, spcclresultlist: String): Unit = {
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    val optionsclresult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcclresultlist
    )

    sparkSession.read.options(options).format("kudu").load
      .where("FIRST_TIME='巡检' and OP_CODE='WB' and WORKSHOP_CODE='生产四区' and FACTORY='汉浦厂'")
      .createOrReplaceTempView("temp1notfiltertime")

    sparkSession.read.options(optionsclresult).format("kudu").load
      .createOrReplaceTempView("getclresult")

    // 获取kudu中结果表里最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(time) from getclresult").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from temp1notfiltertime")
      .collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes {0}
    var timemin = "2021-01-01 00:00:00"
    var sourcetimemax = "2021-04-01 00:00:00"

    sparkSession.sql("select * from temp1notfiltertime")
      .where(s"mdate>'${timemin}' and mdate<='${sourcetimemax}'")
      .createOrReplaceTempView("temp1")

    // 分组查询,不包含流程卡号
    val groupData: DataFrame = sparkSession.sql(
      """
        |
        |    select a.lower,a.upper,a.tdata,a.rcard,a.id_no,a.mdate,a.FIRST_TIME,a.WORKSHOP_CODE,a.PART_SPEC,a.OP_CODE,a.STA_CODE,a.eattribute1_1,
        |    a.testitem, a.FACTORY,
        |    row_number() over(partition by a.FIRST_TIME,
        |    a.WORKSHOP_CODE,
        |    a.PART_SPEC,
        |    a.OP_CODE,
        |    a.STA_CODE,
        |    a.eattribute1_1,
        |    a.testitem,
        |    a.FACTORY
        |    order by a.mdate desc) as num
        |    from temp1 a
      """.stripMargin)

    val datalist = groupData.collect().toIterator

    // 该时间为spc控制限运行的时间
    val spcruntime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

    val integer = new AtomicInteger(0)

    // 创建List保存id_no数据和分组数据
    var idNoList = new ListBuffer[String]()
    var groupDataList = new ListBuffer[String]()

    // 遍历并查询数据
    while (datalist.hasNext) {
      val rowData = datalist.next()
      val dataStr: String = rowData.toString()
      val transfData: String = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = transfData.split("\\,")

      val lowerField = splitArr {0};
      val upperField = splitArr {1};
      val tdataField = splitArr {2};
      val rcardField = splitArr {3};
      val idNoField = splitArr {4};
      val mdateField = splitArr {5};
      val firstTimeField = splitArr {6};
      var workShopCodeField = splitArr {7};
      val partSpecField = splitArr {8};
      val opCodeField = splitArr {9};
      val staCodeField = splitArr {10};
      val eattribute1_1Field = splitArr {11};
      val testItemField = splitArr {12};
      var factoryField = splitArr {13};
      val numField = splitArr {14}.substring(0, splitArr{14}.length - 1);

      // 自增加1
      integer.getAndIncrement()

      if (integer.get() == numField.toInt) {

        // 不断把数据加入到集合中
        idNoList += idNoField
        groupDataList += transfData

      } else if (integer.get() > numField.toInt) {
        // 当达到临界值即有一条数据重新从1开始，获取到一个分组,触发计算并将数据插入kudu中
        // 一个分组截取结束，此时达到临界值触发计算
        if (idNoList.distinct.size >= 30) {

          // 创建集合保存分组数据
          var groupDataOnePcsList = new ListBuffer[String]()

          // 创建集合保存同一分组下不同送检单号的总值、抽检数、极差、标准差
          var sumAllIdNoList = new ListBuffer[Float]()
          var countAllIdNoList = new ListBuffer[Int]()
          var rangeAllIdNoList = new ListBuffer[Float]()
          var stddevAllIdNoList = new ListBuffer[Float]()

          // 创建集合保存同一分组下的tdata值
          var tdataGroupOneList = new ListBuffer[Float]()

          val groupIterator = groupDataList.toIterator

          // 遍历同一个分组中的数据（流程卡号和送检单号不同）
          while (groupIterator.hasNext) {
            val groupDataOne = groupIterator.next()
            val groupDataOneStr = groupDataOne.toString
            val groupDataOneArr: Array[String] = groupDataOneStr
              .substring(1, groupDataOneStr.length - 1).split("\\,")

            val tdataGroupOne = groupDataOneArr {2};
            val rcardGroupOne = groupDataOneArr {3};
            val idNoGroupOne = groupDataOneArr {4};
            val firstTimeGroupOne = groupDataOneArr {6};
            val workShopCodeGroupOne = groupDataOneArr {7};
            val partSpecGroupOne = groupDataOneArr {8};
            val opCodeGroupOne = groupDataOneArr {9};
            val staCodeGroupOne = groupDataOneArr {10};
            val eattribute1_1GroupOne = groupDataOneArr {11};
            val testItemGroupOne = groupDataOneArr {12};
            val factoryGroupOne = groupDataOneArr {13};

            // 获取当前循环的轮次,如果是最后一轮循环后面没有数据了，则触发计算
            var index = groupDataList.indexOf(groupDataOne) + 1

            groupDataOnePcsList += groupDataOneStr
            if (groupDataOnePcsList.size() >= 2) {
              // 获取集合中当前数据的上一条数据
              val groupLastData = groupDataOnePcsList
                .subList(groupDataOnePcsList.length - 2, groupDataOnePcsList.length - 1)
              val groupLastDataStr = groupLastData.toString
              val groupLastDataArr = groupLastDataStr.substring(1).split("\\,")

              val rcardGroupLast = groupLastDataArr {
                3
              };
              val idNoGroupLast = groupLastDataArr {
                4
              };
              val firstTimeGroupLast = groupLastDataArr {
                6
              };
              val workShopCodeGroupLast = groupLastDataArr {
                7
              };
              val partSpecGroupLast = groupLastDataArr {
                8
              };
              val opCodeGroupLast = groupLastDataArr {
                9
              };
              val staCodeGroupLast = groupLastDataArr {
                10
              };
              val eattribute1_1GroupLast = groupLastDataArr {
                11
              };
              val testItemGroupLast = groupLastDataArr {
                12
              };
              val factoryGroupLast = groupLastDataArr {
                13
              };

              // 如果当前数据的分组条件等于上一条数据的分组条件，保存分组条件数据到集合中
              if (rcardGroupOne == rcardGroupLast && idNoGroupOne == idNoGroupLast &&
                firstTimeGroupOne == firstTimeGroupLast && workShopCodeGroupOne == workShopCodeGroupLast &&
                partSpecGroupOne == partSpecGroupLast && opCodeGroupOne == opCodeGroupLast &&
                staCodeGroupOne == staCodeGroupLast && eattribute1_1GroupOne == eattribute1_1GroupLast &&
                testItemGroupOne == testItemGroupLast && factoryGroupOne == factoryGroupLast &&
                index != groupDataList.length) {

              } else {
                // 如果当前数据的分组条件不等于上一条数据的分组条件，触发计算上一条及更早的数据
                // 获取同一送检单号的多条数据并遍历
                val groupSameNoIterator: Iterator[String] = groupDataOnePcsList
                  .subList(0, groupDataOnePcsList.length - 1).toIterator

                // 创建集合保存同一idno下的tdata值
                var tdataSameNoList = new ListBuffer[Float]()

                while (groupSameNoIterator.hasNext) {
                  // 获取同一idno的一条数据
                  val groupSameNoStr: String = groupSameNoIterator.next().toString
                  val groupSameNoArr: Array[String] = groupSameNoStr.split("\\,")

                  // 获取同一idno中的一个tdata
                  var tdataSameNo = groupSameNoArr {2}

                  if (tdataSameNo != "null") {
                    // TDATA中连续的数字用分号分割
                    if (tdataSameNo.contains(";")) {
                      val tdataResults = tdataSameNo.split(";").toIterator
                      while (tdataResults.hasNext) {
                        // 将数据加入到集合中
                        val tdataDatass = tdataResults.next()
                        if (pattern.matcher(s"$tdataDatass").matches()) {
                          tdataSameNoList += tdataDatass.toFloat
                        }
                      }
                    } else if (pattern.matcher(s"$tdataSameNo").matches()) {
                      // 如果tdata匹配的值为浮点数，直接将数据插入到集合中
                      tdataSameNoList += tdataSameNo.toFloat
                    }
                  }
                }

                if (tdataSameNoList.nonEmpty) {

                  // 获取一个idno的总值
                  val oneIdNoSumValue = tdataSameNoList.sum
                  sumAllIdNoList += oneIdNoSumValue

                  // 获取一个idno的抽检数
                  val oneIdNoCount = tdataSameNoList.size
                  countAllIdNoList += oneIdNoCount

                  // 获取一个idno的最大最小值
                  val oneIdNoMaxValue = tdataSameNoList.max
                  val oneIdNoMinValue = tdataSameNoList.min
                  // 计算单个idno的极差
                  var oneIdNoValueR = oneIdNoMaxValue - oneIdNoMinValue

                  // 当pcs为1时，极差值取上一个单号的值和当前值进行计算
                  if (oneIdNoCount == 1) {

                    if (tdataGroupOne != "null") {
                      //TDATA中连续的数字用分号分割
                      if (tdataGroupOne.contains(";")) {
                        val tdataResults = tdataGroupOne.split(";").toIterator
                        while (tdataResults.hasNext) {
                          //将数据加入到集合中
                          val tdataDatasOne = tdataResults.next()
                          if (pattern.matcher(s"$tdataDatasOne").matches()) {
                            tdataGroupOneList += tdataDatasOne.toFloat
                          }
                        }
                      } else if (pattern.matcher(s"$tdataGroupOne").matches()) {
                        //如果tdata匹配的值为浮点数，直接将数据插入到集合中
                        tdataGroupOneList += tdataGroupOne.toFloat
                      }
                    }
                    oneIdNoValueR = (oneIdNoSumValue - tdataGroupOneList.sum).abs
                  }

                  rangeAllIdNoList += oneIdNoValueR

                  // 计算单个idno的标准差
                  val oneInspecNoStddev = sc.parallelize(tdataSameNoList).stdev()
                  stddevAllIdNoList += oneInspecNoStddev.toFloat

                  // 清空集合中上一条及更早的分组条件数据（包括idno）
                  tdataSameNoList.clear()
                  tdataGroupOneList.clear()
                  groupDataOnePcsList.remove(0, groupDataOnePcsList.length - 1)
                }
              }
            }
          }

          // 获取第1条分组条件
          val groupDataFirst = groupDataList.subList(0, 1)
          val groupDataFirstStr = groupDataFirst.toString
          val groupDatasFirstArr = groupDataFirstStr.substring(1).split("\\,")
          val lower = groupDatasFirstArr {
            0
          };
          val upper = groupDatasFirstArr {
            1
          };

          val rcard = groupDatasFirstArr {
            3
          };
          val idNo = groupDatasFirstArr {
            4
          };
          val firstTime = groupDatasFirstArr {
            6
          };
          var workShopCode = groupDatasFirstArr {
            7
          };
          val partSpec = groupDatasFirstArr {
            8
          };
          val opCode = groupDatasFirstArr {
            9
          };
          var staCode = groupDatasFirstArr {
            10
          };
          val eattribute1_1 = groupDatasFirstArr {
            11
          };
          val testItem = groupDatasFirstArr {
            12
          };
          var factory = groupDatasFirstArr {
            13
          };

          // 在此处进行新旧自编码的映射
          if (staCode == "WB01" || staCode == "WB-03-0226") {
            staCode = "GC2WB-19001"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB02" || staCode == "WB-03-0227") {
            staCode = "GC2WB-19002"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB03" || staCode == "WB-03-0228") {
            staCode = "GC2WB-19003"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB04" || staCode == "WB-03-0229") {
            staCode = "GC2WB-19004"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB05" || staCode == "WB-03-0230") {
            staCode = "GC2WB-19005"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB06" || staCode == "WB-03-0231") {
            staCode = "GC2WB-19006"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB07" || staCode == "WB-03-0248") {
            staCode = "HC4WB-00007"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB08" || staCode == "WB-03-0261") {
            staCode = "HC4WB-00008"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB09" || staCode == "WB-03-0250") {
            staCode = "HC4WB-00009"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB10" || staCode == "WB-03-0255") {
            staCode = "HC4WB-00010"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB11" || staCode == "WB-03-0254") {
            staCode = "HC4WB-00011"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB12" || staCode == "WB-03-0253") {
            staCode = "HC4WB-00012"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB13" || staCode == "WB-03-0252") {
            staCode = "HC4WB-00013"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB14" || staCode == "WB-03-0251") {
            staCode = "HC4WB-00014"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB15" || staCode == "WB-03-0249") {
            staCode = "HC4WB-00015"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB16" || staCode == "WB-03-0256") {
            staCode = "HC4WB-00016"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB17" || staCode == "WB-03-0257") {
            staCode = "HC4WB-00017"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB18" || staCode == "WB-03-0262") {
            staCode = "HC4WB-00018"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB19" || staCode == "WB-03-0243") {
            staCode = "HC4WB-00019"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB20" || staCode == "WB-03-0242") {
            staCode = "HC4WB-00020"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB21" || staCode == "WB-03-0241") {
            staCode = "HC4WB-00021"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB22" || staCode == "WB-03-0258") {
            staCode = "HC4WB-00022"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB23" || staCode == "WB-03-0259") {
            staCode = "HC4WB-00023"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB24" || staCode == "WB-03-0260") {
            staCode = "HC4WB-00024"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB25" || staCode == "WB-03-0265") {
            staCode = "HC4WB-00025"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB26" || staCode == "WB-03-0264") {
            staCode = "HC4WB-00026"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB27" || staCode == "WB-03-0263") {
            staCode = "HC4WB-00027"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB28" || staCode == "WB-03-0244") {
            staCode = "HC4WB-00028"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB29" || staCode == "WB-03-0245") {
            staCode = "HC4WB-00029"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB30" || staCode == "WB-03-0246") {
            staCode = "HC4WB-00030"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB31" || staCode == "WB-03-0247") {
            staCode = "HC4WB-00031"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB32" || staCode == "WB-03-0240") {
            staCode = "HC4WB-00032"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB33" || staCode == "WB-03-0239") {
            staCode = "HC4WB-00033"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB34" || staCode == "WB-03-0238") {
            staCode = "HC4WB-00034"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB35" || staCode == "WB-03-0237") {
            staCode = "HC4WB-00035"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB36" || staCode == "WB-03-0236") {
            staCode = "HC4WB-00036"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB37" || staCode == "WB-03-0235") {
            staCode = "HC4WB-00037"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB38" || staCode == "WB-03-0211") {
            staCode = "GC2WB-20001"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB39" || staCode == "WB-03-0212") {
            staCode = "GC2WB-20002"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB40" || staCode == "WB-03-0213") {
            staCode = "GC2WB-20003"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB41" || staCode == "WB-03-0214") {
            staCode = "GC2WB-20004"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB42" || staCode == "WB-03-0215") {
            staCode = "GC2WB-20005"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB43" || staCode == "WB-03-0216") {
            staCode = "GC2WB-20006"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB44" || staCode == "WB-03-0220") {
            staCode = "GC2WB-11001"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB45" || staCode == "WB-03-0221") {
            staCode = "GC2WB-11002"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB46" || staCode == "WB-03-0222") {
            staCode = "GC2WB-11003"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB47" || staCode == "WB-03-0223") {
            staCode = "GC2WB-11004"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB48" || staCode == "WB-03-0224") {
            staCode = "GC2WB-11005"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB49" || staCode == "WB-03-0225") {
            staCode = "GC2WB-11006"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB50" || staCode == "WB-03-0232") {
            staCode = "GC2WB-12001"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB51" || staCode == "WB-03-0233") {
            staCode = "GC2WB-12002"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB52" || staCode == "WB-03-0234") {
            staCode = "GC2WB-12003"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB53" || staCode == "WB-03-0217") {
            staCode = "GC2WB-12004"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB54" || staCode == "WB-03-0218") {
            staCode = "GC2WB-12005"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB55" || staCode == "WB-03-0219") {
            staCode = "GC2WB-12006"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB56" || staCode == "WB-03-0267") {
            staCode = "GC2WB-11007"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB57" || staCode == "WB-03-0266") {
            staCode = "GC2WB-12007"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB58" || staCode == "WB-03-0109") {
            staCode = "HC4WB-00058"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB59" || staCode == "WB-03-0108") {
            staCode = "HC4WB-00059"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB60" || staCode == "WB-03-0110") {
            staCode = "HC4WB-00060"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          } else if (staCode == "WB61" || staCode == "WB-03-0107") {
            staCode = "HC4WB-00061"
            workShopCode = "古二COB二区"
            factory = "古城厂"
          }

          if (sumAllIdNoList.nonEmpty && sumAllIdNoList.length >= 30) {
            // 获取到同一分组(送检单号不同)下的各送检单号求出的总值、抽检数、极差、标准差
            val sumAll = sumAllIdNoList.subList(sumAllIdNoList.length - 30, sumAllIdNoList.length).sum
            val countAllList: util.List[Int] = countAllIdNoList.subList(countAllIdNoList.length - 30, countAllIdNoList.length)
            val countAll = countAllList.sum

            // 计算30个送检单号的xbar
            val xbarsum = sumAll / countAll
            // 计算30个送检单号的极差
            val rangeAll = rangeAllIdNoList.subList(rangeAllIdNoList.length - 30, rangeAllIdNoList.length).sum / 30
            // 计算30个送检单号的标准差
            val stdevAll = stddevAllIdNoList.subList(stddevAllIdNoList.length - 30, stddevAllIdNoList.length).sum / 30

            var pcsByTdataPcsCount = 1

            // 华为数据抽检数只有1个点，只计算单值和极差图的上下限
            var uslvalue = "null"
            var lslvalue = "null"
            if (upper != "null") {
              uslvalue = upper.toFloat.formatted("%.3f");
            }
            if (lower != "null") {
              lslvalue = lower.toFloat.formatted("%.3f");
            }
            val value1Ucl = (xbarsum + 2.660 * rangeAll).formatted("%.3f")
            val value1Lcl = (xbarsum - 2.660 * rangeAll).formatted("%.3f")
            val range1Ucl = (3.267 * rangeAll).formatted("%.3f")
            val range1Lcl = (0 * rangeAll).formatted("%.3f")

            upsertData(kuduMaster, spcclresultlist, s"${firstTime}", s"${workShopCode}",
              s"${partSpec}", s"${opCode}", s"${staCode}",
              s"${eattribute1_1}", s"${testItem}", s"${factory}", spcruntime, pcsByTdataPcsCount, s"${upper}",
              s"${lower}", value1Ucl, value1Lcl, range1Ucl, range1Lcl, "", "")

            println("1个点的双边" + value1Ucl + "====" + value1Lcl + "====" + range1Ucl + "====" + range1Lcl)

            sumAllIdNoList.clear()
            countAllIdNoList.clear()
            rangeAllIdNoList.clear()
            stddevAllIdNoList.clear()
          }
        }

        //println("一个分组结束，触发计算")
        // 清空当前集合
        idNoList.clear()
        groupDataList.clear()
        // 将自增数为1时的数据插入到集合中
        idNoList += idNoField
        groupDataList += transfData
        // 将数字初始值置为1
        integer.set(1)
      }
    }
  }

  def upsertData(kuduMaster: String, spcclresultlist: String, firsttime:String, workshopcode: String, partspec: String,
                 opcode: String, stacode: String, eattribute11:String, testitem: String, factory:String, time: String,
                 pcs: Int, usl :String, lsl: String, avgucl: String, avglcl: String, rangeucl: String, rangelcl: String,
                 stddevucl: String, stddevlcl: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spcclresultlist)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val upsert = kuduTable.newUpsert()
    val rowUpsert = upsert.getRow()

    rowUpsert.addString("FIRST_TIME", s"${firsttime}")
    rowUpsert.addString("WORKSHOP_CODE", s"${workshopcode}")
    rowUpsert.addString("PART_SPEC", s"${partspec}")
    rowUpsert.addString("OP_CODE", s"${opcode}")
    rowUpsert.addString("STA_CODE", s"${stacode}")
    rowUpsert.addString("EATTRIBUTE1_1", s"${eattribute11}")
    rowUpsert.addString("TESTITEM", s"${testitem}")
    rowUpsert.addString("FACTORY", s"${factory}")
    rowUpsert.addString("TIME", s"${time}")
    rowUpsert.addInt("PCS", pcs)
    rowUpsert.addString("USL", s"${usl}")
    rowUpsert.addString("LSL", s"${lsl}")
    rowUpsert.addString("AVGUCL", s"${avgucl}")
    rowUpsert.addString("AVGLCL", s"${avglcl}")
    rowUpsert.addString("RANGEUCL", s"${rangeucl}")
    rowUpsert.addString("RANGELCL", s"${rangelcl}")
    rowUpsert.addString("STDDEVUCL", s"${stddevucl}")
    rowUpsert.addString("STDDEVLCL", s"${stddevlcl}")

    // 执行upsert操作
    session.apply(upsert)

    session.flush()
    session.close()
    kuduClient.close()

  }


}

