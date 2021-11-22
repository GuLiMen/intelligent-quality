package cn.qtech.bigdata.spccontrollimitchange

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

object SpcMesControlLimitForGcCob1 {

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

    val tableName = "ADS_INSPECTIONPASSRATE"
    val spcclresultlist = "ADS_SPCMESCONTROLLIMIT"

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
      .where("FIRST_TIME='巡检' and OP_CODE='WB' and WORKSHOP_CODE='古城COB一区' and FACTORY='古城厂'")
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
      val workShopCodeField = splitArr {7};
      val partSpecField = splitArr {8};
      val opCodeField = splitArr {9};
      val staCodeField = splitArr {10};
      val eattribute1_1Field = splitArr {11};
      val testItemField = splitArr {12};
      val factoryField = splitArr {13};
      val numField = splitArr {14}.substring(0, splitArr {14}.length - 1);

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
                      // TDATA中连续的数字用分号分割
                      if (tdataGroupOne.contains(";")) {
                        val tdataResults = tdataGroupOne.split(";").toIterator
                        while (tdataResults.hasNext) {
                          // 将数据加入到集合中
                          val tdataDatasOne = tdataResults.next()
                          if (pattern.matcher(s"$tdataDatasOne").matches()) {
                            tdataGroupOneList += tdataDatasOne.toFloat
                          }
                        }
                      } else if (pattern.matcher(s"$tdataGroupOne").matches()) {
                        // 如果tdata匹配的值为浮点数，直接将数据插入到集合中
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
          val workShopCode = groupDatasFirstArr {
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
          val factory = groupDatasFirstArr {
            13
          };

          // 在此处进行新旧自编码的映射
          if (staCode == "WB011") {
            staCode = "GC1WB-01001"
          } else if (staCode == "WB012") {
            staCode = "GC1WB-01002"
          } else if (staCode == "WB013") {
            staCode = "GC1WB-01003"
          } else if (staCode == "WB014") {
            staCode = "GC1WB-01004"
          } else if (staCode == "WB015") {
            staCode = "GC1WB-01005"
          } else if (staCode == "WB016") {
            staCode = "GC1WB-01006"
          } else if (staCode == "WB017") {
            staCode = "GC1WB-01007"
          } else if (staCode == "WB018") {
            staCode = "GC1WB-01008"
          } else if (staCode == "WB021") {
            staCode = "GC1WB-02001"
          } else if (staCode == "WB022") {
            staCode = "GC1WB-02002"
          } else if (staCode == "WB023") {
            staCode = "GC1WB-02003"
          } else if (staCode == "WB024") {
            staCode = "GC1WB-02004"
          } else if (staCode == "WB025") {
            staCode = "GC1WB-02005"
          } else if (staCode == "WB026") {
            staCode = "GC1WB-02006"
          } else if (staCode == "WB027") {
            staCode = "GC1WB-02007"
          } else if (staCode == "WB028") {
            staCode = "GC1WB-02008"
          } else if (staCode == "WB031") {
            staCode = "GC1WB-03001"
          } else if (staCode == "WB032") {
            staCode = "GC1WB-03002"
          } else if (staCode == "WB033") {
            staCode = "GC1WB-03003"
          } else if (staCode == "WB034") {
            staCode = "GC1WB-03004"
          } else if (staCode == "WB035") {
            staCode = "GC1WB-03005"
          } else if (staCode == "WB036") {
            staCode = "GC1WB-03006"
          } else if (staCode == "WB037") {
            staCode = "GC1WB-03007"
          } else if (staCode == "WB038") {
            staCode = "GC1WB-03008"
          } else if (staCode == "WB041") {
            staCode = "GC1WB-04001"
          } else if (staCode == "WB042") {
            staCode = "GC1WB-04002"
          } else if (staCode == "WB043") {
            staCode = "GC1WB-04003"
          } else if (staCode == "WB044") {
            staCode = "GC1WB-04004"
          } else if (staCode == "WB045") {
            staCode = "GC1WB-04005"
          } else if (staCode == "WB046") {
            staCode = "GC1WB-04006"
          } else if (staCode == "WB051") {
            staCode = "GC1WB-05001"
          } else if (staCode == "WB052") {
            staCode = "GC1WB-05002"
          } else if (staCode == "WB053") {
            staCode = "GC1WB-05003"
          } else if (staCode == "WB054") {
            staCode = "GC1WB-05004"
          } else if (staCode == "WB055") {
            staCode = "GC1WB-05005"
          } else if (staCode == "WB056") {
            staCode = "GC1WB-05006"
          } else if (staCode == "WB061") {
            staCode = "GC1WB-06001"
          } else if (staCode == "WB062") {
            staCode = "GC1WB-06002"
          } else if (staCode == "WB063") {
            staCode = "GC1WB-06003"
          } else if (staCode == "WB064") {
            staCode = "GC1WB-06004"
          } else if (staCode == "WB065") {
            staCode = "GC1WB-06005"
          } else if (staCode == "WB066") {
            staCode = "GC1WB-06006"
          } else if (staCode == "WB071") {
            staCode = "GC1WB-07001"
          } else if (staCode == "WB072") {
            staCode = "GC1WB-07002"
          } else if (staCode == "WB073") {
            staCode = "GC1WB-07003"
          } else if (staCode == "WB074") {
            staCode = "GC1WB-07004"
          } else if (staCode == "WB075") {
            staCode = "GC1WB-07005"
          } else if (staCode == "WB076") {
            staCode = "GC1WB-07006"
          } else if (staCode == "WB081") {
            staCode = "GC1WB-08001"
          } else if (staCode == "WB082") {
            staCode = "GC1WB-08002"
          } else if (staCode == "WB083") {
            staCode = "GC1WB-08003"
          } else if (staCode == "WB084") {
            staCode = "GC1WB-08004"
          } else if (staCode == "WB085") {
            staCode = "GC1WB-08005"
          } else if (staCode == "WB086") {
            staCode = "GC1WB-08006"
          } else if (staCode == "WB091") {
            staCode = "GC1WB-09001"
          } else if (staCode == "WB092") {
            staCode = "GC1WB-09002"
          } else if (staCode == "WB093") {
            staCode = "GC1WB-09003"
          } else if (staCode == "WB094") {
            staCode = "GC1WB-09004"
          } else if (staCode == "WB095") {
            staCode = "GC1WB-09005"
          } else if (staCode == "WB096") {
            staCode = "GC1WB-09006"
          } else if (staCode == "WB101") {
            staCode = "GC1WB-10001"
          } else if (staCode == "WB102") {
            staCode = "GC1WB-10002"
          } else if (staCode == "WB103") {
            staCode = "GC1WB-10003"
          } else if (staCode == "WB104") {
            staCode = "GC1WB-10004"
          } else if (staCode == "WB105") {
            staCode = "GC1WB-10005"
          } else if (staCode == "WB106") {
            staCode = "GC1WB-10006"
          } else if (staCode == "WB111") {
            staCode = "GC1WB-11001"
          } else if (staCode == "WB112") {
            staCode = "GC1WB-11002"
          } else if (staCode == "WB113") {
            staCode = "GC1WB-11003"
          } else if (staCode == "WB114") {
            staCode = "GC1WB-11004"
          } else if (staCode == "WB115") {
            staCode = "GC1WB-11005"
          } else if (staCode == "WB116") {
            staCode = "GC1WB-11006"
          } else if (staCode == "WB121") {
            staCode = "GC1WB-12001"
          } else if (staCode == "WB122") {
            staCode = "GC1WB-12002"
          } else if (staCode == "WB123") {
            staCode = "GC1WB-12003"
          } else if (staCode == "WB124") {
            staCode = "GC1WB-12004"
          } else if (staCode == "WB125") {
            staCode = "GC1WB-12005"
          } else if (staCode == "WB126") {
            staCode = "GC1WB-12006"
          } else if (staCode == "WB131") {
            staCode = "GC1WB-13001"
          } else if (staCode == "WB132") {
            staCode = "GC1WB-13002"
          } else if (staCode == "WB133") {
            staCode = "GC1WB-13003"
          } else if (staCode == "WB134") {
            staCode = "GC1WB-13004"
          } else if (staCode == "WB135") {
            staCode = "GC1WB-13005"
          } else if (staCode == "WB136") {
            staCode = "GC1WB-13006"
          } else if (staCode == "WB137") {
            staCode = "GC1WB-13007"
          } else if (staCode == "WB138") {
            staCode = "GC1WB-13008"
          } else if (staCode == "WB141") {
            staCode = "GC1WB-14001"
          } else if (staCode == "WB142") {
            staCode = "GC1WB-14002"
          } else if (staCode == "WB143") {
            staCode = "GC1WB-14003"
          } else if (staCode == "WB144") {
            staCode = "GC1WB-14004"
          } else if (staCode == "WB145") {
            staCode = "GC1WB-14005"
          } else if (staCode == "WB146") {
            staCode = "GC1WB-14006"
          } else if (staCode == "WB147") {
            staCode = "GC1WB-14007"
          } else if (staCode == "WB148") {
            staCode = "GC1WB-14008"
          } else if (staCode == "WB151") {
            staCode = "GC1WB-15001"
          } else if (staCode == "WB152") {
            staCode = "GC1WB-15002"
          } else if (staCode == "WB153") {
            staCode = "GC1WB-15003"
          } else if (staCode == "WB154") {
            staCode = "GC1WB-15004"
          } else if (staCode == "WB155") {
            staCode = "GC1WB-15005"
          } else if (staCode == "WB156") {
            staCode = "GC1WB-15006"
          } else if (staCode == "WB161") {
            staCode = "GC1WB-16001"
          } else if (staCode == "WB162") {
            staCode = "GC1WB-16002"
          } else if (staCode == "WB163") {
            staCode = "GC1WB-16003"
          } else if (staCode == "WB164") {
            staCode = "GC1WB-16004"
          } else if (staCode == "WB165") {
            staCode = "GC1WB-16005"
          } else if (staCode == "WB166") {
            staCode = "GC1WB-16006"
          } else if (staCode == "WB167") {
            staCode = "GC1WB-16007"
          } else if (staCode == "WB168") {
            staCode = "GC1WB-16008"
          } else if (staCode == "WB171") {
            staCode = "GC1WB-17001"
          } else if (staCode == "WB172") {
            staCode = "GC1WB-17002"
          } else if (staCode == "WB173") {
            staCode = "GC1WB-17003"
          } else if (staCode == "WB174") {
            staCode = "GC1WB-17004"
          } else if (staCode == "WB175") {
            staCode = "GC1WB-17005"
          } else if (staCode == "WB176") {
            staCode = "GC1WB-17006"
          } else if (staCode == "WB181") {
            staCode = "GC1WB-18001"
          } else if (staCode == "WB182") {
            staCode = "GC1WB-18002"
          } else if (staCode == "WB183") {
            staCode = "GC1WB-18003"
          } else if (staCode == "WB184") {
            staCode = "GC1WB-18004"
          } else if (staCode == "WB185") {
            staCode = "GC1WB-18005"
          } else if (staCode == "WB186") {
            staCode = "GC1WB-18006"
          } else if (staCode == "WB191") {
            staCode = "GC1WB-19001"
          } else if (staCode == "WB192") {
            staCode = "GC1WB-19002"
          } else if (staCode == "WB193") {
            staCode = "GC1WB-19003"
          } else if (staCode == "WB194") {
            staCode = "GC1WB-19004"
          } else if (staCode == "WB195") {
            staCode = "GC1WB-19005"
          } else if (staCode == "WB196") {
            staCode = "GC1WB-19006"
          }

          if (sumAllIdNoList.nonEmpty && sumAllIdNoList.length >= 30) {
            // 获取到同一分组(送检单号不同)下的各送检单号求出的总值、抽检数、极差、标准差
            val sumAll = sumAllIdNoList.subList(sumAllIdNoList.length - 30, sumAllIdNoList.length).sum
            val countAllList: util.List[Int] = countAllIdNoList
              .subList(countAllIdNoList.length - 30, countAllIdNoList.length)
            val countAll = countAllList.sum

            // 计算30个送检单号的xbar
            val xbarsum = sumAll / countAll
            // 计算30个送检单号的极差
            val rangeAll = rangeAllIdNoList.subList(rangeAllIdNoList.length - 30, rangeAllIdNoList.length).sum / 30
            // 计算30个送检单号的标准差
            val stdevAll = stddevAllIdNoList.subList(stddevAllIdNoList.length - 30, stddevAllIdNoList.length).sum / 30

            var pcsByTdataPcsCount = 0
            if (countAllList.nonEmpty) {
              // 获取抽样数中出现频率最高的数作为抽样值
              pcsByTdataPcsCount = sc.makeRDD(countAllList)
                .flatMap(_.toString.split("\\,"))
                .map((_, 1))
                .reduceByKey(_ + _)
                .sortBy(_._2, false)
                .collect().toList.toString.substring(6).split("\\,") {
                0
              }
                .toInt
            }

            // 1个点，单值和极差图的上下限
            if (pcsByTdataPcsCount == 1) {
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

              // 2到5个点，平均值图和极差图的上下限
            } else if (pcsByTdataPcsCount >= 2 && pcsByTdataPcsCount <= 5) {

              var a2constant: Double = 0
              if (pcsByTdataPcsCount == 2) {
                a2constant = 1.880
              } else if (pcsByTdataPcsCount == 3) {
                a2constant = 1.023
              } else if (pcsByTdataPcsCount == 4) {
                a2constant = 0.729
              } else if (pcsByTdataPcsCount == 5) {
                a2constant = 0.577
              }

              var d4constant: Double = 0
              if (pcsByTdataPcsCount == 2) {
                d4constant = 3.267
              } else if (pcsByTdataPcsCount == 3) {
                d4constant = 2.574
              } else if (pcsByTdataPcsCount == 4) {
                d4constant = 2.282
              } else if (pcsByTdataPcsCount == 5) {
                d4constant = 2.114
              }

              var uslvalue = "null"
              var lslvalue = "null"
              if (upper != "null") {
                uslvalue = upper.toFloat.formatted("%.3f");
              }
              if (lower != "null") {
                lslvalue = lower.toFloat.formatted("%.3f");
              }
              val value2to5Ucl = (xbarsum + a2constant * rangeAll).formatted("%.3f")
              val value2to5Lcl = (xbarsum - a2constant * rangeAll).formatted("%.3f")
              val range2to5Ucl = (d4constant * rangeAll).formatted("%.3f")
              val range2to5Lcl = (0 * rangeAll).formatted("%.3f")

              upsertData(kuduMaster, spcclresultlist, s"${firstTime}", s"${workShopCode}",
                s"${partSpec}", s"${opCode}", s"${staCode}",
                s"${eattribute1_1}", s"${testItem}", s"${factory}", spcruntime, pcsByTdataPcsCount, s"${upper}",
                s"${lower}", value2to5Ucl, value2to5Lcl, range2to5Ucl, range2to5Lcl, "", "")

              println("2到5个点的双边" + value2to5Ucl + "====" + value2to5Lcl + "====" + range2to5Ucl + "====" + range2to5Lcl)

              // 6个点到25个点，平均值图和标准差图的上下限
            } else if (pcsByTdataPcsCount >= 6 && pcsByTdataPcsCount <= 25) {

              var a3constant: Double = 0
              if (pcsByTdataPcsCount == 6) {
                a3constant = 1.287
              } else if (pcsByTdataPcsCount == 7) {
                a3constant = 1.182
              } else if (pcsByTdataPcsCount == 8) {
                a3constant = 1.099
              } else if (pcsByTdataPcsCount == 9) {
                a3constant = 1.032
              } else if (pcsByTdataPcsCount == 10) {
                a3constant = 0.975
              } else if (pcsByTdataPcsCount == 11) {
                a3constant = 0.927
              } else if (pcsByTdataPcsCount == 12) {
                a3constant = 0.886
              } else if (pcsByTdataPcsCount == 13) {
                a3constant = 0.850
              } else if (pcsByTdataPcsCount == 14) {
                a3constant = 0.817
              } else if (pcsByTdataPcsCount == 15) {
                a3constant = 0.789
              } else if (pcsByTdataPcsCount == 16) {
                a3constant = 0.763
              } else if (pcsByTdataPcsCount == 17) {
                a3constant = 0.739
              } else if (pcsByTdataPcsCount == 18) {
                a3constant = 0.718
              } else if (pcsByTdataPcsCount == 19) {
                a3constant = 0.698
              } else if (pcsByTdataPcsCount == 20) {
                a3constant = 0.680
              } else if (pcsByTdataPcsCount == 21) {
                a3constant = 0.663
              } else if (pcsByTdataPcsCount == 22) {
                a3constant = 0.647
              } else if (pcsByTdataPcsCount == 23) {
                a3constant = 0.633
              } else if (pcsByTdataPcsCount == 24) {
                a3constant = 0.619
              } else if (pcsByTdataPcsCount == 25) {
                a3constant = 0.606
              }

              var b4constant: Double = 0
              if (pcsByTdataPcsCount == 6) {
                b4constant = 1.970
              } else if (pcsByTdataPcsCount == 7) {
                b4constant = 1.882
              } else if (pcsByTdataPcsCount == 8) {
                b4constant = 1.815
              } else if (pcsByTdataPcsCount == 9) {
                b4constant = 1.761
              } else if (pcsByTdataPcsCount == 10) {
                b4constant = 1.716
              } else if (pcsByTdataPcsCount == 11) {
                b4constant = 1.679
              } else if (pcsByTdataPcsCount == 12) {
                b4constant = 1.646
              } else if (pcsByTdataPcsCount == 13) {
                b4constant = 1.618
              } else if (pcsByTdataPcsCount == 14) {
                b4constant = 1.594
              } else if (pcsByTdataPcsCount == 15) {
                b4constant = 1.572
              } else if (pcsByTdataPcsCount == 16) {
                b4constant = 1.552
              } else if (pcsByTdataPcsCount == 17) {
                b4constant = 1.534
              } else if (pcsByTdataPcsCount == 18) {
                b4constant = 1.518
              } else if (pcsByTdataPcsCount == 19) {
                b4constant = 1.503
              } else if (pcsByTdataPcsCount == 20) {
                b4constant = 1.490
              } else if (pcsByTdataPcsCount == 21) {
                b4constant = 1.477
              } else if (pcsByTdataPcsCount == 22) {
                b4constant = 1.466
              } else if (pcsByTdataPcsCount == 23) {
                b4constant = 1.455
              } else if (pcsByTdataPcsCount == 24) {
                b4constant = 1.445
              } else if (pcsByTdataPcsCount == 25) {
                b4constant = 1.435
              }

              var b3constant: Double = 0
              if (pcsByTdataPcsCount == 6) {
                b3constant = 0.030
              } else if (pcsByTdataPcsCount == 7) {
                b3constant = 0.118
              } else if (pcsByTdataPcsCount == 8) {
                b3constant = 0.185
              } else if (pcsByTdataPcsCount == 9) {
                b3constant = 0.239
              } else if (pcsByTdataPcsCount == 10) {
                b3constant = 0.284
              } else if (pcsByTdataPcsCount == 11) {
                b3constant = 0.321
              } else if (pcsByTdataPcsCount == 12) {
                b3constant = 0.354
              } else if (pcsByTdataPcsCount == 13) {
                b3constant = 0.382
              } else if (pcsByTdataPcsCount == 14) {
                b3constant = 0.406
              } else if (pcsByTdataPcsCount == 15) {
                b3constant = 0.428
              } else if (pcsByTdataPcsCount == 16) {
                b3constant = 0.448
              } else if (pcsByTdataPcsCount == 17) {
                b3constant = 0.466
              } else if (pcsByTdataPcsCount == 18) {
                b3constant = 0.482
              } else if (pcsByTdataPcsCount == 19) {
                b3constant = 0.497
              } else if (pcsByTdataPcsCount == 20) {
                b3constant = 0.510
              } else if (pcsByTdataPcsCount == 21) {
                b3constant = 0.523
              } else if (pcsByTdataPcsCount == 22) {
                b3constant = 0.534
              } else if (pcsByTdataPcsCount == 23) {
                b3constant = 0.545
              } else if (pcsByTdataPcsCount == 24) {
                b3constant = 0.555
              } else if (pcsByTdataPcsCount == 25) {
                b3constant = 0.565
              }

              var uslvalue = "null"
              var lslvalue = "null"
              if (upper != "null") {
                uslvalue = upper.toFloat.formatted("%.3f");
              }
              if (lower != "null") {
                lslvalue = lower.toFloat.formatted("%.3f");
              }

              val value6Ucl = (xbarsum + a3constant * stdevAll).formatted("%.3f")
              val value6Lcl = (xbarsum - a3constant * stdevAll).formatted("%.3f")
              val stddev6Ucl = (b4constant * stdevAll).formatted("%.3f")
              val stddev6Lcl = (b3constant * stdevAll).formatted("%.3f")

              upsertData(kuduMaster, spcclresultlist, s"${firstTime}", s"${workShopCode}",
                s"${partSpec}", s"${opCode}", s"${staCode}",
                s"${eattribute1_1}", s"${testItem}", s"${factory}", spcruntime, pcsByTdataPcsCount, s"${upper}",
                s"${lower}", value6Ucl, value6Lcl, "", "", stddev6Ucl, stddev6Lcl)

              println("6个点的双边" + value6Ucl + "====" + value6Lcl + "====" + stddev6Ucl + "====" + stddev6Lcl)

            }
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
                 pcs: Int, usl :String, lsl: String, avgucl: String, avglcl: String, rangeucl: String,
                 rangelcl: String, stddevucl: String, stddevlcl: String): Unit = {
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
