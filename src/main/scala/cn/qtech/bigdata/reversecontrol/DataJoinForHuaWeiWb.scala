package cn.qtech.bigdata.reversecontrol

import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.util.UUID

object DataJoinForHuaWeiWb {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu").set("spark.debug.maxToStringFields", "100")
    //.setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).config("spark.sql.crossJoin.enabled","true").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "10.170.3.11,10.170.3.12,10.170.3.13"

    val tableName1 = "MESC_TEST_OQC_DATA_DETAIL"
    val tableName2 = "MESC_TEST_OQC_DATA"
    val resultTable = "ADS_DATAJOINHUAWEIWB"

    processData(sparkSession, sc, kuduMaster, tableName1, tableName2, resultTable)

    sc.stop()
  }

  private def processData(sparkSession: SparkSession, sc:
  SparkContext, kuduMaster: String, tableName1: String, tableName2: String, resultTable: String): Unit = {
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

    sparkSession.read.options(option1).format("kudu").load.createOrReplaceTempView("temp1")
    sparkSession.read.options(option2).format("kudu").load
      .where("TESTSTATION='WB' and TESTTYPE='巡检'")
      .createOrReplaceTempView("temp2notfiltertime")
    sparkSession.read.options(optionResult).format("kudu").load
      .createOrReplaceTempView("resultTable")

    // 获取kudu中结果表最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(mdate) from resultTable").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    var sourcetimelists = sparkSession.sql("select min(TESTTIME),max(TESTTIME) from temp2notfiltertime")
      .collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes{0}
    var sourcetimemax = sourcetimes{1}
    var timemin = "2021-01-01 00:00:00"

    if (kudutimes != "null") {
      timemin = kudutimes
    }

    sparkSession.sql("select * from temp2notfiltertime")
      .where(s"TESTTIME>'${timemin}' and TESTTIME<='${sourcetimemax}'")
      .createOrReplaceTempView("temp2")

    // 对华为数据的两张源表进行关联
    val datas = sparkSession.sql(
      """
        |
        |SELECT temp2.FACTORYNAME,temp2.TESTTIME,temp2.TESTTYPE,temp2.RCARD,temp2.PART_SPEC,temp2.TESTSTATION,
        |temp2.LINE,temp1.TESTITEM,temp1.TESTCONDITION,temp1.ID,temp1.LOWER,temp1.UPPER,temp1.SUBITEMTESTVALUE
        |FROM temp1 JOIN temp2
        |ON temp2.ID=temp1.ID
        |
              """.stripMargin)

    // 遍历数据
    datas.rdd.map (item => {
      val dataStr = item.toString()
      val dataSubStr = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = dataSubStr.split("\\,")

      val workshopcodefield = splitArr {0};
      val mdatefield = splitArr {1};
      val firsttimefield = splitArr {2};
      val rcardfield = splitArr {3};
      val partspecfield = splitArr {4};
      val opcodefield = splitArr {5};
      var staCode = splitArr {6};
      val testitemfield = splitArr {7};
      val eattribute1_1field = splitArr {8};
      val idnofield = splitArr {9}.toDouble;
      val lowerfield = splitArr {10};
      val upperfield = splitArr {11};
      val tdatafield = splitArr {12}.substring(0, splitArr{12}.length - 1);

      println(tdatafield)

      var uuid = UUID.randomUUID
      var id = uuid.toString

      // 处理厂区字段
      var factoryfield = ""
      if (workshopcodefield == "生产一区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "生产二区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "生产三区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "生产四区") {
        factoryfield = "汉浦厂"
      } else if (workshopcodefield == "生产五区") {
        factoryfield = "汉浦厂"
      } else if (workshopcodefield == "生产六区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "生产七区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "生产八区") {
        factoryfield = "汉浦厂"
      } else if (workshopcodefield == "生产九区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "NP区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "修复区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "3D区") {
        factoryfield = "台虹厂"
      } else if (workshopcodefield == "古城COB一区") {
        factoryfield = "古城厂"
      } else if (workshopcodefield == "古城后段一区") {
        factoryfield = "古城厂"
      } else if (workshopcodefield == "古城后段二区") {
        factoryfield = "古城厂"
      } else if (workshopcodefield == "古城后段三区") {
        factoryfield = "古城厂"
      } else if (workshopcodefield == "古城试产一区") {
        factoryfield = "古城厂"
      } else if (workshopcodefield =="古城试产二区") {
        factoryfield = "古城厂"
      }

      // 在此处进行自编码的映射
      if (workshopcodefield == "生产一区" && factoryfield == "台虹厂"){

        if (staCode == "WB02" || staCode == "WB-01-0030") {
          staCode = "TC1WB-00002"
        } else if (staCode == "WB31" || staCode == "WB-01-0032") {
          staCode = "TC1WB-00031"
        }else if (staCode == "WB29" || staCode == "WB-01-0033") {
          staCode = "TC1WB-00029"
        } else if (staCode == "WB30" || staCode == "WB-01-0034") {
          staCode = "TC1WB-00030"
        } else if (staCode == "WB34" || staCode == "WB-01-0035") {
          staCode = "TC1WB-00034"
        } else if (staCode == "WB20" || staCode == "WB-01-0036") {
          staCode = "TC1WB-00020"
        } else if (staCode == "WB14" || staCode == "WB-01-0037") {
          staCode = "TC1WB-00014"
        } else if (staCode == "WB13" || staCode == "WB-01-0038") {
          staCode = "TC1WB-00013"
        } else if (staCode == "WB12" || staCode == "WB-01-0039") {
          staCode = "TC1WB-00012"
        } else if (staCode == "WB10" || staCode == "WB-01-0040") {
          staCode = "TC1WB-00010"
        } else if (staCode == "WB28" || staCode == "WB-01-0041") {
          staCode = "TC1WB-00028"
        } else if (staCode == "WB27" || staCode == "WB-01-0042") {
          staCode = "TC1WB-00027"
        } else if (staCode == "WB26" || staCode == "WB-01-0043") {
          staCode = "TC1WB-00026"
        } else if (staCode == "WB25" || staCode == "WB-01-0044") {
          staCode = "TC1WB-00025"
        } else if (staCode == "WB24" || staCode == "WB-01-0045") {
          staCode = "TC1WB-00024"
        } else if (staCode == "WB23" || staCode == "WB-01-0046") {
          staCode = "TC1WB-00023"
        } else if (staCode == "WB22" || staCode == "WB-01-0047") {
          staCode = "TC1WB-00022"
        } else if (staCode == "WB21" || staCode == "WB-01-0048") {
          staCode = "TC1WB-00021"
        } else if (staCode == "WB32" || staCode == "WB-01-0049") {
          staCode = "TC1WB-00032"
        } else if (staCode == "WB19" || staCode == "WB-01-0050") {
          staCode = "TC1WB-00019"
        } else if (staCode == "WB16" || staCode == "WB-01-0080") {
          staCode = "TC1WB-00016"
        } else if (staCode == "WB18" || staCode == "WB-01-0081") {
          staCode = "TC1WB-00018"
        } else if (staCode == "WB08" || staCode == "WB-01-0082") {
          staCode = "TC1WB-00008"
        } else if (staCode == "WB09" || staCode == "WB-01-0083") {
          staCode = "TC1WB-00009"
        } else if (staCode == "WB33" || staCode == "WB-01-0084") {
          staCode = "TC1WB-00033"
        } else if (staCode == "WB15" || staCode == "WB-01-0085") {
          staCode = "TC1WB-00015"
        } else if (staCode == "WB11" || staCode == "WB-01-0086") {
          staCode = "TC1WB-00011"
        } else if (staCode == "WB03" || staCode == "WB-01-0087") {
          staCode = "TC1WB-00003"
        } else if (staCode == "WB04" || staCode == "WB-01-0088") {
          staCode = "TC1WB-00004"
        } else if (staCode == "WB06" || staCode == "WB-01-0089") {
          staCode = "TC1WB-00006"
        } else if (staCode == "WB05" || staCode == "WB-01-0090") {
          staCode = "TC1WB-00005"
        } else if (staCode == "WB07" || staCode == "WB-01-0091") {
          staCode = "TC1WB-00007"
        }

      } else if (workshopcodefield == "生产四区" && factoryfield == "汉浦厂") {

        if (staCode == "WB01" || staCode == "WB-03-0226") {
          staCode = "HC4WB-00001"
        } else if (staCode == "WB02" || staCode == "WB-03-0227") {
          staCode = "HC4WB-00002"
        } else if (staCode == "WB03" || staCode == "WB-03-0228") {
          staCode = "HC4WB-00003"
        } else if (staCode == "WB04" || staCode == "WB-03-0229") {
          staCode = "HC4WB-00004"
        } else if (staCode == "WB05" || staCode == "WB-03-0230") {
          staCode = "HC4WB-00005"
        } else if (staCode == "WB06" || staCode == "WB-03-0231") {
          staCode = "HC4WB-00006"
        } else if (staCode == "WB07" || staCode == "WB-03-0248") {
          staCode = "HC4WB-00007"
        } else if (staCode == "WB08" || staCode == "WB-03-0261") {
          staCode = "HC4WB-00008"
        } else if (staCode == "WB09" || staCode == "WB-03-0250") {
          staCode = "HC4WB-00009"
        } else if (staCode == "WB10" || staCode == "WB-03-0255") {
          staCode = "HC4WB-00010"
        } else if (staCode == "WB11" || staCode == "WB-03-0254") {
          staCode = "HC4WB-00011"
        } else if (staCode == "WB12" || staCode == "WB-03-0253") {
          staCode = "HC4WB-00012"
        } else if (staCode == "WB13" || staCode == "WB-03-0252") {
          staCode = "HC4WB-00013"
        } else if (staCode == "WB14" || staCode == "WB-03-0251") {
          staCode = "HC4WB-00014"
        } else if (staCode == "WB15" || staCode == "WB-03-0249") {
          staCode = "HC4WB-00015"
        } else if (staCode == "WB16" || staCode == "WB-03-0256") {
          staCode = "HC4WB-00016"
        } else if (staCode == "WB17" || staCode == "WB-03-0257") {
          staCode = "HC4WB-00017"
        } else if (staCode == "WB18" || staCode == "WB-03-0262") {
          staCode = "HC4WB-00018"
        } else if (staCode == "WB19" || staCode == "WB-03-0243") {
          staCode = "HC4WB-00019"
        } else if (staCode == "WB20" || staCode == "WB-03-0242") {
          staCode = "HC4WB-00020"
        } else if (staCode == "WB21" || staCode == "WB-03-0241") {
          staCode = "HC4WB-00021"
        } else if (staCode == "WB22" || staCode == "WB-03-0258") {
          staCode = "HC4WB-00022"
        } else if (staCode == "WB23" || staCode == "WB-03-0259") {
          staCode = "HC4WB-00023"
        } else if (staCode == "WB24" || staCode == "WB-03-0260") {
          staCode = "HC4WB-00024"
        } else if (staCode == "WB25" || staCode == "WB-03-0265") {
          staCode = "HC4WB-00025"
        } else if (staCode == "WB26" || staCode == "WB-03-0264") {
          staCode = "HC4WB-00026"
        } else if (staCode == "WB27" || staCode == "WB-03-0263") {
          staCode = "HC4WB-00027"
        } else if (staCode == "WB28" || staCode == "WB-03-0244") {
          staCode = "HC4WB-00028"
        } else if (staCode == "WB29" || staCode == "WB-03-0245") {
          staCode = "HC4WB-00029"
        } else if (staCode == "WB30" || staCode == "WB-03-0246") {
          staCode = "HC4WB-00030"
        } else if (staCode == "WB31" || staCode == "WB-03-0247") {
          staCode = "HC4WB-00031"
        } else if (staCode == "WB32" || staCode == "WB-03-0240") {
          staCode = "HC4WB-00032"
        } else if (staCode == "WB33" || staCode == "WB-03-0239") {
          staCode = "HC4WB-00033"
        } else if (staCode == "WB34" || staCode == "WB-03-0238") {
          staCode = "HC4WB-00034"
        } else if (staCode == "WB35" || staCode == "WB-03-0237") {
          staCode = "HC4WB-00035"
        } else if (staCode == "WB36" || staCode == "WB-03-0236") {
          staCode = "HC4WB-00036"
        } else if (staCode == "WB37" || staCode == "WB-03-0235") {
          staCode = "HC4WB-00037"
        } else if (staCode == "WB38" || staCode == "WB-03-0211") {
          staCode = "HC4WB-00038"
        } else if (staCode == "WB39" || staCode == "WB-03-0212") {
          staCode = "HC4WB-00039"
        } else if (staCode == "WB40" || staCode == "WB-03-0213") {
          staCode = "HC4WB-00040"
        } else if (staCode == "WB41" || staCode == "WB-03-0214") {
          staCode = "HC4WB-00041"
        } else if (staCode == "WB42" || staCode == "WB-03-0215") {
          staCode = "HC4WB-00042"
        } else if (staCode == "WB43" || staCode == "WB-03-0216") {
          staCode = "HC4WB-00043"
        } else if (staCode == "WB44" || staCode == "WB-03-0220") {
          staCode = "HC4WB-00044"
        } else if (staCode == "WB45" || staCode == "WB-03-0221") {
          staCode = "HC4WB-00045"
        } else if (staCode == "WB46" || staCode == "WB-03-0222") {
          staCode = "HC4WB-00046"
        } else if (staCode == "WB47" || staCode == "WB-03-0223") {
          staCode = "HC4WB-00047"
        } else if (staCode == "WB48" || staCode == "WB-03-0224") {
          staCode = "HC4WB-00048"
        } else if (staCode == "WB49" || staCode == "WB-03-0225") {
          staCode = "HC4WB-00049"
        } else if (staCode == "WB50" || staCode == "WB-03-0232") {
          staCode = "HC4WB-00050"
        } else if (staCode == "WB51" || staCode == "WB-03-0233") {
          staCode = "HC4WB-00051"
        } else if (staCode == "WB52" || staCode == "WB-03-0234") {
          staCode = "HC4WB-00052"
        } else if (staCode == "WB53" || staCode == "WB-03-0217") {
          staCode = "HC4WB-00053"
        } else if (staCode == "WB54" || staCode == "WB-03-0218") {
          staCode = "HC4WB-00054"
        } else if (staCode == "WB55" || staCode == "WB-03-0219") {
          staCode = "HC4WB-00055"
        } else if (staCode == "WB56" || staCode == "WB-03-0267") {
          staCode = "HC4WB-00056"
        } else if (staCode == "WB57" || staCode == "WB-03-0266") {
          staCode = "HC4WB-00057"
        } else if (staCode == "WB58" || staCode == "WB-03-0109") {
          staCode = "HC4WB-00058"
        } else if (staCode == "WB59" || staCode == "WB-03-0108") {
          staCode = "HC4WB-00059"
        } else if (staCode == "WB60" || staCode == "WB-03-0110") {
          staCode = "HC4WB-00060"
        } else if (staCode == "WB61" || staCode == "WB-03-0107") {
          staCode = "HC4WB-00061"
        }

      } else if (workshopcodefield == "古城COB一区" && factoryfield == "古城厂") {

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

      }



      try {
        insertData(kuduMaster, resultTable, id, s"${workshopcodefield}", s"${mdatefield}",
          s"${firsttimefield}", s"${rcardfield}", s"${partspecfield}",
          s"${opcodefield}", s"${staCode}", s"${testitemfield}",
          s"${eattribute1_1field}",  idnofield, s"${lowerfield}", s"${upperfield}",
          s"${tdatafield}", s"${factoryfield}")
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

    }).collect()

  }

  def insertData(kuduMaster: String, insertTable: String,id: String, workshopcode: String, mdate: String,
                 firsttime: String, rcard: String, partspec: String, opcode: String, stacode: String,
                 testitem: String, eattribute1_1: String, idno: Double, lower: String, upper: String,
                 tdata: String, factory: String): Unit ={
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(insertTable)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)


    val insert = kuduTable.newUpsert()
    val rowUpsert = insert.getRow()

    rowUpsert.addString("ID", id)
    rowUpsert.addString("WORKSHOP_CODE", s"${workshopcode}")
    rowUpsert.addString("MDATE", s"${mdate}")
    rowUpsert.addString("FIRST_TIME", s"${firsttime}")
    rowUpsert.addString("RCARD", s"${rcard}")
    rowUpsert.addString("PART_SPEC", s"${partspec}")
    rowUpsert.addString("OP_CODE", s"${opcode}")
    rowUpsert.addString("STA_CODE", s"${stacode}")
    rowUpsert.addString("TESTITEM", s"${testitem}")
    rowUpsert.addString("EATTRIBUTE1_1", s"${eattribute1_1}")
    rowUpsert.addDouble("ID_NO", idno)
    rowUpsert.addString("LOWER", s"${lower}")
    rowUpsert.addString("UPPER", s"${upper}")
    rowUpsert.addString("TDATA", s"${tdata}")
    rowUpsert.addString("FACTORY", s"${factory}")
    // 执行insert操作
    session.apply(insert)

    session.flush()
    session.close()
    kuduClient.close()

  }


}

