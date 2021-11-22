package cn.qtech.bigdata.clean

import org.apache.kudu.client.{KuduClient, KuduTable, SessionConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import cn.qtech.bigdata.common.filterData.newWork_code
import org.apache.spark.rdd.RDD

object auditClean {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("DataFrameKudu")
      .set("spark.debug.maxToStringFields", "100")
//      .setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "10.170.3.11,10.170.3.12,10.170.3.13"

    val tableName = "MESC_T_SJQC_INSPECT_RECORD"
    val resultTable = "ADS_T_SJQC_INSPECT_RECORD"

    processData(sparkSession, sc, kuduMaster, tableName, resultTable)

    sc.stop()

  }

  private def processData(sparkSession: SparkSession, sc:
  SparkContext, kuduMaster: String, tableName: String, resultTable: String): Unit = {
    val option = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    val optionResult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> resultTable
    )


    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(resultTable)

    val session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)


    sparkSession.read.options(option).format("kudu").load
      .createOrReplaceTempView("temp1notfiltertime")
    sparkSession.read.options(optionResult).format("kudu").load
      .createOrReplaceTempView("resultTable")

    // 获取kudu中结果表最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(mdate) from resultTable").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from temp1notfiltertime").collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes {
      0
    }
    var sourcetimemax = sourcetimes {
      1
    }
    var timemin = "2018-06-30 00:00:00"

    if (kudutimes != "null") {
      timemin = kudutimes
    }

    sparkSession.sql("select * from temp1notfiltertime")
      .where(s"mdate>'${timemin}' and mdate<='${sourcetimemax}'")
//      .where(s"mdate>'2018-06-30 00:00:00'")
      .createOrReplaceTempView("temp1")

    val datas = sparkSession.sql("select mdate,muser,day,shift,partspec,wocode,rcard,stage,ownetype,promptdeal,dutyowner,confirmowner," +
      "qcowner,dutysta,dutyday,remark,qctype,id,area,eattribute1,eattribute2,qty,machineno,wirepull5,ballshear1,ballshear2,ballshear3,ballshear4," +
      "ballshear5,ballsize1,ballsize2,ballsize3,ballsize4,memo1,memo2,memo3,memo4,description from temp1")

    // 遍历数据
    val resultRDD: RDD[BasicFeatures] = datas.rdd.repartition(8).map(item => {

      val dataStr = item.toString()
      val dataSubStr = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = dataSubStr.split("\\,")

      val mdatefield = splitArr {0};
      val muserfield = splitArr {1};
      val dayfield = splitArr {2};
      val shiftfield = splitArr {3};
      val partspecfield = splitArr {4};
      val wocodefield = splitArr {5};
      val rcardfield = splitArr {6};
      val stagefield = splitArr {7};
      val ownetypefield = splitArr {8};
      val promptdealfield = splitArr {9};
      val dutyownerfield = splitArr {10};
      val confirmownerfield = splitArr {11};
      val qcownerfield = splitArr {12};
      val dutystafield = splitArr {13};
      val dutydayfield = splitArr {14};
      val remarkfield = splitArr {15};
      val qctypefield = splitArr {16};
      var idfield = splitArr {17};
      val areafield = splitArr {18};
      val eattribute1field = splitArr {19};
      val eattribute2field = splitArr {20};
      val qtyfield = splitArr {21};
      val machinenofield = splitArr {22};
      val wirepull5field = splitArr {23};
      val ballshear1field = splitArr {24};
      val ballshear2field = splitArr {25};
      val ballshear3field = splitArr {26};
      val ballshear4field = splitArr {27};
      val ballshear5field = splitArr {28};
      val ballsize1field = splitArr {29};
      val ballsize2field = splitArr {30};
      val ballsize3field = splitArr {31};
      val ballsize4field = splitArr {32};
      val memo1field = splitArr {33};
      val memo2field = splitArr {34};
      val memo3field = splitArr {35};
      val memo4field = splitArr {36};
      var description = ""
      for (x <- 37 to splitArr.length - 1) {
        description = description + "," + splitArr {
          x
        }
      }
      val descriptionfield = description.substring(1, description.length - 1)

      // 处理厂区字段
      var factoryfield = ""

      val tuple: (String, String) = newWork_code(factoryfield, areafield, eattribute2field)

      var factoryNew = tuple._1
      var areaNew = tuple._2

      //处理工段字段
      var sectionfield = ""
      if (eattribute2field == "十级") {
        sectionfield = "COB"
      } else if (eattribute2field == "千级") {
        sectionfield = "测试"
      }

      BasicFeatures(descriptionfield, mdatefield, muserfield, dayfield, shiftfield, partspecfield, wocodefield,
        rcardfield, stagefield, ownetypefield, promptdealfield, dutyownerfield, confirmownerfield,
        qcownerfield, dutystafield, dutydayfield, remarkfield, qctypefield, idfield.toDouble, areaNew, eattribute1field,
        eattribute2field, qtyfield, machinenofield, wirepull5field, ballshear1field, ballshear2field,
        ballshear3field, ballshear4field, ballshear5field, ballsize1field, ballsize2field, ballsize3field,
        ballsize4field, memo1field, memo2field, memo3field, memo4field, factoryNew, sectionfield)
    })

    val sql = sparkSession.sqlContext
    import sql.implicits._
    resultRDD.toDF().createOrReplaceTempView("resultDF")

//    print(sparkSession.sql("select * from resultDF").count())

    sparkSession.sql("insert into resultTable select * from resultDF")

  }

  case class BasicFeatures(description: String, mdate: String, muser: String,
                           day: String, shift: String, partspec: String, wocode: String, rcard: String, stage: String,
                           ownetype: String, promptdeal: String, dutyowner: String, confirmowner: String, qcowner: String,
                           dutysta: String, dutyday: String, remark: String, qctype: String, id: Double, area: String,
                           eattribute1: String, eattribute2: String, qty: String, machineno: String, wirepull5: String,
                           ballshear1: String, ballshear2: String, ballshear3: String, ballshear4: String, ballshear5: String,
                           ballsize1: String, ballsize2: String, ballsize3: String, ballsize4: String, memo1: String,
                           memo2: String, memo3: String, memo4: String, factory: String, section: String)

}