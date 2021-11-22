package cn.qtech.bigdata.clean

import java.util.UUID

import org.apache.kudu.client.{KuduClient}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object fqcRejectRateClean {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu").set("spark.debug.maxToStringFields", "100").set("spark.port.maxRetries", "500")
//      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).config("spark.sql.crossJoin.enabled", "true").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "10.170.3.11,10.170.3.12,10.170.3.13"
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()

    val tableName1 = "MESC_WSCPARAMETER"
    val tableName2 = "MESC_T_RESUME"
    val tableName3 = "MESC_I_MATERIAL"
    val tableName4 = "MESC_T_SNERRORCODE"
    val tableName5 = "MESC_B_ERRORCODE"

    val resultTable = "ADS_FQCREJECTRATE"

    processData(sparkSession, sc, kuduMaster, tableName1, tableName2, tableName3, tableName4, tableName5, resultTable)

    sc.stop()
  }

  private def processData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, tableName1: String,
                          tableName2: String, tableName3: String, tableName4: String, tableName5: String,
                          resultTable: String): Unit = {
    val option1 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName1
    )
    val option2 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName2
    )
    val option3 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName3
    )
    val option4 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName4
    )
    val option5 = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName5
    )
    val optionResult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> resultTable
    )

    sparkSession.read.options(option1).format("kudu").load
      .createOrReplaceTempView("wscparameterTable")
    sparkSession.read.options(option2).format("kudu").load
      .createOrReplaceTempView("tresumeTablenotfiltertime")
    sparkSession.read.options(option3).format("kudu").load
      .createOrReplaceTempView("imaterialTable")
    sparkSession.read.options(option4).format("kudu").load
      .createOrReplaceTempView("tsnerrorcodeTable")
    sparkSession.read.options(option5).format("kudu").load
      .createOrReplaceTempView("berrorcodeTable")
    sparkSession.read.options(optionResult).format("kudu").load
      .createOrReplaceTempView("resultTable")

    // 获取kudu中结果表最大的时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql(s"select max(mdate) from resultTable").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6, kudutimeList.length - 2)

    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from tresumeTablenotfiltertime").collect().toList
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

    sparkSession.sql("select * from tresumeTablenotfiltertime aa where aa.op_code='QC外观检' or aa.op_code='QC CCD检验'")
      .where(s"mdate>'${timemin}' and mdate<='${sourcetimemax}'")
      .createOrReplaceTempView("tresumeTable")


    // 对FQC批退率指标相关的表进行关联
    val datas = sparkSession.sql("SELECT aa.wo_code ,dd.pixel ,aa.rcard ,aa.router_code,aa.op_code ,aa.ok_qty ,aa.sampling_qty ,cc.ec_name ,bb.qty " +
      ",aa.muser , " +
      "aa.mdate ,aa.workshop_code FROM tresumeTable aa left JOIN tsnerrorcodeTable bb ON aa.rcard=bb.rcard AND aa.op_code=bb.op_code AND aa.MDATE=bb" +
      ".MDATE left JOIN berrorcodeTable cc ON bb.EC_CODE=cc.EC_CODE inner JOIN imaterialTable dd ON aa.ITEM_CODE=dd.PART_CODE left JOIN " +
      "wscparameterTable ee ON aa.OP_CODE=ee.PNAME")

    // 遍历数据
    val resultRDD: RDD[BasicFeatures] = datas.rdd.map(item => {
      val dataStr = item.toString()
      val dataSubStr = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = dataSubStr.split("\\,")

      val wocodefield = splitArr {
        0
      };
      val pixelfield = splitArr {
        1
      };
      val rcardfield = splitArr {
        2
      };
      val routercodefield = splitArr {
        3
      };
      val opcodefield = splitArr {
        4
      };
      val okqtyfield = splitArr {
        5
      };
      val samplingqtyfield = splitArr {
        6
      };
      val ecnamefield = splitArr {
        7
      };
      val ngqtyfield = splitArr {
        8
      };
      val muserfield = splitArr {
        9
      };
      val mdatefield = splitArr {
        10
      };
      val workshopcodefield = splitArr {
        11
      }.substring(0, splitArr {
        11
      }.length - 1);

      var uuid = UUID.randomUUID
      var id = uuid.toString

      var resultfield = ""
      // 处理判断不良结果字段
      if (ngqtyfield == "null" || ngqtyfield == "0.0") {
        resultfield = "OK"
      } else {
        resultfield = "NG"
      }

      // 处理厂区字段
      var factoryfield = ""
      var workshopcodeNew = ""
      if (workshopcodefield.equals("生产一区")) {
        workshopcodeNew = "台虹测试一区"
        factoryfield = "台虹厂"
      } else if (workshopcodefield.equals("生产二区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹测试二区"
      } else if (workshopcodefield.equals("生产三区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹测试三区"
      } else if (workshopcodefield.equals("生产四区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二测试三区"
      } else if (workshopcodefield.equals("生产五区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二COB二区"
      } else if (workshopcodefield.equals("生产六区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹测试六区"
      } else if (workshopcodefield.equals("生产七区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹测试七区"
      } else if (workshopcodefield.equals("生产九区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹测试九区"
      } else if (workshopcodefield.equals("NP区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹试产区"
      } else if (workshopcodefield.equals("修复区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = "台虹修复区"
      } else if (workshopcodefield.equals("3D区")) {
        factoryfield = "台虹厂"
        workshopcodeNew = workshopcodefield
      }
      else if (workshopcodefield.equals("古城COB一区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一COB一区"
      } else if (workshopcodefield.equals("古城COB二区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一COB一区"
      } else if (workshopcodefield.equals("古城后段一区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一测试一区"
      } else if (workshopcodefield.equals("古城后段二区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一测试二区"
      } else if (workshopcodefield.equals("古城试产一区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一试产一区"
      } else if (workshopcodefield.equals("古城试产二区")) {
        factoryfield = "古城一厂"
        workshopcodeNew = "古一试产二区"
      } else if (workshopcodefield.equals("古二COB二区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二COB二区"
      } else if (workshopcodefield.equals("古二试产一区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二试产一区"
      } else if (workshopcodefield.equals("古二试产二区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二试产二区"
      } else if (workshopcodefield.equals("古二测试三区")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二测试三区"
      } else if (workshopcodefield.equals("SMT")) {
        factoryfield = "古城二厂"
        workshopcodeNew = "古二SMT"
      } else {
        workshopcodeNew = workshopcodefield
        factoryfield = factoryfield
      }
      BasicFeatures(id, wocodefield, pixelfield, rcardfield, routercodefield, opcodefield, okqtyfield.toDouble, samplingqtyfield.toDouble, ecnamefield, ngqtyfield, resultfield, muserfield, mdatefield, workshopcodeNew, factoryfield)

    })
    val sql = sparkSession.sqlContext
    import sql.implicits._
    resultRDD.toDF().createOrReplaceTempView("resultDF")

    sparkSession.sql("insert into resultTable select * from resultDF")

  }

  case class BasicFeatures(id: String, wocode: String, pixel: String, rcard: String,
                           routercode: String, opcode: String, okqty: Double, samplingqty: Double, ecname: String, ngqty: String,
                           result: String, muser: String, mdate: String, workshopcode: String, factory: String)

}

