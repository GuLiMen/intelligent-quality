package cn.qtech.bigdata.clean

import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import cn.qtech.bigdata.common.filterData

object filterCpkAndSpcItem {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameKudu").set("spark.debug.maxToStringFields", "100")
//    .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).config("spark.sql.crossJoin.enabled","true")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduMaster = "10.170.3.11, 10.170.3.12, 10.170.3.13"

//    val tableName = "ADS_CPKWEEKLISTBYMES"
//    val tableName = "ADS_CPKMONTHLISTBYMES"
    val tableName = "ADS_CPKQUARTERLISTBYMES"
//    val tableName = "ADS_SPCMESRESULTLIST"

    processData(sparkSession, sc, kuduMaster, tableName)

    sc.stop()
  }

  private def processData(sparkSession: SparkSession, sc:
  SparkContext, kuduMaster: String, tableName: String): Unit = {
    val option = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

    sparkSession.read.options(option).format("kudu").load
      .createOrReplaceTempView("temp")

    var datas = sparkSession.sql("select * from temp")

    // 遍历数据
    datas.rdd.map (item => {
      val dataStr = item.toString()
      val dataSubStr = dataStr.substring(1, dataStr.length)
      val splitArr: Array[String] = dataSubStr.split("\\,")

      val idField = splitArr {0}

      // 获取到CPK表的主键id、站组、测试项
      val opcodeField = splitArr {7};
      val testitemField = splitArr {10};

      // 获取到SPC表的站组、测试项
//      val opcodeField = splitArr {5};
//      val testitemField = splitArr {8};

      // 判断是否为要删除的数据
      val filterSuccess = filterData.filterData(opcodeField, testitemField)
      if (!filterSuccess) {
          deleteData(kuduMaster, tableName, idField)
      }

    }).collect()


  }


  // 根据获取到的主键id, 删除表中对应行的数据
  def deleteData(kuduMaster: String, tableName: String, id:String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(tableName)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val delete = kuduTable.newDelete()
    val rowDelete = delete.getRow()

    rowDelete.addString("ID", s"${id}")

    // 执行upsert操作
    session.apply(delete)

    session.flush()
    session.close()
    kuduClient.close()

  }


}

