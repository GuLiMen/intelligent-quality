package cn.qtech.bigdata.spcprocess

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import org.apache.kudu.client.{KuduClient, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SpcMesPoint {

  // 匹配浮点数
  private final val res = "[-+]?[0-9]*\\.?[0-9]+"
  private final val pattern = Pattern.compile(res)

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

    val tableName = "ADS_INSPECTIONPASSRATE"

    val spcclresultlist = "ADS_SPCMESCONTROLLIMIT"

    val insertTable = "ADS_SPCMESPOINTLIST"

    PointCalculate(sparkSession: SparkSession, sc:
      SparkContext, kuduMaster: String, tableName: String, spcclresultlist: String, insertTable:String)

    sc.stop()
  }

  private def PointCalculate (sparkSession: SparkSession, sc: SparkContext, kuduMaster: String,
                              tableName: String, spcclresultlist: String, insertTable:String): Unit = {
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    val optionsclresult = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> spcclresultlist
    )
    val optionsinserttable = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> insertTable
    )

    sparkSession.read.options(options).format("kudu").load
      .where("first_time='巡检'").createOrReplaceTempView("temp1notfiltertime")
    sparkSession.read.options(optionsclresult).format("kudu").load
      .createOrReplaceTempView("getclresult")
    sparkSession.read.options(optionsinserttable).format("kudu").load
      .createOrReplaceTempView("pointtable")

    // 获取kudu中结果表的最大时间，把这个时间作为本次运行起始时间
    val kudutimeLists = sparkSession.sql("select max(mdate) from pointtable").collect().toList
    var kudutimeList = kudutimeLists.toString()
    var kudutimes = kudutimeList.substring(6,kudutimeList.length-2)
    var timemin = "2020-06-30 01:12:51.303"

    // 获取源表的最大时间作为程序运行的结束时间
    var sourcetimelists = sparkSession.sql("select min(mdate),max(mdate) from temp1notfiltertime")
      .collect().toList
    var sourcetimelist = sourcetimelists.toString()
    var sourcetimes = sourcetimelist.substring(6, sourcetimelist.length - 2).split("\\,")
    var sourcetimemin = sourcetimes{0}
    var sourcetimemax = sourcetimes{1}

    if (kudutimes != "null") {
      timemin = kudutimes
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var sDate = sdf.parse(timemin)

    val calendar = Calendar.getInstance()
    calendar.setTime(sDate);
    calendar.add(Calendar.DATE,-10)  // 把日期往前推十天,整数

    var minTime = sdf.format(calendar.getTime())

    sparkSession.sql("select * from temp1notfiltertime")
      .where(s"mdate>'${minTime}' and mdate<='${sourcetimemax}'")
//      .where(s"mdate>'2021-07-01 00:00:00' and mdate<='2021-09-30 00:00:00' and part_spec = 'C18F05' ")
      .createOrReplaceTempView("temp1")

    // 关联源表和控制限表，判断点是否存在对应的控制限
    sparkSession.sql(
      """
        |SELECT a.lower,a.upper ,a.tdata ,a.id_no ,a.rcard,a.mdate ,a.first_time ,a.workshop_code ,
        |a.part_spec ,a.op_code ,a.sta_code,a.eattribute1_1,a.testitem,a.factory
        |from temp1 as a
        |join getclresult as b
        |on a.FIRST_TIME=b.FIRST_TIME and a.WORKSHOP_CODE=b.WORKSHOP_CODE and
        |    a.part_spec=b.part_spec and a.OP_CODE=b.OP_CODE and a.STA_CODE=b.STA_CODE
        |    and a.eattribute1_1=b.eattribute1_1 and a.TESTITEM=b.TESTITEM and a.FACTORY=b.FACTORY
      """.stripMargin).createOrReplaceTempView("temp2")

    sparkSession.sql("select * from temp2").show()

    // 把ID_NO相同的数据加入到同一分组中
    val groupdata = sparkSession.sql(
      """
        |
        |select a.lower,a.upper ,a.tdata ,a.id_no ,a.rcard,a.mdate ,a.first_time ,a.workshop_code ,
        |a.part_spec ,a.op_code ,a.sta_code,a.eattribute1_1,a.testitem,a.factory,
        |    row_number() over(partition by a.FIRST_TIME,
        |    a.WORKSHOP_CODE,
        |    a.PART_SPEC,
        |    a.OP_CODE,
        |    a.STA_CODE,
        |    a.eattribute1_1,
        |    a.testitem,
        |    a.FACTORY,
        |    a.id_no
        |    order by a.mdate asc) as num
        |    from temp2 a
      """.stripMargin)

    val datalist = groupdata.collect().toIterator

    val integer = new AtomicInteger(0)

    // 创建List保存id_no数据和分组数据
    var groupDataList = new ListBuffer[String]()
    // 创建集合保存分组数据，如果当分组条件一直与前一条数据相等，就不断的把分组数据加入到集合中
    // 直到当分组条件不等于前一条数据时，清空集合，用以后面当PCS为1时，从List中取值
    var groupDataOnePcsList = new ListBuffer[String]()
    // 定义存放tdata数据的集合
    var tdataDatas = new ListBuffer[Float]()

    // 遍历并查询数据
    while (datalist.hasNext) {
      val rowdata = datalist.next()
      val datastr: String = rowdata.toString()
      val transfdata: String = datastr.substring(1, datastr.length - 1)
      val splitArr: Array[String] = transfdata.split("\\,")

      val firstTimeGroupNew = splitArr {6};
      val workShopCodeGroupNew = splitArr {7};
      val partSpecGroupNew = splitArr {8};
      val opCodeGroupNew = splitArr {9};
      val staCodeGroupNew = splitArr {10};
      val eattribute1_1GroupNew = splitArr {11};
      val testItemGroupNew = splitArr {12};
      val factoryGroupNew = splitArr {13};
      val numfield = splitArr {14};

      // 自增加1
      integer.getAndIncrement()

      if (integer.get() == numfield.toInt) {

        // 不断把数据加入到集合中
        groupDataList += transfdata

      } else if (integer.get() > numfield.toInt) {

        // 当同一个分组的数据都加入集合后，遍历集合
        groupDataList.foreach (item => {

          // 遍历获取到一个ID_NO的数据
          val pointDatasStr = item.toString
          var pointArrs = pointDatasStr.split("\\,")

          val tdata = pointArrs {2};

          // 将同一个分组，相同ID_NO号的TDATA值用分号分割或直接加入到集合中
          if (tdata.contains(";")) {
            val tdataResults = tdata.split(";").toIterator
            while (tdataResults.hasNext) {
              // 将数据加入到集合中
              val tdataDatass = tdataResults.next()
              if (pattern.matcher(s"$tdataDatass").matches()) {
                tdataDatas += tdataDatass.toFloat
              }
            }
          } else if (pattern.matcher(s"$tdata").matches()) {
            // 如果inspectionResult匹配的值为浮点数，直接将数据插入到集合中
            tdataDatas += tdata.toFloat
          }

        })

        // 主键id
        var uuid = UUID.randomUUID
        var id = uuid.toString

        val groupDataOne = groupDataList.subList(0,1)
        val groupDataStr = groupDataOne.toString
        val groupDatasArr = groupDataStr.substring(1).split("\\,")
        val id_NoField = groupDatasArr {3};
        val rCardField = groupDatasArr {4};
        val maxcreatedtime = groupDatasArr {5};
        val firstTimeField = groupDatasArr {6};
        val workShopCodeField = groupDatasArr {7};
        val partSpecField = groupDatasArr {8};
        val opCodeField = groupDatasArr {9};
        val staCodeField = groupDatasArr {10};
        val eattribute1_1Field = groupDatasArr {11};
        val testItemField = groupDatasArr {12};
        val factoryField = groupDatasArr {13};

        if (tdataDatas.size == 1 && groupDataOnePcsList.size >= 2 && maxcreatedtime > timemin) {
          // 如果抽检数为1，计算极差（取上一个单号的值进行比较）和单值（单值和移动极差图）

          // 获取上一个单号的值
          val groupDataOnePcsArr = groupDataOnePcsList
            .subList(groupDataOnePcsList.length - 2, groupDataOnePcsList.length - 1).toString.split("\\,")
          val othertdataStr = groupDataOnePcsArr{2}

          // 创建集合，保存tdata数据
          var otherTdatadatas = new ListBuffer[Float]()

          if (othertdataStr.contains(";")) {
            val otherResults = othertdataStr.split(";").toIterator
            while (otherResults.hasNext) {
              // 将数据加入到集合中
              val otherdatass = otherResults.next()
              if (pattern.matcher(s"$otherdatass").matches()) {
                otherTdatadatas += otherdatass.toFloat
              }
            }
          } else if (pattern.matcher(s"$othertdataStr").matches()) {
            // 如果inspectionResult匹配的值为浮点数，直接将数据插入到集合中
            otherTdatadatas += othertdataStr.toFloat
          }

          val avgdata = tdataDatas.sum / tdataDatas.size
          if (otherTdatadatas.nonEmpty) {
            if (avgdata >= otherTdatadatas.max) {
              val rangeonepcs = (avgdata - otherTdatadatas.min).formatted("%.3f")

              insertData(kuduMaster, insertTable, id, firstTimeField, workShopCodeField, partSpecField, opCodeField,
                staCodeField, eattribute1_1Field, testItemField, factoryField, id_NoField, rCardField, maxcreatedtime,
                avgdata.formatted("%.3f").toString, rangeonepcs.toString, "")

            } else if (avgdata <= otherTdatadatas.max && avgdata >= otherTdatadatas.min) {
              val rangeonepcs = (otherTdatadatas.max - otherTdatadatas.min).formatted("%.3f")

              insertData(kuduMaster, insertTable, id, firstTimeField, workShopCodeField, partSpecField, opCodeField,
                staCodeField, eattribute1_1Field, testItemField, factoryField, id_NoField, rCardField, maxcreatedtime,
                avgdata.formatted("%.3f").toString, rangeonepcs.toString, "")

            } else if (avgdata <= otherTdatadatas.min) {
              val rangeonepcs = (otherTdatadatas.max - avgdata).formatted("%.3f")

              insertData(kuduMaster, insertTable, id, firstTimeField, workShopCodeField, partSpecField, opCodeField,
                staCodeField, eattribute1_1Field, testItemField, factoryField, id_NoField, rCardField, maxcreatedtime,
                avgdata.formatted("%.3f").toString, rangeonepcs.toString, "")

            }
          }
          otherTdatadatas.clear()

        } else if (tdataDatas.size >= 2 && tdataDatas.size <= 5 && maxcreatedtime > timemin) {
          // 如果为2-5，计算极差和平均值 (极差值在极差图，平均值在均值图）
          val rangedata = (tdataDatas.max - tdataDatas.min).formatted("%.3f")
          val avgdata = (tdataDatas.sum / tdataDatas.size).formatted("%.3f")

          insertData(kuduMaster, insertTable, id, firstTimeField, workShopCodeField, partSpecField, opCodeField,
            staCodeField, eattribute1_1Field, testItemField, factoryField, id_NoField, rCardField, maxcreatedtime,
            avgdata.toString, rangedata.toString, "")

        } else if (tdataDatas.size >= 6 && maxcreatedtime > timemin) {
          // 如果大于6，计算标准差和平均值 （标准差值在标准差图，平均值在均值图）
          val inspecResultRDD = sc.makeRDD(tdataDatas)
          val stddevdata = inspecResultRDD.stdev().toFloat.formatted("%.3f")
          val avgdata = (tdataDatas.sum / tdataDatas.size).formatted("%.3f")

          insertData(kuduMaster, insertTable, id, firstTimeField, workShopCodeField, partSpecField, opCodeField,
            staCodeField, eattribute1_1Field, testItemField, factoryField, id_NoField, rCardField, maxcreatedtime,
            avgdata.toString, "", stddevdata.toString)

        }

        println("一个分组结束，触发计算")
        // 清空当前集合
        tdataDatas.clear()
        groupDataList.clear()
        // 将自增数为1时的数据插入到集合中
        groupDataList += transfdata
        // 将数字初始值置为1
        integer.set(1)
      }

      // 将分组数据加入到集合中
      groupDataOnePcsList += transfdata
      if (groupDataOnePcsList.size() >= 2) {
        // 获取集合中当前数据的上一条数据
        val groupLastData = groupDataOnePcsList
          .subList(groupDataOnePcsList.length - 2, groupDataOnePcsList.length - 1)
        val groupLastDataStr = groupLastData.toString
        val groupLastDataArr = groupLastDataStr.substring(1).split("\\,")
        val firstTimeGroupLast = groupLastDataArr {6};
        val workShopCodeGroupLast = groupLastDataArr {7};
        val partSpecGroupLast = groupLastDataArr {8};
        val opCodeGroupLast = groupLastDataArr {9};
        val staCodeGroupLast = groupLastDataArr {10};
        val eattribute1_1GroupLast = groupLastDataArr {11};
        val testItemGroupLast = groupLastDataArr {12};
        val factoryGroupLast = groupLastDataArr {13};

        // 如果当前数据的分组条件等于上一条数据的分组条件，保存分组条件数据到集合中
        if (firstTimeGroupNew == firstTimeGroupLast && workShopCodeGroupNew == workShopCodeGroupLast &&
          partSpecGroupNew == partSpecGroupLast && opCodeGroupNew == opCodeGroupLast &&
          staCodeGroupNew == staCodeGroupLast && eattribute1_1GroupNew == eattribute1_1GroupLast &&
          testItemGroupNew == testItemGroupLast && factoryGroupNew == factoryGroupLast) {

        } else {
          // 如果当前数据的分组条件不等于上一条数据的分组条件，清空集合中上一条及更早的分组条件数据
          groupDataOnePcsList.remove(0, groupDataOnePcsList.length - 1)
        }
      }

    }
  }

  def insertData(kuduMaster: String, spcclresultlist: String, id:String, firsttime:String, workshopcode: String,
                 partspec: String, opcode: String, stacode: String, eattribute11:String, testitem: String,
                 factory:String, idno:String, rcard:String, mdate: String, avgvalue :String, rangevalue: String,
                 stddevvalue: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val kuduTable = kuduClient.openTable(spcclresultlist)

    val  session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val insert = kuduTable.newInsert()
    val rowUpsert = insert.getRow()

    rowUpsert.addString("ID", s"${id}")
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
    rowUpsert.addString("MDATE", s"${mdate}")
    rowUpsert.addString("AVGVALUE", s"${avgvalue}")
    rowUpsert.addString("RANGEVALUE", s"${rangevalue}")
    rowUpsert.addString("STDDEVVALUE", s"${stddevvalue}")

    // 执行upsert操作
    session.apply(insert)

    session.flush()
    session.close()
    kuduClient.close()

  }

}
