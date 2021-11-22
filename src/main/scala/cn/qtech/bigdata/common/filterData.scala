package cn.qtech.bigdata.common

object filterData {


  def main(args: Array[String]): Unit = {
    val tuple: (String, String) = newWork_code("", "生产二区")

    print(tuple._2)
  }

  /**
    * 制程质量CPK及SPC只针对指定测试项数据进行计算，在此过滤掉其它数据
    *
    * @param opcode   站组
    * @param testitem 测试项
    * @return 数据是否包含于指定测试项范围
    */
  def filterData(opcode: String, testitem: String): Boolean = {
    var flag = true
    if (opcode == "DB") {
      if (testitem == "偏心X" || testitem == "偏心Y" || testitem == "浮高Z" || testitem == "rotation W/um" ||
        testitem == "胶重" || testitem == "胶厚" || testitem == "翘曲" || testitem == "Die shear") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "WB") {
      if (testitem == "Loop Height" || testitem == "Inside loop height" || testitem == "Outside Height" ||
        testitem == "Wire pull" || testitem == "Stich  Pull" || testitem == "Ball Size" ||
        testitem == "Ball  shear(lead)" || testitem == "Ball  shear(pad)" || testitem == "ball thinkess(Lead)" ||
        testitem == "ball thinkess(die)" || testitem == "金线线距") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "HM") {
      if (testitem == "浮高Z" || testitem == "偏心X" || testitem == "偏心Y" || testitem == "cover 偏移" ||
        testitem == "cover 角度" || testitem == "cover 胶宽" || testitem == "cover 胶厚" || testitem == "cover 推力" ||
        testitem == "cover 成品尺寸" || testitem == "ROTATION  W/UM" || testitem == "holder shear") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "AA") {
      if (testitem == "ROTATION  W/UM" || testitem == "holder shear" || testitem == "尺寸高" ||
        testitem == "尺寸宽" || testitem == "尺寸长" || testitem == "UV后胶厚（上）" ||
        testitem == "UV后胶厚（下）" || testitem == "UV后胶厚（左）" || testitem == "UV后胶厚（右）" ||
        testitem == "UV后胶宽（上）" || testitem == "UV后胶宽（下）" || testitem == "UV后胶宽（左）" ||
        testitem == "UV后胶宽（右）") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "VCM封合") {
      if (testitem == "浮高Z" || testitem == "偏心X" || testitem == "偏心Y" || testitem == "胶水推力" ||
        testitem == "ROTATION  W/UM" || testitem == "尺寸长" || testitem == "尺寸宽" || testitem == "尺寸高" ||
        testitem == "holder shear") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "锁付") {
      if (testitem == "锁付高度" || testitem == "锁付扭力") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "无螺牙锁附") {
      if (testitem == "无螺牙推力" || testitem == "无螺牙tilt" || testitem == "锁付高度" || testitem == "胶重") {
        flag = true
      } else {
        flag = false
      }

    } else if (opcode == "保护膜") {
      if (testitem == "保护膜角度" || testitem == "保护膜偏移") {
        flag = true
      } else {
        flag = false
      }

    } else {
      flag = false
    }
    return flag
  }

  def newWork_code(factoryfield: String, workshopcodefield: String): (String, String) = {
    var factoryfield = ""
    var work_code_new = ""
    if (workshopcodefield.equals("生产一区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹COB一区"
    } else if (workshopcodefield.equals("生产二区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹COB二区"
    } else if (workshopcodefield.equals("生产三区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹COB三区"
    } else if (workshopcodefield.equals("生产六区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹COB六区"
    } else if (workshopcodefield.equals("生产七区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹COB七区"
    } else if (workshopcodefield.equals("生产九区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹测试九区"
    } else if (workshopcodefield.equals("NP区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹试产区"
    } else if (workshopcodefield.equals("修复区")) {
      factoryfield = "台虹厂"
      work_code_new = "台虹修复区"
    } else if (workshopcodefield.equals("3D区")) {
      factoryfield = "台虹厂"
      work_code_new = workshopcodefield
    } else if (workshopcodefield.equals("古城后段一区")) {
      factoryfield = "古城一厂"
      work_code_new = "古一测试一区"
    } else if (workshopcodefield.equals("古城后段二区")) {
      factoryfield = "古城一厂"
      work_code_new = "古一测试二区"
    } else if (workshopcodefield.equals("古城试产二区")) {
      factoryfield = "古城一厂"
      work_code_new = "古一试产二区"
    } else if (workshopcodefield.equals("古城COB一区")) {
      factoryfield = "古城一厂"
      work_code_new = "古一COB一区"
    }else if (workshopcodefield.equals("古城COB二区")) {
      factoryfield = "古城一厂"
      work_code_new = "古一COB一区"
    } else if (workshopcodefield.equals("古二试产一区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二试产一区"
    } else if (workshopcodefield.equals("古二试产一区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二试产二区"
    }else if (workshopcodefield.equals("古二测试三区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二测试三区"
    }else if (workshopcodefield.equals("古二COB二区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二COB二区"
    } else if (workshopcodefield.equals("生产四区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二COB二区"
    } else if (workshopcodefield.equals("生产五区")) {
      factoryfield = "古城二厂"
      work_code_new = "古二COB二区"
    }else if (workshopcodefield.equals("SMT")) {
      factoryfield = "古城二厂"
      work_code_new = "古二SMT"
    } else {
      work_code_new = workshopcodefield
      factoryfield = factoryfield
    }

    (factoryfield, work_code_new)
  }


  def newWork_code(factoryfield: String, workshopcodefield: String, eattribute2field: String): (String, String) = {
    var factoryfield = ""
    var workshopcodeNew = ""

    if (workshopcodefield.equals("生产一区")) {
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")) {
        workshopcodeNew = "台虹COB一区"
      } else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")) {
        workshopcodeNew = "台虹测试一区"
      }
      factoryfield = "台虹厂"
    } else if (workshopcodefield.equals("生产二区")) {
      factoryfield = "台虹厂"
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")) {
        workshopcodeNew = "台虹COB二区"
      }else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")) {
        workshopcodeNew = "台虹测试二区"
      }
    } else if (workshopcodefield.equals("生产三区")) {
      factoryfield = "台虹厂"
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")) {
        workshopcodeNew = "台虹COB三区"
      } else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")){
        workshopcodeNew = "台虹测试三区"
      }
    } else if (workshopcodefield.equals("生产四区")) {
      factoryfield = "古城二厂"
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")) {
        workshopcodeNew = "古二COB二区"
      } else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")){
        workshopcodeNew = "古二测试三区"
      }
    } else if (workshopcodefield.equals("生产五区")) {
      factoryfield = "古城二厂"
      workshopcodeNew = "古二COB二区"
    } else if (workshopcodefield.equals("生产六区")) {
      factoryfield = "台虹厂"
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")){
        workshopcodeNew = "台虹COB六区"
      } else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")) {
        workshopcodeNew = "台虹测试六区"
      }
    } else if (workshopcodefield.equals("生产七区")) {
      factoryfield = "台虹厂"
      if (eattribute2field.equals("十级") || eattribute2field.equals("COB")){
        workshopcodeNew = "台虹COB七区"
      } else if(eattribute2field.equals("千级")|| eattribute2field.equals("测试")) {
        workshopcodeNew = "台虹测试七区"
      }
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

    (factoryfield, workshopcodeNew)
  }


}
