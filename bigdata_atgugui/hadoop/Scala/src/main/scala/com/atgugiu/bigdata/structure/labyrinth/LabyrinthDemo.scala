package com.atgugiu.bigdata.structure.labyrinth

/**
  * Create by chenqinping on 2019/5/29 11:42
  */
object LabyrinthDemo {

  def main(args: Array[String]): Unit = {
    //创建迷宫
    val map = Array.ofDim[Int](8, 7)

    //给map加墙 1
    for (i <- 0 until 7) {
      map(0)(i) = 1
      map(7)(i) = 1
    }

    for (i <- 0 until 8) {
      map(i)(0) = 1
      map(i)(6) = 1
    }
    map(3)(1)=1
    map(3)(2)=1
    map(2)(2)=1
    println("迷宫")
    for (row <- map) {
      for (elem <- row) {
        printf("%d \t", elem)
      }
      println()
    }

    //测试

    setWay1(map,1,1)
    println("小球找后的结果")
    for (row <- map) {
      for (elem <- row) {
        printf("%d \t", elem)
      }
      println()
    }
    /**
      * //找路
      *
      * @param map 地图
      * @param i   探测 横坐标
      * @param j   探测 纵坐标
      */
    def setWay(map: Array[Array[Int]], i: Int, j: Int): Boolean = {
      if (map(6)(5) == 2) {
        println("通路找到了")
        return true
      } else {
        //0 , 1, 2, 3
        if (map(i)(j) == 0) {
          //没有探测过
          // 使用自己定好策略进行探测（下->右->上->左）
          //假定可以走通的,但是不一定
          map(i)(j) = 2
          //下走
          if (setWay(map, i + 1, j)) {
            //下
            return true
          } else if (setWay(map, i, j + 1)) {
            //右
            return true
          } else if (setWay(map, i - 1, j)) {
            //上
            return true
          } else if (setWay(map, i, j - 1)) {
            //左
            return true
          } else {
            map(i)(j) = 3 //四个方向都走不通
            return false
          }

        } else { //1,2,3
          return false
        }
      }
    }



    def setWay1(map: Array[Array[Int]], i: Int, j: Int): Boolean = {
      if (map(6)(5) == 2) {
        println("通路找到了")
        return true
      } else {
        //0 , 1, 2, 3
        if (map(i)(j) == 0) {
          //没有探测过
          // 使用自己定好策略进行探测(下->右->上->左)
          //假定可以走通的,但是不一定
          map(i)(j) = 2
          //下走
          if (setWay1(map, i - 1, j)) {
            //下
            return true
          } else if (setWay1(map, i, j + 1)) {
            //右
            return true
          } else if (setWay1(map, i + 1, j)) {
            //上
            return true
          } else if (setWay1(map, i, j - 1)) {
            //左
            return true
          } else {
            map(i)(j) = 3 //四个方向都走不通
            return false
          }

        } else { //1,2,3
          return false
        }
      }
    }

  }
}
