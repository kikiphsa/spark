package com.atgugiu.bigdata.structure.labyrinth

/**
  * Create by chenqinping on 2019/5/29 13:27
  */
object HanoiTowerDemo {

  def main(args: Array[String]): Unit = {

    HanoiTower(3, 'A', 'B', 'C')

    println(0 % 5)

    ///∥汉诺塔的递归方法
    ///思路
    ////->如果只有一个盘，A->C
    ////->如果有两个或者两个以上盘，将看成两个部分，最下面盘，和上面的盘
    ////1.将上面的盘A->B//2.将最下的盘A->C
    ////3.将B塔的所有盘移动到C，B->C
    def HanoiTower(nums: Int, a: Char, b: Char, c: Char) {

      /*if (nums == 1) {
        println("第一个盘" + a + "->" + c)
      } else {
        HanoiTower(nums - 1, a, c, b)
        println("第" + nums + "个盘" + a + "->" + c)
        HanoiTower(nums - 1, b, a, c)
      }*/

      if (nums == 1) {
        println("第一个盘" + a + "->" + c)
      } else {
        HanoiTower(nums - 1, a, c, b)
        println("第" + nums + "个盘", a + "->" + c)
        HanoiTower(nums - 1, b, a, c)

      }
    }
  }
}

