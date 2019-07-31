package copy

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Create by chenqinping on 2019/6/12 15:47
  */
object Test {

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val date = new Date()
    val str: String = format.format(date)

    println(str)

  }

}
