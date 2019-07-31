package com.atguigu.bigdata.zhaocheng.bean

/**
  * Create by chenqinping on 2019/6/11 18:10
  */
object AlertType extends Enumeration{
  val CouponFraud =Value(1,"优惠券欺诈")
  val Other = Value(2,"其他")
}

case class AlertEvent(mid:String,alterType:AlertType.Value ,uids:String,startTs:Long) {

}

