package com.odianyun

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by user on 2017/9/18.
  * 计算用户宽表的数据
  */
object UserWideCalc {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\hadoop-common-2.2.0-bin-master")
//    val conf = new SparkConf().setAppName("UserWideCalc").setMaster("local[4]")
//    val sc = new SparkContext(conf)
//    val hiveCtx = new HiveContext(sc)
//    val rows = hiveCtx.sql("select * from rds.uc_memeber_type limit 10")
//    // 需要转换成rdd或者需要新建一个encoder
//    val keys = rows.rdd.map(row=>row.getString(0))
//    keys.foreach(key=>print(key)+",")
    val spark =SparkSession.builder.master("local[*]").appName("UserWideCalc").enableHiveSupport().getOrCreate()
    val rows = spark.sql("SELECT a.id,a.username,a.mobile,a.telephone,a.email,a.address," +
      " c.merchant_id,c.level_code,c.level_name,c.menbership_level_type," +
      " d.member_type,d.member_type_name" +
      " FROM rds.u_user a,rds.uc_user_membership_level b,rds.uc_membership_level c,rds.uc_member_type d" +
      " where a.id = b.entity_id" +
      " and b.membership_level_code = c.level_code" +
      " and c.member_type = d.member_type")
//    val keys = rows.rdd.map(row=>row.get(0))
//    keys.foreach(key=>println(key+","))
//    rows DF
      rows.createOrReplaceTempView("tmp");
      spark.sql("insert into bi.bi_user partition(dt='"+NowDate+"') select * from tmp")
//      rows.rdd.saveAsTextFile("/user/hive/warehouse/")
      spark.stop()
  }

  /**
    * 获取当前时间
    * @return
    */
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(now)
    return date
  }
}
