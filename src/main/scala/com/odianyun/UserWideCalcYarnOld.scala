package com.odianyun

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by user on 2017/9/19.
  */
object UserWideCalcYarnOld {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","D:\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("UserOld")
    val sc = new SparkContext(conf)
    val hiveCxt = new HiveContext(sc)
    hiveCxt.setConf("spark.sql.shuffle.partitions","1")
    val rows = hiveCxt.sql("SELECT a.id,a.username,a.mobile,a.telephone,a.email,a.address," +
      "c.merchant_id,c.level_code,c.level_name,c.menbership_level_type," +
      "d.member_type,d.member_type_name " +
      " FROM rds.u_user a,rds.uc_user_membership_level b,rds.uc_membership_level c,rds.uc_member_type d " +
      " where a.id = b.entity_id " +
      " and b.membership_level_code = c.level_code " +
      " and c.member_type = d.member_type")
    rows.registerTempTable("bi_result")
    hiveCxt.sql("insert into bi.bi_user partition(dt='\"+NowDate+\"') select * from bi_result")
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
