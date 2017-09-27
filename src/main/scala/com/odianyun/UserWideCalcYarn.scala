package com.odianyun

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * Created by user on 2017/9/18.
  */
object UserWideCalcYarn {
  def main(args: Array[String]): Unit = {

  val spark =SparkSession.builder.appName("UserWideCalc").enableHiveSupport().getOrCreate()
  spark.conf.set("spark.yarn.jars","hdfs://heimdall01.test.hadoop.com:8020/user/oozie/share/lib/lib_20170908120310/spark2/*")
  // 执行spark sql语句
  val rows = spark.sql("SELECT a.id,a.username,a.mobile,a.telephone,a.email,a.address," +
    " c.merchant_id,c.level_code,c.level_name,c.menbership_level_type," +
    " d.member_type,d.member_type_name" +
    " FROM rds.u_user a,rds.uc_user_membership_level b,rds.uc_membership_level c,rds.uc_member_type d" +
    " where a.id = b.entity_id" +
    " and b.membership_level_code = c.level_code" +
    " and c.member_type = d.member_type")
    // 将DataFrame注册为一张表
    //  rows.createOrReplaceTempView("bi_result")
    // 防止生成很多小文件
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","1")
    // 将关联查询结果插入到hive的分区表中
    //  spark.sql("insert into bi.bi_user partition(dt='"+NowDate+"') select * from bi_result")
    val now = NowDate
    rows.rdd.saveAsTextFile("/user/hive/warehouse/bi.db/bi_user/"+now)
    spark.sql("alter table bi.bi_user set location /user/hive/warehouse/bi.db/bi_user/"+now)
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
