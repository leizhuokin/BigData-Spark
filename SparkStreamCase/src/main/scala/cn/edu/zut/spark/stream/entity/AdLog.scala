package cn.edu.zut.spark.stream.entity

case class AdLog(timestamp: Long,
                 area: String,
                 cityName: String,
                 userId: String,
                 adId: String)
