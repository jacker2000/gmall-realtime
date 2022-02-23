package com.gf.gmall.realtime.bean

case class OrderInfo(
                      var id: Long = 0L,
                      var province_id: Long = 0L,
                      var order_status: String = null,
                      var user_id: Long = 0L,
                      var total_amount: Double = 0D,
                      var activity_reduce_amount: Double = 0D,
                      var coupon_reduce_amount: Double = 0D,
                      var original_total_amount: Double = 0D,
                      var feight_fee: Double = 0D,
                      var feight_fee_reduce: Double = 0D,
                      var create_time: String = null,
                      var operate_time: String = null,
                      var expire_time: String = null,
                      var refundable_time: String = null,
                      //其他字段处理得到
                      var create_date: String = null,
                      var create_hour: String = null,
                      //地区维度信息
                      var province_name: String = null,
                      var province_area_code: String = null,
                      var province_iso_code: String = null,
                      var province_iso_3166_2: String = null,
                      //用户维度信息 性别,年龄
                      var user_gender: String = null,
                      var user_age: Int = 0, //当前时间-出生日期
                    ) {}
