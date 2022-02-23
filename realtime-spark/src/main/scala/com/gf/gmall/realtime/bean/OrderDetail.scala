package com.gf.gmall.realtime.bean

case class OrderDetail(
                        var id: Long,
                        var order_id: Long,
                        var sku_id: Long,
                        var order_price: Double,
                        var sku_num: Long,
                        var sku_name: String,
                        var create_time: String,
                        var split_total_amount: Double = 0D,
                        var split_activity_amount: Double = 0D,
                        var split_coupon_amount: Double = 0D
                      ) {}
