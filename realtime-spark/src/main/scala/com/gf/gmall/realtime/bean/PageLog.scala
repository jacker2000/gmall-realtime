package com.gf.gmall.realtime.bean

case class PageLog(
                    var province_id: String,
                    var brand: String,
                    var channel: String,
                    var is_new: String,
                    var model: String,
                    var mid: String,
                    var operate_system: String,
                    var user_id: String,
                    var version_code: String,
                    var during_time: Long,
                    var page_item: String,
                    var page_item_type: String,
                    var last_page_id: String,
                    var page_id: String,
                    var sourceType: String,
                    var ts: Long
                  )
