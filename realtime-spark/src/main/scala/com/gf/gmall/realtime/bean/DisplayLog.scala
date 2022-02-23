package com.gf.gmall.realtime.bean

case class DisplayLog(
                       var mid: String,
                       var user_id: String,
                       var province_id: String,
                       var channel: String,
                       var is_new: String,
                       var model: String,
                       var operate_system: String,
                       var version_code: String,
                       var page_id: String,
                       var last_page_id: String,
                       var page_item: String,
                       var page_item_type: String,
                       var during_time: Long,
                       var displayType: String,
                       var display_item: String,
                       var display_item_type: String,
                       var order: String,
                       var pos_id: String,
                       var ts: Long
                     )
