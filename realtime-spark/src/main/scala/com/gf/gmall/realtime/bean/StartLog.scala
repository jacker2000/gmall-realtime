package com.gf.gmall.realtime.bean

case class StartLog(
                     var mid: String,
                     var user_id: String,
                     var province_id: String,
                     var channel: String,
                     var is_new: String,
                     var model: String,
                     var operate_system: String,
                     var version_code: String,
                     var entry: String,
                     var loading_time: Long,
                     var open_ad_id: String,
                     var open_ad_ms: Long,
                     var open_ad_skip_ms: Long,
                     var ts: Long
                   )
