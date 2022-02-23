package com.gf.gmall.realtime.bean

case class DauInfo(
                    //访问页面的基本字段
                    var province_id: String =null,
                    var brand: String,
                    var channel: String =null,
                    var is_new: String =null,
                    var model: String =null,
                    var mid: String  =null,
                    var operate_system: String =null,
                    var user_id: String  =null,
                    var version_code: String =null,

                    var during_time: Long =0L,
                    var page_item: String =null,
                    var page_item_type: String =null,
                    //last_page_id  :String,
                    var page_id: String =null,
                    //                  var   sourceType: String,

                    //用户维度信息 性别,年龄
                    var user_gender: String =null,
                    var user_age: String =null, //当前时间-出生日期

                    //地区维度信息
                    var province_name: String,
                    var province_iso_code: String,
                    var province_iso_3166_2: String,

                    var province_area_code: String,

                    //日期维度信息
                    var dt: String, //日期时间
                    var hr: String, //
                    var ts: Long //日期毫秒值

                  ) {
  def this() { //辅助构造
    //辅助构造器，生成无参构造，this调用有参构造，赋初值
    this(null, null, null, null, null, null, null, null, null, 0L, null, null, null, null, null, null, null, null, null, null, null, 0L)
  }
}
