package com.gf.gmall.realtime.util

object DateUtils {

  import java.util.Calendar

  /**
   * 判断当前时间距离第二天凌晨的秒数
   *
   * @return 返回值单位为[s:秒]
   */
  def getSecondsNextEarlyMorning: Long = {
    val cal: Calendar = Calendar.getInstance
    cal.add(Calendar.DAY_OF_YEAR, 1)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    (cal.getTimeInMillis - System.currentTimeMillis) / 1000
  }
}
