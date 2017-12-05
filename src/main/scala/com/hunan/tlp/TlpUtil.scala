package com.hunan.tlp

import java.text.SimpleDateFormat


object TlpUtil {
  def convertTimeStr(timeStr:String, format:String): Long ={
    val timeFormatter = new SimpleDateFormat(format)
    val date = timeFormatter.parse(timeStr)
    date.getTime/1000
  }
}
