package com.gf.gmall.realtime.util

import java.lang.reflect.{Field, Method, Modifier}

import com.gf.gmall.realtime.bean.{DauInfo, PageLog}

import scala.util.control.Breaks

object MyBeanUtils {
  def main(args: Array[String]): Unit = {
    val pageLog: PageLog = PageLog("101", null, null, null, null, "m1001", null, "1001", null, 0L, null, null, null, null, null, 0L)
    val dauInfo = new DauInfo() // todo 对于目标对象的属性必须是var，否则无法赋值
    copyProperties(pageLog,dauInfo)
    println(dauInfo)
  }
  def copyProperties(srcObj:Any,destObj:Any): Unit ={
    if(srcObj==null ||destObj==null) return
    //获取目标对象中所有属性
    val allfields: Array[Field] = destObj.getClass.getDeclaredFields

    for (field <- allfields) {

      Breaks.breakable{
        //判断修饰符是否为final
        if (Modifier.isFinal(field.getModifiers)) {
          Breaks.break()
        }
        //强制访问
        field.setAccessible(true)
        //get:field()
        val getMethodName = field.getName
        //set:field_$eq
        val setMethodName = field.getName + "_$eq"

        //从源对象中获取get方法
        val getMethod: Method = try {
          srcObj.getClass.getDeclaredMethod(getMethodName)
        } catch {
          case ex : Exception => Breaks.break()
        }
        //从目标对象中获取set方法
        val setMethod: Method = destObj.getClass.getDeclaredMethod(setMethodName, field.getType)
        //从源对象中获取属性值，设置到目标对象中
        setMethod.invoke(destObj,getMethod.invoke(srcObj))
      }
    }
  }
}
