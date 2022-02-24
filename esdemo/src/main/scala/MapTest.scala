object MapTest {
  def main(args: Array[String]): Unit = {
//    val  scores=Map("Alice"->10,"Bob"->3,"Cindy"->8)
    //     获取所有的key
//    val nameList=scores.map(_._1)
//    //     map 函数返回List
//    println(nameList.getClass)
////    遍历list中的元素
//    nameList.foreach((x:String)=>print(x+" "))
//    输出 ：Alice Bob Cindy

    //     或取所有的value

//    val   resultList=scores.map(_._2)
//    resultList.foreach {(x:Int)=>print(x+" ") }
////    输出：10 3 8
////    对于Tuple可以使用一样的方法
//
//    val  scores=List((1,"Alice",10),(2,"Bob",30),(3,"Cindy",50))
//    //   获取所有Tuples中的第三个元素
//    val  scoreList=scores.map(_._3)
//    for (scores<-scoreList){
//      print(scores +" ")
//    }
////    反向操作可以使用zip，将两个list转化为一个map，其中一个list作为key，另一个作为value
//
    val  keyList=List("Alice","Bob","Cindy")
    val valueList=List(10,3,8)
    val scores=keyList.zip(valueList).toMap
    println(scores)
//        Map(Alice -> 10, Bob -> 3, Cindy -> 8)
  }
}
