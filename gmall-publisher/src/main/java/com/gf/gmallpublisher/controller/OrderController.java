package com.gf.gmallpublisher.controller;

import com.gf.gmallpublisher.bean.NameValue;
import com.gf.gmallpublisher.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
public class OrderController {

    @Autowired
    OrderService orderService;

    /**
     *  通过类型查询明细
     *      http://bigdata.gmall.com/detailByItem?date=2021-02-02
     *          &itemName=小米手机&pageNo=1&pageSize=20
     */
    @GetMapping("/detailByItem")
    public Map<String,Object> detailByItem(@RequestParam("itemName") String itemName,
                                           @RequestParam("date") String date,
                                           @RequestParam(value = "pageNo",required = false,defaultValue = "1")Integer pageNo  ,
                                           @RequestParam(value = "pageSize",required = false,defaultValue = "20")Integer  pageSize){
        Map<String,Object> resultMap=orderService.detailByItem(itemName,date,pageNo,pageSize);
        return resultMap;
    }


    /**
     *  通过类别统计
     *      http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     *           [
     *             { value: 1048, name: "男" },
     *             { value: 735, name: "女" } 
     *          ]
     *      http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     *            [
     *                  { value: 1048, name: "20岁以下" },
     *                  { value: 735, name: "20岁至29岁" } ,
     *                  { value: 34, name: "30岁以上" }  
     *          ]
     */
    @GetMapping("/statsByItem")
    public List<NameValue>  statsByItem(@RequestParam("itemName") String itemName,
                                        @RequestParam("date")String date,
                                        @RequestParam("t")String type){
        List<NameValue> nameValues = orderService.statsByItem(itemName,date,type);

        return nameValues;
    }



}
