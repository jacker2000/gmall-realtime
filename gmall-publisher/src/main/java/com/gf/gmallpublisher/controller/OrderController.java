package com.gf.gmallpublisher.controller;

import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

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

}
