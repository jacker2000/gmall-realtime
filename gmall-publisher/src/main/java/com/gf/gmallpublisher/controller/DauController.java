package com.gf.gmallpublisher.controller;

import com.gf.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class DauController {

    @Autowired
    DauService dauService;
    /**
     * 请求URL:
     *      http://bigdata.gmall.com/dauRealtime?td=2022-01-01
     *
     *  响应结果：
     *      { dauTotal:123,
     *         dauYd:{"12":90,"13":33,"17":166 },
     *         dauTd:{"11":232,"15":45,"18":76}
     *      }
     */
    @GetMapping("dauRealtime")
    public Map<String,Object> dauRealtime(@RequestParam("td") String td){
        Map<String,Object> results=dauService.searchDauHour(td);
        return results;
    }
}
