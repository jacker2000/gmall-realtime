package com.gf.gmallpublisher.service;

import com.gf.gmallpublisher.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface OrderService {

    List<NameValue> statsByItem(String itemName, String date, String type);

    Map<String, Object> detailByItem(String itemName, String date, Integer pageNo, Integer pageSize);

}
