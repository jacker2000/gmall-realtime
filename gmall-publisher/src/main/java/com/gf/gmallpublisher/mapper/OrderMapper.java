package com.gf.gmallpublisher.mapper;

import com.gf.gmallpublisher.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String itemName, String date, Integer pageNo, Integer pageSize);
}
