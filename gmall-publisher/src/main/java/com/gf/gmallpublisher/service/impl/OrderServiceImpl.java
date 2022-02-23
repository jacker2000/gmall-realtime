package com.gf.gmallpublisher.service.impl;

import com.gf.gmallpublisher.bean.NameValue;
import com.gf.gmallpublisher.mapper.OrderMapper;
import com.gf.gmallpublisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    OrderMapper orderMapper;

    @Override
    public List<NameValue> statsByItem(String itemName, String date, String type) {

        List<NameValue> results = orderMapper.searchStatsByItem(itemName, date, typeToField(type));
        return transformRestult(type,results);
//        return  results ;
    }

    @Override
    public Map<String, Object> detailByItem(String itemName, String date, Integer pageNo, Integer pageSize) {
        return orderMapper.searchDetailByItem(itemName,date,pageNo,pageSize) ;
    }

    /**
     * 将查询类型转换结果
     */
    private List<NameValue> transformRestult(String type, List<NameValue> results) {
        if ("gender".equals(type)) {
            for (NameValue nameValue : results) {
                if (nameValue.getName().equals("M")) {
                    nameValue.setName("男");
                } else {
                    nameValue.setName("女");
                }
            }
            return results;
        } else if ("age".equals(type)) {
            //30以下
            double amount30 = 0;

            //30-50
            double amount30To50 = 0;

            //50以上
            double amount50 = 0;
            for (NameValue nameValue : results) {
                int name = Integer.parseInt(nameValue.getName());
                double value = (double) nameValue.getValue();
                if (name < 30) {
                    amount30 += value;
                } else if (name <= 50) {
                    amount30To50 += value;
                } else {
                    amount50 += value;
                }
            }
            results.clear();
            results.add(new NameValue("30岁以下", amount30));
            results.add(new NameValue("30-50岁", amount30To50));
            results.add(new NameValue("50岁以上", amount50));
            return results;
        } else {
            return new ArrayList<>();
        }
    }


    /**
     * 将查询的类型转换成查询的字段
     *
     * @param type
     * @return
     */
    private String typeToField(String type) {
        if ("gender".equals(type)) {
            return "user_gender";
        } else if ("age".equals(type)) {
            return "user_age";
        } else {
            return null;
        }
    }
}
