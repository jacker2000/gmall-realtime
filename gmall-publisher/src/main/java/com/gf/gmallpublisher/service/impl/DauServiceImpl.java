package com.gf.gmallpublisher.service.impl;

import com.gf.gmallpublisher.mapper.DauMapper;
import com.gf.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DauServiceImpl implements DauService {
    @Autowired
    DauMapper dauMapper;
    @Override
    public Map<String, Object> searchDauHour(String td) {
        return dauMapper.searchDauHourFromEs(td);
    }
}
