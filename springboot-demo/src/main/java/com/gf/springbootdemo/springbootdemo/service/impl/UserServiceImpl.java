package com.gf.springbootdemo.springbootdemo.service.impl;

import com.gf.springbootdemo.springbootdemo.bean.User;
import com.gf.springbootdemo.springbootdemo.mapper.UserMapper;
import com.gf.springbootdemo.springbootdemo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    UserMapper userMapper;
    @Override
    public void registService(User user) {
        //数据层
        System.out.println("UserServiceImpl");

        // ....业务处理

        //将对象持久化
        userMapper.saveUser(user);
    }

    @Override
    public List<User> searchUserService() {
        return userMapper.searchAllUser();
    }


}
