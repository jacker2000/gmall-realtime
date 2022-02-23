package com.gf.springbootdemo.springbootdemo.service;

import com.gf.springbootdemo.springbootdemo.bean.User;

import java.util.List;

/*
   业务层接口
 */
public interface UserService {
    void registService(User user);

    List<User> searchUserService();

}
