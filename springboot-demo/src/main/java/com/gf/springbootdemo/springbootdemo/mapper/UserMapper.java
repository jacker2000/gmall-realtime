package com.gf.springbootdemo.springbootdemo.mapper;

import com.gf.springbootdemo.springbootdemo.bean.User;

import java.util.List;

public interface UserMapper {

    void saveUser(User user);

    List<User> searchAllUser();

}
