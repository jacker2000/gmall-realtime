package com.gf.springbootdemo.springbootdemo.mapper;

import com.gf.springbootdemo.springbootdemo.bean.User;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据层
 */
@Repository
public class UserMapperImpl implements UserMapper {
    @Override
    public void saveUser(User user) {

        //JDBC
        System.out.println("将user对象写入到数据库成功");
    }

    @Override
    public List<User> searchAllUser() {
        List<User> users = new ArrayList<>();
        users.add(new User("zhangsan","111",22));
        users.add(new User("lisi","222",23));
        users.add(new User("wangwu","333",24));

        return users;
    }
}
