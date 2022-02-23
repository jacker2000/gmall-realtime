package com.gf.springbootdemo.springbootdemo.controller;
import com.gf.springbootdemo.springbootdemo.bean.User;
import com.gf.springbootdemo.springbootdemo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 *  控制层:
 *      1.接收前端的请求
 *      2.调用业务层
 *      3.接收业务层返回的结果
 *      4.给前端响应结果
 */
@RestController
public class UserController {

    @Autowired
    @Qualifier("userServiceImpl")
    UserService userService;
    /**
     *  客户端请求:
     *      http://localhost:8080/regist
     *      http://localhost:8080/login
     *      http://localhost:8080/update
     *      http://localhost:8080/search
     * @return
     */
    @RequestMapping("/regist")
//    @ResponseBody //json方式
    public String handleRegist(){
        return "sucess";
    }
    /**
     * todo
     *  请求参数: 键值对参数
     *  http://localhost:8080/regist1?username=zhangsan&age=22
     *  请求方式：  GET(查)  POST(写)  PUT DELETE .....
     */
    @RequestMapping(value = "/regist1",method = RequestMethod.GET)
    public String handleRegist1(){
        return "sucess";
    }

    /**
     * todo
     *  请求参数: 请求路径中的键值对参数
     *  http://localhost:8080/regist2?username=zhangsan&age=22
     *  参数:
     *    username = zhangsan
     *    age = 22
     *  如果请求参数名与请求方法的形参名一致， 可以直接映射.
     *  如果名字不一致，可以通过@RequestParam("请求参数名") 指定映射到对应的方法形参上.
     *
     */
    @RequestMapping(value = "/regist2")
    public String handleRegist2(String username,Integer age){
        return "username="+username+",age="+age;
    }
    /**
     * todo
     *  请求参数:  获取请求地址中拼接的参数
     *  http://localhost:8080/regist3/zhangsan/33
     *  通过@PathVariable("占位符的名字") 将请求路径中的值映射到请求方法的形参上
     */
    @RequestMapping(value = "/regist3/{username}/{age}")
    public String handleRegist3(@PathVariable("username") String username, @PathVariable("age")Integer age){
        return "username="+username+",age="+age;
    }
    /**
     * 请求参数: 请求体中的参数
     *
     * 通过@RequestBody将请求体中的参数直接映射到Bean对象对应的属性上.
     */
    @PostMapping(value = "/regist4")
    public String handleRegist4(@RequestBody User user){
        return "username="+user.getUsername()+",age="+user.getAge();
    }

    /**
     *  注册
     */
    @PostMapping(value = "/regist5")
    public String handleRegist5(@RequestBody User user){
        userService.registService(user);
        return "username="+user.getUsername()+",age="+user.getAge();
    }

    /**
     * 查询所有的用户
     * localhost:8080/searchUser
     */
    @GetMapping("/searchUser")
    public List<User> searchAllUser(){
        List<User> users=userService.searchUserService();
        return users; //jackson
    }


}
