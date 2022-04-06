package com.example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hive2")
public class HiveController {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    //分男女算日均支出
    @RequestMapping("/list")
    public List<Map<String, Object>> list() {
        String sql = "select sum(money)/count(distinct(id)) ,sex,DAY(TO_DATE(time)) from consume group by DAY(TO_DATE(time)),sex";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        return list;
    }


    //分类消费比重
    @RequestMapping("/list2")
    public List<Map<String, Object>> list2() {
        String sql = "select sum(money),consumetype from consume group by consumetype";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        return list;
    }

    //食堂流量
    @RequestMapping("/list3")
    public List<Map<String, Object>> list3() {
        String sql = "select count(id),HOUR(time),MINUTE (time) from consume where (machineno>'10032' and machineno<'10076')  group by HOUR(time),MINUTE (time)";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        return list;
    }

    //澡堂流量
    @RequestMapping("/list4")
    public List<Map<String, Object>> list4() {
        String sql = "select count(id),HOUR(time),MINUTE (time) from consume where (machineno>'10082' and machineno<'10085')  group by HOUR(time),MINUTE (time)";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        return list;
    }


    //澡堂流量
    @RequestMapping("/list5")
    public List<Map<String, Object>> list5() {
        String sql = "select count(id) ,company from consume where (companytype=1)  group by company";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        return list;
    }

}
