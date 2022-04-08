package com.example.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hive2")
public class HiveController {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    File cache=new File("./data.json");

    @RequestMapping("/list")
    public JSONObject list() {
        String str="{}";
        try {
            str = FileUtils.readFileToString(cache, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JSONObject.parseObject(str);
    }

    @RequestMapping("/update")
    public boolean update() throws IOException {
        JSONObject result=new JSONObject();
        //食堂流量
        result.put("resFlow",getRes());
        result.put("date",new Date().toString());
        if(!cache.exists()){
            cache.createNewFile();
        }
        FileUtils.writeStringToFile(cache,JSONObject.toJSONString(result),"utf-8");
        return true;
    }



    private JSONArray getRes(){
        String sql = "select count(*),YEAR(time),MONTH(time),DAY(time),HOUR(time) from restrauntFlow group by YEAR(time),MONTH(time),DAY(time),HOUR(time)";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray flow=new JSONArray();
        for(Map<String, Object> map : list){
            JSONObject object=new JSONObject();
            object.put("count",map.get("_c0"));
            object.put("year",map.get("_c1"));
            object.put("month",map.get("_c2"));
            object.put("day",map.get("_c3"));
            object.put("hour",map.get("_c4"));
            flow.add(object);
        }
        return flow;
    }



}
