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
        //分男女算日均支出
        result.put("avgList",getAvg());
        //分类消费比重
        result.put("consumePer",getPercent());
        //食堂流量
        result.put("resFlow",getRes());
        //澡堂流量
        result.put("bathFlow",getBath());
        //商铺流量
        result.put("shopFlow",getShop());
        result.put("date",new Date().toString());
        if(!cache.exists()){
           cache.createNewFile();
        }
        FileUtils.writeStringToFile(cache,JSONObject.toJSONString(result),"utf-8");
        return true;
    }

    private JSONArray getAvg(){
        String sql = "select sum(money)/count(distinct(id)) ,sex,DAY(TO_DATE(time)) from consume group by DAY(TO_DATE(time)),sex";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray avg=new JSONArray();
        for(Map<String, Object> map : list){
               JSONObject object=new JSONObject();
               object.put("avg",map.get("_c0"));
               object.put("sex",map.get("sex"));
               object.put("day",map.get("_c2"));
               avg.add(object);
        }
        return avg;
    }

    private JSONArray getPercent(){
        String sql = "select sum(money),consumetype from consume group by consumetype";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray percent=new JSONArray();
        for(Map<String, Object> map : list){
            JSONObject object=new JSONObject();
            object.put("sum",map.get("_c0"));
            object.put("consumetype",map.get("consumetype"));
            percent.add(object);
        }
        return percent;
    }

    private JSONArray getRes(){
        String sql = "select count(id),HOUR(time),MINUTE (time) from consume where (machineno>'10032' and machineno<'10076')  group by HOUR(time),MINUTE (time)";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray flow=new JSONArray();
        for(Map<String, Object> map : list){
            JSONObject object=new JSONObject();
            object.put("count",map.get("_c0"));
            object.put("hour",map.get("_c1"));
            object.put("minute",map.get("_c2"));
            flow.add(object);
        }
        return flow;
    }

    private JSONArray getBath(){
        String sql = "select count(id),HOUR(time),MINUTE (time) from consume where (machineno>'10082' and machineno<'10085')  group by HOUR(time),MINUTE (time)";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray flow=new JSONArray();
        for(Map<String, Object> map : list){
            JSONObject object=new JSONObject();
            object.put("count",map.get("_c0"));
            object.put("hour",map.get("_c1"));
            object.put("minute",map.get("_c2"));
            flow.add(object);
        }
        return flow;
    }

    private JSONArray getShop(){
        String sql = "select count(id) ,company from consume where (companytype=1)  group by company";
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        JSONArray flow=new JSONArray();
        for(Map<String, Object> map : list){
            JSONObject object=new JSONObject();
            object.put("count",map.get("_c0"));
            object.put("company",map.get("company"));
            flow.add(object);
        }
        return flow;
    }

}
