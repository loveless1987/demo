package com.watercube.rocketspringbootconsumer.receive.service.impl;

import ch.qos.logback.core.joran.spi.ElementSelector;
import com.alibaba.fastjson.JSONObject;
import com.watercube.rocketspringbootconsumer.receive.service.ReceiveService;
import com.watercube.rocketspringbootconsumer.unit.DateUtils;
import io.netty.util.internal.StringUtil;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.freemarker.FreeMarkerTemplateAvailabilityProvider;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class ReceiveServiceImpl implements ReceiveService {

    @Autowired
    @Qualifier("jdbcTemplateOne")
    JdbcTemplate jdbcTemplateOne;

    @Resource(name = "jdbcTemplateTwo")
    JdbcTemplate jdbcTemplateTwo;

    @Override
    public void messageReceiveTest(MessageExt ext,Integer n) {
        System.out.println("接收到的消息为：" + n);
        //记录时间
        System.out.println("接收时间：" + (new Date().getTime()));
    }

    //接收消息
    @Override
    public void messageReceive(MessageExt ext){
        //获取当前时间
        Date date = new Date();
        //处理业务逻辑
        try {
            System.out.println("接收到的消息为：" + new String(ext.getBody(),"UTF-8"));
            //记录时间
            System.out.println("接收时间：" + DateUtils.convertStr(date,"yyyy-MM-dd HH:mm:ss"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //设置超时时间配置
        jdbcTemplateTwo.setQueryTimeout(120);
        jdbcTemplateOne.setQueryTimeout(120);

        //解析数据
        JSONObject jsonObject = JSONObject.parseObject(new String(ext.getBody()));
        //数据值
        String val = jsonObject.getString("dataValue");
        //采集时间
        String tm = jsonObject.getString("tm").replace("T", " ");
        //数据类型
        String dataTypeCode = jsonObject.getString("dataTypeCode");
        //预定义SQL语句
        //查询sql语句、插入sql语句、更新sql语句
        String selectSql, insertSql, updateSql;
        //定义最大水位、最小水位、最大流量、最小流量
        String maxz, minz, maxz_qx, minz_qx;

        String q = "NULL";
        Map<String, Object> map;
        Map<String, Object> map2 = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        List<Map<String, Object>> list2 = new ArrayList<>();
        List<Map<String, Object>> list3 = new ArrayList<>();

        //测站编码
        String stcd = jsonObject.getString("telemetryStationCode");
        if(StringUtil.isNullOrEmpty(stcd) || stcd.length() <= 2){
            return;
        }
        //截取测站编码，去掉起始的00
        stcd = stcd.substring(2);
        //记录编码用于后续的数据校验
        map2.put("stcd", stcd);
        if(!stcd.equals(null)&&!stcd.equals("")) {
            selectSql = " select CASE when stcd_new IS NOT NULL THEN stcd_new ELSE stcd END stcd  from st_stbprp_b where 1=1 and (datatype in ('MK','LL','NEW','byh','SQ') or issq='01' or sttp='SQ')";
            //获取44.8上的测站编码集合
            list2 = jdbcTemplateTwo.queryForList(selectSql);
            //获取此站点对应的新旧测站编码
            String sql02 = "select stcd,stcd_new, from st_stbprp_b where stcd='" + stcd + "' or stcd_new='" + stcd + "'";
            list3 = jdbcTemplateTwo.queryForList(sql02);
            //获取测站 最大最小值
            selectSql = "select (case\n" +
                    "             when maxz is not null then\n" +
                    "              maxz\n" +
                    "             else\n" +
                    "              -1\n" +
                    "           end) MAXZ,\n" +
                    "           (case\n" +
                    "             when minz is not null then\n" +
                    "              minz\n" +
                    "             else\n" +
                    "              -1\n" +
                    "           end) MINZ \n" +
                    "      from st_stbprp_b\n" +
                    "     where stcd = '" + stcd + "'";
            list = jdbcTemplateOne.queryForList(selectSql);
            if (list.size() > 0) {
                try {
                    maxz = list.get(0).get("MAXZ").toString();
                    minz = list.get(0).get("MINZ").toString();
                } catch (Exception e) {
                    maxz = "-1";
                    minz = "-1";
                }
            } else {
                maxz = "-1";
                minz = "-1";
            }
            stcd = checkNewST(stcd);
            switch (dataTypeCode) {
                //水位
                case "39":
                    stcd = getString(stcd);
                    //判断是否需要计算流量
                    selectSql = "select count(1) C from  st_bxqx_be where stcd ='" + stcd + "'";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (!map.get("C").toString().equals("0")) {
                        selectSql = "select max(z) MAXZ, min(z) MINZ from st_bxqx_be where stcd ='" + stcd + "'";
                        list = jdbcTemplateOne.queryForList(selectSql);
                        if (list.size() > 0)  //存在推流曲线，啧计算推流
                        {
                            maxz_qx = list.get(0).get("MAXZ").toString();
                            minz_qx = list.get(0).get("MINZ").toString();
                            //在范围内的计算流量
                            if (Float.parseFloat(val) <= Float.parseFloat(maxz_qx) && Float.parseFloat(val) >= Float.parseFloat(minz_qx)) {
                                selectSql = "select Z,Q\n" +
                                        "     from st_bxqx_be\n" +
                                        "     where stcd = '" + stcd + "'\n" +
                                        "     and Z = (select to_number(max(Z))\n" +
                                        "     from st_bxqx_be\n" +
                                        "     where Z <= " + val + "\n" +
                                        "     and stcd =  '" + stcd + "')";
                                list = jdbcTemplateOne.queryForList(selectSql);
                                Float max_z = Float.parseFloat(list.get(0).get("Z").toString());
                                Float max_q = Float.parseFloat(list.get(0).get("Q").toString());
                                selectSql = "select Z,Q\n" +
                                        "     from st_bxqx_be\n" +
                                        "     where stcd = '" + stcd + "'\n" +
                                        "     and Z = (select to_number(min(Z))\n" +
                                        "     from st_bxqx_be\n" +
                                        "     where Z >= " + val + "\n" +
                                        "     and stcd =  '" + stcd + "')";
                                list = jdbcTemplateOne.queryForList(selectSql);
                                Float min_z = Float.parseFloat(list.get(0).get("Z").toString());
                                Float min_q = Float.parseFloat(list.get(0).get("Q").toString());
                                if (max_z != min_z) {
                                    q = String.valueOf((max_q - min_q) / (max_z - min_z) * (Float.parseFloat(val) - min_z) + min_q);
                                }
                            }
                            //比最小值还小流量计0
                            else if (Float.parseFloat(val) < Float.parseFloat(minz_qx)) {
                                q = "0";
                            }
                        }
                    }
                    //录入水位
                    selectSql = "select count(1) C from rtu_zq_river where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update rtu_zq_river set z=round('" + val + "',2) where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into rtu_zq_river(stcd,tm,z,mark) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2),'1')";
                        jdbcTemplateOne.execute(insertSql);
                    }
                    //记录正式数据
                    selectSql = "select count(1) C from st_river_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        //只记录符合要求的水位流量
                        if ((maxz.equals("-1") || Float.parseFloat(maxz) >= Float.parseFloat(val)) && (minz.equals("-1") || Float.parseFloat(minz) <= Float.parseFloat(val))) {
                            updateSql = "update st_river_r set z=round('" + val + "',2)";
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            if (val.equals("NaN")) {
                                val = "NULL";
                            }
                            if (!q.equals("NULL")) {
                                updateSql += ",q='" + q + "' ";
                            }
                            updateSql += " where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                            jdbcTemplateOne.execute(updateSql);

                        } else {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            insertSql = "insert into ST_RIVER_MIDE(stcd,tm,z,q) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2)," + q + ")";
                            jdbcTemplateOne.execute(insertSql);

                        }
                    } else {
                        //执行插入操作
                        //只记录符合要求的水位流量
                        if ((maxz.equals("-1") || Float.parseFloat(maxz) >= Float.parseFloat(val)) && (minz.equals("-1") || Float.parseFloat(minz) <= Float.parseFloat(val))) {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            if (val.equals("NaN")) {
                                val = "NULL";
                            }
                            insertSql = "insert into st_river_r(stcd,tm,z) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                            jdbcTemplateOne.execute(insertSql);

                        } else {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            insertSql = "insert into ST_RIVER_MIDE(stcd,tm,z) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                            jdbcTemplateOne.execute(insertSql);

                        }
                    }

                    //推送到44.8
                    if (list2.indexOf(map2) >= 0) {
                        if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                            stcd =  list3.get(0).get("stcd").toString();
                        }
                    }
                    selectSql = "select count(1) C from st_river_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        //只记录符合要求的水位流量
                        if ((maxz.equals("-1") || Float.parseFloat(maxz) >= Float.parseFloat(val)) && (minz.equals("-1") || Float.parseFloat(minz) <= Float.parseFloat(val))) {
                            updateSql = "update st_river_r set z=round('" + val + "',2)";
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            if (val.equals("NaN")) {
                                val = "NULL";
                            }
                            if (!q.equals("NULL")) {
                                //updateSql += ",q='" + q + "' ";
                            }
                            updateSql += " where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                            jdbcTemplateTwo.execute(updateSql);

                        } else {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            insertSql = "insert into ST_RIVER_MIDE(stcd,tm,z,q) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2)," + q + ")";
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    } else {
                        //执行插入操作
                        //只记录符合要求的水位流量
                        if ((maxz.equals("-1") || Float.parseFloat(maxz) >= Float.parseFloat(val)) && (minz.equals("-1") || Float.parseFloat(minz) <= Float.parseFloat(val))) {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            if (val.equals("NaN")) {
                                val = "NULL";
                            }
                            insertSql = "insert into st_river_r(stcd,tm,z) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                            jdbcTemplateTwo.execute(insertSql);

                        } else {
                            if (q.equals("NaN")) {
                                q = "NULL";
                            }
                            insertSql = "insert into ST_RIVER_MIDE(stcd,tm,z) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                //流速
                case "37":
                    break;
                //流量
                case "27":
                    stcd = getString(stcd);
                    selectSql = "select count(1) C from rtu_zq_river where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update rtu_zq_river set q=round('" + val + "',2) where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into rtu_zq_river(stcd,tm,q,mark) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2),'1')";
                        jdbcTemplateOne.execute(insertSql);
                    }

                    //记录正式数据
                    selectSql = "select count(1) C from st_river_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        if (q.equals("NaN")) {
                            q = "NULL";
                        }
                        if (val.equals("NaN")) {
                            val = "NULL";
                        }
                        //执行更新操作
                        updateSql = "update st_river_r set q=round('" + val + "',2) where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);

                    } else {
                        if (q.equals("NaN")) {
                            q = "NULL";
                        }
                        if (val.equals("NaN")) {
                            val = "NULL";
                        }
                        insertSql = "insert into st_river_r(stcd,tm,q) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                        jdbcTemplateOne.execute(insertSql);
                    }
                    //更新44.8上的数据
                    if (list2.indexOf(map2) >= 0) {
                        if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                            stcd =  list3.get(0).get("stcd").toString();
                        }
                    }
                    selectSql = "select count(1) C from st_river_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        if (q.equals("NaN")) {
                            q = "NULL";
                        }
                        if (val.equals("NaN")) {
                            val = "NULL";
                        }
                        //执行更新操作
                        if(stcd !="30700450" && stcd !="30700300") {
                            updateSql = "update st_river_r set q=round('" + val + "',2) where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        if (q.equals("NaN")) {
                            q = "NULL";
                        }
                        if (val.equals("NaN")) {
                            val = "NULL";
                        }
                        if(stcd !="30700450" && stcd !="30700300") {
                            insertSql = "insert into st_river_r(stcd,tm,q) values ('" + stcd + "',to_date('" + tm + "','yyyy-mm-dd hh24:mi:ss'),round('" + val + "',2))";
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                //10cm土壤含水量  Moisture
                //{"agreement":6,"dataType":"10厘米处土壤含水量","dataTypeCode":"10","dataUnit":"","dataValue":10.3,"factorTypeCode":6,"factorTypeName":"墒情","id":"0d8a9c3f5c41487ab6cb0391bdfeab56","telemetryStationCode":"0000003011","telemetryStationName":"东马各庄","tm":"2022-05-09 16:00:00"}

                /*
                * 2022-07-29
                * 分开查询本地和44.8上所存在的数据。
                * 可能存在某些情况导致本地插入了数据，但是44.8上还没有，导致44.8进入update，更新了空数据
                * */

                case "10":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m10='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m10) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                    }

                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m10='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        if (list2.indexOf(map2) >= 0) {

                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            System.out.println(updateSql);
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m10) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(insertSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                //20cm土壤含水量
                //{"agreement":6,"dataType":"20厘米处土壤含水量","dataTypeCode":"11","dataUnit":"","dataValue":11.3,"factorTypeCode":6,"factorTypeName":"墒情","id":"02f360f23542426194bb305261301ef3","telemetryStationCode":"0000003011","telemetryStationName":"东马各庄","tm":"2022-05-09 16:00:00"}
                case "11":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m20='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(updateSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m20) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(insertSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }

                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m20='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(updateSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m20) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(insertSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                //40cm土壤含水量
                //{"agreement":6,"dataType":"40厘米处土壤含水量","dataTypeCode":"13","dataUnit":"","dataValue":0.0,"factorTypeCode":6,"factorTypeName":"墒情","id":"9ef6993c85034e2ab90532ca3cbf64d8","telemetryStationCode":"0000003021","telemetryStationName":"苏子峪村","tm":"2022-05-09 16:00:00"}
                case "13":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m40='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(updateSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m40) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(insertSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }

                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set m40='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(updateSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,m40) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        if (list2.indexOf(map2) >= 0) {
                            System.out.println(insertSql);
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                    //墒情站点温度
                //f92、93、94分别是10、20、40土壤温度
                case "FF92":
                case "FF93":
                case "FF94":
                case "FF95":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from ST_RTU_STATUS where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update ST_RTU_STATUS set "+ dataTypeCode.replace("FF","F") +"='"+val+"' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into ST_RTU_STATUS (stcd,tm,"+ dataTypeCode.replace("FF","F") +") values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                    }

                    selectSql = "select count(1) C from ST_RTU_STATUS where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update ST_RTU_STATUS set "+ dataTypeCode.replace("FF","F") +"='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateTwo.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into ST_RTU_STATUS (stcd,tm,"+ dataTypeCode.replace("FF","F") +") values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateTwo.execute(insertSql);
                    }
                    break;
                    //电压
                case "38":
                    //太阳能充电电压
                case "76":
                    //rtu信号强度
                case "77":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from ST_RTU_STATUS where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update ST_RTU_STATUS set v"+dataTypeCode+"='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into ST_RTU_STATUS (stcd,tm,v"+dataTypeCode+") values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                    }

                    selectSql = "select count(1) C from ST_RTU_STATUS where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update ST_RTU_STATUS set v"+dataTypeCode+"='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateTwo.execute(updateSql);
                    } else {
                        //执行插入操作
                        insertSql = "insert into ST_RTU_STATUS (stcd,tm,v"+dataTypeCode+") values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateTwo.execute(insertSql);
                    }
                    break;
                //墒情站降水量
                case "20":
                    stcd = String.valueOf(Integer.parseInt(stcd));
                    map2 = new HashMap<>();
                    map2.put("stcd", stcd);
                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateOne.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set R='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.execute(updateSql);
                        if (list2.indexOf(map2) >= 0) {
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,R) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        jdbcTemplateOne.execute(insertSql);
                        if (list2.indexOf(map2) >= 0) {
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }

                    selectSql = "select count(1) C from st_Moisture_r where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                    map = jdbcTemplateTwo.queryForMap(selectSql);
                    if (Integer.parseInt(map.get("C").toString()) > 0) {
                        //执行更新操作
                        updateSql = "update st_Moisture_r set R='" + val + "' where stcd='" + stcd + "' and tm=to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss')";
                        if (list2.indexOf(map2) >= 0) {
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                updateSql = updateSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(updateSql);
                        }
                    } else {
                        //执行插入操作
                        insertSql = "insert into st_Moisture_r (stcd,tm,R) values ('" + stcd + "',to_date('" + tm + "','yyyy-MM-dd hh24:mi:ss'),'" + val + "')";
                        if (list2.indexOf(map2) >= 0) {
                            if (list3!=null&&list3.size()>0&&list3.get(0).get("stcd_new")!=null&&list3.get(0).get("stcd_new").equals(stcd)) {
                                insertSql = insertSql.replace(stcd, list3.get(0).get("stcd").toString());
                            }
                            jdbcTemplateTwo.execute(insertSql);
                        }
                    }
                    break;
                //接收到的消息为：{"agreement":6,"dataType":"降水量累计值","dataTypeCode":"26","dataUnit":"mm","dataValue":23.4,"departmentCode":"1207849431387193345","departmentName":"北京市水文总站","factorTypeCode":2,"factorTypeName":"雨量","id":"f81671a2103f473b8cc160c6898fbc02","rktm":"2023-03-21T11:00:56.213","telemetryStationCode":"0030826900","telemetryStationName":"霞云岭","tm":"2023-03-21T11:00:00"}
                case "26":
                    //雨量数据
                    Float Paravalue = Float.parseFloat(val);   //累计雨量
                    if(Paravalue == 0)
                    {
                        //代表雨量计的累计数据被重置，直接写入0，从新开始累加
                        return;
                    }
                    //判断是否为非汛期，非汛期屏蔽掉部分翻斗雨量计的非平安报
                    Calendar calendar = Calendar.getInstance();
                    selectSql = "select * from ST_FLOOD_SEASON where id ='1'";
                    Map fs = jdbcTemplateTwo.queryForMap(selectSql);
                    Date st =  DateUtils.strToDate(calendar.get(Calendar.YEAR) + "-" + fs.get("st").toString(),"yyyy-MM-dd");
                    Date et =  DateUtils.strToDate(calendar.get(Calendar.YEAR) + "-" + fs.get("et").toString(),"yyyy-MM-dd");
                    selectSql = "select case when raintype is null then 'FD' else raintype end raintype from st_stbprp_b where stcd = '"+stcd+"'";
                    List<Map<String, Object>>  rt =  jdbcTemplateTwo.queryForList(selectSql);
                    if(rt.size()==0)
                    {
                        return ;
                    }
                    String raintype = String.valueOf(rt.get(0).get("raintype"));
                    if((date.getTime() <= st.getTime() || date.getTime() >= et.getTime() ) && DateUtils.strToDate(tm).getMinutes()!=0 && raintype.equals("FD"))
                    {
                        //如果数据处于非汛期，并且是翻斗雨量计，则会屏蔽掉非平安报
                        //return;
                    }
                    //定义时间戳变量，判断是否为注水实验，注水试验直接指定时间段的非平安报写入时间-1，不推送给集成平台和水情课
                    selectSql = "select count(1) C from st_stbprp_b_zs where stcd='"+stcd+"' " +
                            "and to_date('"+tm+"','yyyy-MM-dd hh24:mi:ss')>starttime " +
                            "and to_date('"+tm+"','yyyy-MM-dd hh24:mi:ss')<endtime ";
                    List<Map<String, Object>>  c = jdbcTemplateTwo.queryForList(selectSql);
                    String times = "0";  //用来确定是否推送的时间戳
                    if(c.size()>0 && c.get(0).get("C").toString().equals("1"))
                    {
                        times = "-1";
                    }
                    //计算插补值
                    selectSql = "select case when PARAVALUE is null then "+val+" else PARAVALUE end PARAVALUE FROM ST_RAIN_RE WHERE (STCD,TM) IN (SELECT STCD,MAX(TM) FROM ST_RAIN_RE WHERE STCD='"+stcd+"' and TM<TO_DATE('"+tm+"','yyyy-MM-dd hh24:mi:ss') group by stcd)";
                    List<Map<String, Object>> MaxR = jdbcTemplateOne.queryForList(selectSql); //获取之前的累计雨量
                    Float oldR = Float.parseFloat((MaxR != null && MaxR.size()>0) ? MaxR.get(0).get("PARAVALUE").toString() : val);
                    Float R = Float.parseFloat(val);
                    Float newR = R - oldR;
                    //判断结果，整数直接执行插入操作
                    //负数查询比他大的最小数据进行插补，并且更新后续数据。
                    if(newR < 0)
                    {
                        //获取比插入值小的最大累计雨量数据
                        selectSql = "select stcd,tm,paravalue from st_rain_re where (stcd,tm) in (select '"+stcd+"',max(tm) from st_rain_re where stcd = '"+stcd+"' and paravalue<"+val+")";
                        Map MaxR2 =  jdbcTemplateOne.queryForMap(selectSql);
                        //更新之后的数据，全部减去插补的雨量
                        updateSql = "update st_rain_re set R=R+"+newR+",times=sysdate where stcd='"+stcd+"' and tm > to_date('"+MaxR2.get("tm")+"','yyyy-MM-dd hh24:mi:ss')";
                        jdbcTemplateOne.queryForMap(selectSql);
                        jdbcTemplateTwo.queryForMap(selectSql);
                    }
                    //插入插补的雨量数据
                    selectSql = "select count(1) C from st_rain_re where stcd='"+stcd+"' and tm=to_date('"+tm+"','yyyy-MM-dd hh24:mi:ss')";
                    c = jdbcTemplateOne.queryForList(selectSql);
                    if(c.size()>0 && c.get(0).get("C").toString().equals("1"))
                    {
                        //更新雨量数据
                        updateSql = "update st_rain_re set  R=(R-"+newR+")  where stcd='"+stcd+"' and tm = to_date('"+tm+"','yyyy-MM-dd hh24:mi:ss')";
                    }
                    else
                    {
                        insertSql = "insert into st_rain_re(stcd,tm,r,paravalue,times) values('"+stcd+"',to_date('"+tm+"','yyyy-MM-dd hh24:mi:ss'),'"+newR+"','"+val+"',(sysdate + "+times+"))";
                        jdbcTemplateOne.execute(insertSql);
                        jdbcTemplateTwo.execute(insertSql);
                    }
                    //保存在本地
                    //推送给44.8
                    //推送给金山云水文自动化
                    //推送给金山云集成平台
                    break;
            }


        }
        /*
        sql = "update sm_bw set et=sysdate where id='"+jsonObject.getString("id")+"'";
        jdbcTemplateOne.execute(sql);
        //为了查询效率，日志只保留14天的记录
//        sql = "delete from sm_bw where inserttime < sysdate -14";
//        jdbcTemplateOne.execute(sql);
         */
    }

    public void messageReceive2(MessageExt ext)
    {
        //获取当前时间
        Date date = new Date();
        //处理业务逻辑
        try {
            System.out.println("接收到的消息为：" + new String(ext.getBody(),"UTF-8"));
            //记录时间
            System.out.println("接收时间：" + DateUtils.convertStr(date,"yyyy-MM-dd HH:mm:ss"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //设置超时时间配置
        jdbcTemplateTwo.setQueryTimeout(120);
        jdbcTemplateOne.setQueryTimeout(120);

        //解析数据
        JSONObject jsonObject = JSONObject.parseObject(new String(ext.getBody()));
        //数据值
        String val = jsonObject.getString("dataValue");
        //采集时间
        String tm = jsonObject.getString("tm").replace("T", " ");
        //数据类型
        String dataTypeCode = jsonObject.getString("dataTypeCode");
        //预定义SQL语句
        //查询sql语句、插入sql语句、更新sql语句
        String selectSql, insertSql, updateSql;
        //定义最大水位、最小水位、最大流量、最小流量
        //预定义查询结果列表、测站编码列表
        List<Map<String, Object>> selectlist,stbprpList;
        //校验测站编码的map
        Map<String, Object> stcdMap = new HashMap<>();

        //测站编码
        String stcd = jsonObject.getString("telemetryStationCode");
        if(StringUtil.isNullOrEmpty(stcd) || stcd.length() <= 2){
            return;
        }
        //截取测站编码，去掉起始的00
        stcd = stcd.substring(2);
        //记录编码用于后续的数据校验
        stcdMap.put("stcd", stcd);
        if(!stcd.equals(null)&&!stcd.equals("")) {
            selectSql = "select stcd,stcd_new from st_stbprp_b where stcd='" + stcd + "' or stcd_new='" + stcd + "'";
            stbprpList = jdbcTemplateTwo.queryForList(selectSql);
            switch(dataTypeCode)
            {
                //水位
                case "39":
                    //判定水位范围
                    selectSql = "select (case when maxz is not null then maxz else -1) maxz,(case when minz is not null then minz else -1) minz from st_stbprp_b where stcd = '"+stcd+"' or stcd_new='"+stcd+"'";
                    selectlist =  jdbcTemplateTwo.queryForList(selectSql);
                    Float maxz =(selectlist!=null && selectlist.size()>0) ?  Float.parseFloat(selectlist.get(0).get("maxz").toString()): -1;
                    break;
                //流速
                case "37":
                    break;
                //流量
                case "27":
                    break;
                //10cm墒情
                case "10":
                    break;
                //20cm墒情
                case "11":
                    break;
                //30cm墒情
                case "13":
                    break;
                //墒情站雨量数据
                case "20":
                    break;
                //245雨量站数据
                case "26":
                    break;
            }
        }
    }
    private String getString(String stcd){
            switch (stcd) {
                case "30503162":
                    stcd = "30503162";
                    break;
                case "30503151":
                    stcd = "3050315S";
                    break;
                case "30503641":
                    stcd = "3050364S";
                    break;
                case "30504051":
                    stcd = "3050405S";
                    break;
                case "30503756":
                    stcd = "30503756";
                    break;
                case "30503621":
                    stcd = "3050362S";
                    break;
                case "30503768":
                    stcd = "30503768";
                    break;
                case "30503701":
                    stcd = "3050370S";
                    break;
                case "30503601":
                    stcd = "3050360S";
                    break;
                case "30503661":
                    stcd = "3050366S";
                    break;
                case "30503631":
                    stcd = "3050363S";
                    break;
                case "30503642":
                    stcd = "30503642";
                    break;
                case "30504075":
                    stcd = "30504075";
                    break;
                case "30503590":
                    stcd = "30503590";
                    break;
                case "30503955":
                    stcd = "30503955";
                    break;
            }
            return stcd;
    }

    /**
     * 创建文件
     *
     * @throws IOException
     */
    public boolean creatTxtFile(String name) throws IOException {
        boolean flag = false;
        filenameTemp = name;
        File filename = new File(filenameTemp);
        if (!filename.exists()) {
            filename.createNewFile();
            flag = true;
        }
        return flag;
    }
    private static String filenameTemp;
    /**
     * 传入文件名以及字符串, 将字符串信息保存到文件中
     *
     * @param strFilename
     * @param strBuffer
     */
    public void TextToFile(final String strFilename, final String strBuffer)
    {
        try
        {
            // 创建文件对象
            File fileText = new File(strFilename);
            // 向文件写入对象写入信息
            FileWriter fileWriter = new FileWriter(fileText);
            // 写文件
            fileWriter.write(strBuffer);
            // 关闭
            fileWriter.close();
        }
        catch (IOException e)
        {
            //异常
            e.printStackTrace();
        }
    }

    private String checkNewST(String stcd)
    {
        switch (stcd)
        {
            case "":
                stcd = "";
                break;
        }
        return  stcd;
    }
}
