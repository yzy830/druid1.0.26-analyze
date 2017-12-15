package com.alibaba.druid.yang;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

public class Visitor {
    public static void main(String[] args) {
//    	String sql = "select o.order_id as orderId, max(o.order_sum) as orderSum, upper(uv.user_name) userName from t_d_order o "
//                + "join (select u.user_id, u.user_name,u.gender from t_d_user u join t_d_user_detail ud on u.user_id = ud.user_id) uv on o.user_id = uv.user_id "
//                + "where o.order_sum > 100 and order_cat = 1 and (uv.user_name = 'a' or uv.user_name = 'b') group by o.user_id having max(o.order_sum) > 200 order by uv.gender desc limit 0,10";
//        
        // 验证，只有在单表的情况下，字段才能不带别名。否则，无法解析每个列所属的表
        String sql = "select order_id from t_d_order where user_id = 2 and user_id = 1";
        
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        
        SQLStatement statement = parser.parseStatement();
        
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);
        
        System.out.println("columns = " + visitor.getColumns());
        System.out.println("conditions = " + visitor.getConditions());
        System.out.println("aggregates = " + visitor.getAggregateFunctions());
        System.out.println("funtions = " + visitor.getFunctions());
        System.out.println("groupByColumns = " + visitor.getGroupByColumns());
        System.out.println("orderByColumns = " + visitor.getOrderByColumns());
        System.out.println("parameters = " + visitor.getParameters());
        System.out.println("relations = " + visitor.getRelationships());
        System.out.println("tables = " + visitor.getTables());
        System.out.println("currentTable = " + visitor.getCurrentTable());
        System.out.println("aliasMap = " + visitor.getAliasMap());
        System.out.println("variants = " + visitor.getVariants());
    	
//        String sql = "select e.eventId, e.eventKey, e.eventName, e.flag from "
//                + "event e join user u where eventId = ? and eventKey = ? and (eventName = ? or eventName = ?) "
//                + "and u.user_id = 1 and e.event_time = ? group by u.user_id order by eventName";
//        MySqlStatementParser parser = new MySqlStatementParser(sql); 
//        SQLStatement statement = parser.parseStatement(); 
//        MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor(); 
//        
//        statement.accept(visitor);
    }
}
