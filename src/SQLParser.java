/**************************
Created on Jun 27, 2020
@author: Sabyasachi Nayak
**************************/

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SQLParser {
	
	public static ArrayList<String> columnsList = new ArrayList<String>();
	public static ArrayList<String> joinColumnsList = new ArrayList<String>();
	
	public static String[] getQueries(){
		String[] raw_queries_list = {"select * from users where id >1",
	    		"select id, name from users where user_id=1 group by age",
	    		"select address, name, first_name, user_id from users where user_name='Zeashan'",
	    		"select *, ssn from users where id=123456789",
	    		"select id, name from users where user_id=1 and age > 20 and b< 5 and a = 2 and e =9",
	    		"select id, person_address.city from person right join person_address on person.b = person_address.d",
	    		"select pii_field1 from table1 left join table2 on table1.pii_field1 = table2.id where table1.pii_field3 = 123",
	    		"select name from movies group by category_id,year_released having category_id=8",
	    		"select E.CaseNum, E.FileNum, E.ActivityNum,E.Grade,V.score from Evalulation E join MarketValue V ON E.CaseNum=V.CaseNum ",
	    		"select sum(promo_sales) from (select promo_desc, sum(dollars) as promo_sales from promotion group by dollars) as promos where promo_desc not like 'No%'"};
	
		return raw_queries_list;
	}

	public static ArrayList<String> getProjectedColumns(SqlSelect selectNode)
	{
		ArrayList<String> projectedColumnsList = new ArrayList<String>();
		SqlNodeList selectList = selectNode.getSelectList();
		
    	for (int i = 0; i < selectList.size(); i++) {
    		 String column = selectList.get(i).toString();
    		 projectedColumnsList.add(column);
    	}
    	 
		return projectedColumnsList;
	}
	
	public static ArrayList<String> getPredicateColumns(SqlNode[] sqlNodeList)
	{
		 boolean isLast = true;
		 
    	 if (sqlNodeList[0].getKind().sql.equalsIgnoreCase("IDENTIFIER"))
    	 { 
    		 columnsList.add(sqlNodeList[0].toString());
    	 }
    	 else if (sqlNodeList[0].getKind().sql.equalsIgnoreCase("AND") || sqlNodeList[0].getKind().sql.equalsIgnoreCase("OR") 
    			 || ((SqlBasicCall)sqlNodeList[0]).getOperator().getName().contains("=") 
    			 || ((SqlBasicCall)sqlNodeList[0]).getOperator().getName().contains(">")
    		 	 || ((SqlBasicCall)sqlNodeList[0]).getOperator().getName().contains("<"))
    	 {
    		for(int i = sqlNodeList.length -1; i >= 0; i--) {
    			SqlNode[] whereSqlNode = ((SqlBasicCall)sqlNodeList[i]).getOperands();
    		    if (isLast)
    		    {
    		    	columnsList.add(whereSqlNode[0].toString());
	                isLast = false;
    		    }
    		    else
                {	
    		    	getPredicateColumns(((SqlBasicCall)sqlNodeList[i]).getOperands());
                }
            }
    	  }

		return columnsList;
	}
	
	public static ArrayList<String> getJoinPredicateColumns(SqlNode[] sqlNodeList)
	{
		 boolean isLast = true;
		 
	   	 if (sqlNodeList[0].getKind().sql.equalsIgnoreCase("IDENTIFIER") &&
	   			sqlNodeList[1].getKind().sql.equalsIgnoreCase("IDENTIFIER"))
	   	 { 
	   		joinColumnsList.add(sqlNodeList[0].toString());
	   		joinColumnsList.add(sqlNodeList[1].toString());
	   	 }
	   	 else if (sqlNodeList[0].getKind().sql.contains("=") || sqlNodeList[0].getKind().sql.equalsIgnoreCase("AND"))
	   	 {
	   		for(int i = sqlNodeList.length -1; i >= 0; i--) 
	   		{
	   			SqlNode[] joinSqlNode = ((SqlBasicCall)sqlNodeList[i]).getOperands();
	   		    if (isLast)
	   		    {
	   		    	joinColumnsList.add(joinSqlNode[0].toString());
	   		    	joinColumnsList.add(joinSqlNode[1].toString());
		            isLast = false;
	   		    }
	   		    else
	            {	
	   		    	getJoinPredicateColumns(((SqlBasicCall)sqlNodeList[i]).getOperands());
	            }
	         }
	   	  }

		return joinColumnsList;
	}
	
	public static Map<String, Map<String, ArrayList<String>>> processQuery(String query)
	{
		 Map<String, Map<String, ArrayList<String>>> taggedColumnsMap = new HashMap<>();
		
		 ArrayList<String> projectedColsList = new ArrayList<>();
    	 ArrayList<String> predicateColsList = new ArrayList<>();
    	 ArrayList<String> joinPredicateColsList = new ArrayList<>();
    	 Map<String, ArrayList<String>> columnsMap = new HashMap<>();
    	 
		 try {
		    	 //Parse SQL query
		    	 SqlParser parser = SqlParser.create(query);
		     	 SqlSelect selectNode = (SqlSelect) parser.parseQuery();
		     	 
		     	 //Get Projected columns
		     	 projectedColsList = SQLParser.getProjectedColumns(selectNode);
		     	 columnsMap.put("projection", projectedColsList);
		     	
		     	 //Get Predicate columns
		     	 if (selectNode.hasWhere())
		     	 {
			     	 SqlNode where = selectNode.getWhere();
			     	 SqlNode[] sqlNodeList = ((SqlBasicCall)where).getOperands();
			     	 predicateColsList = SQLParser.getPredicateColumns(sqlNodeList);
			     	 columnsMap.put("predicate", predicateColsList);
		     	 }
		     	 
		     	 //Get Join columns
		     	 SqlNode from = selectNode.getFrom();
		     	 if(from.getKind().sql.contains("JOIN"))
		     	 {
		     		 SqlJoin sj = (SqlJoin)from;
			     	 SqlNode[] sqlJoinNodeList = ((SqlBasicCall)sj.getCondition()).getOperands();
			     	 joinPredicateColsList = SQLParser.getJoinPredicateColumns(sqlJoinNodeList);
			     	 columnsMap.put("join_predicate", joinPredicateColsList);
			     	 
		     	 }
		     	 
		     	taggedColumnsMap.put(query, columnsMap);
		    } 
		    catch (SqlParseException e) {
		        System.out.println("Invalid query.SQL Parser failed to parse.Please check it.");
		    }
		 return taggedColumnsMap;
	}
    
	public static Map<String, ArrayList<String>> getTaggedColumns()
	{
		 Map<String, ArrayList<String>> taggedColumnsMap = new HashMap<>();
		 taggedColumnsMap.put("projection", new ArrayList<>(Arrays.asList("ssn","salary")));
		 taggedColumnsMap.put("predicate", new ArrayList<>(Arrays.asList("salary")));
		 taggedColumnsMap.put("join_predicate", new ArrayList<>(Arrays.asList("ssn")));
		 return taggedColumnsMap;
	}
	
	public static boolean queryCheck( Map<String, Map<String, ArrayList<String>>> queryColumnsListMap)
	{
		boolean queryStatus = false;
		Map<String, ArrayList<String>> taggedColumnsMap = new HashMap<>();
		
		//Get tagged columns
	    taggedColumnsMap = SQLParser.getTaggedColumns();
	    //taggedColumnsMap.forEach((key, value) -> System.out.println(key + " : " + value));
	    
	    Iterator colListIterator = queryColumnsListMap.entrySet().iterator();
	    while(colListIterator.hasNext())
	    {
	    	Map.Entry mapElement = (Map.Entry)colListIterator.next();
	    	Map<String, ArrayList<String>> queryColsMap = (Map<String, ArrayList<String>>) mapElement.getValue();
	    	
	    	for(String queryKey: queryColsMap.keySet())
	    	{
	    		for(String taggedKey:taggedColumnsMap.keySet())
	    		{
	    			if(queryKey.equalsIgnoreCase(taggedKey))
	    			{
	    				for(String col: queryColsMap.get(queryKey))
	    				{
	    					Iterator<String> taggedColumnsIterator = taggedColumnsMap.get(queryKey).iterator();
	    					while(taggedColumnsIterator.hasNext())
	    					{
		    					if(taggedColumnsIterator.next().equalsIgnoreCase(col))
		    					{
		    						queryStatus = true;
		    					}
	    					}
	    				}
	    			}
	    		}
	    	}
	    }
	    
		return queryStatus;
	}
    public static void main(String[] args) 
    {
	    String[] rawQueries = SQLParser.getQueries();
	    for(String query : rawQueries)
	    {
	    	Map<String, Map<String, ArrayList<String>>> queryColumnsListMap = new HashMap<>();
	    	queryColumnsListMap = SQLParser.processQuery(query);
	 	    queryColumnsListMap.forEach((key, value) -> System.out.println(key + " : " + value));
	 	    
	 	    boolean inappropriate = false;
	 	    inappropriate = SQLParser.queryCheck(queryColumnsListMap);
	 	    System.out.println(inappropriate);
	 	    
	 	    SQLParser.joinColumnsList.clear();
	     	SQLParser.columnsList.clear();
	    }
     }
}