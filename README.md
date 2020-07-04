# SQL-Parser

This SQL Parser parses sql queries using Apache Calcite and extracts column names(projected columns,predicate columns and join predicate columns) and checks if a query is appropriate or not based on tagged columns.

Ex:-
Tagged Columns:
predicate : [salary]
projection : [ssn, salary]
join_predicate : [ssn]

query1= "select address, name, first_name, user_id from users where user_name='Zeashan'"
Output:
{predicate columns=[USER_NAME], projection columns =[ADDRESS, NAME, FIRST_NAME, USER_ID]}  isAppropriate:false

query2 = "select address, name, first_name, user_id from users where user_name='Zeashan' and salary=5000"
Output:
{predicate columns=[USER_NAME], projection columns =[ADDRESS, NAME, FIRST_NAME, USER_ID]}  isAppropriate:true

Initially these tagged columns are hard-coded.But the plan is to get those columns from Apache Ranger.This application can be extended to get the sql queries from Spark streaming and parse it.
