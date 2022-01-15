//
// Created by root on 21-11-1.
//

#include<mysql/mysql.h>

#ifndef FFER_MYSQLHELPER_H
#define FFER_MYSQLHELPER_H

#include <iostream>
#include <string>
#include <vector>
#include "json/json.h"

using namespace std;



#endif //FFER_MYSQLHELPER_H




class MYSQLHelper
{
    MYSQL *mysql;
    MYSQL_ROW row;
    MYSQL_FIELD* field = NULL;
    MYSQL_RES* result;
public:
    int mysql_connect(const char *host,const char *user, const char *passwd, const char *db)
    {
        mysql = mysql_init(NULL);
        if (!mysql_real_connect(mysql,host,user,passwd,db,0,NULL,0))
        {
            printf("Can not connect db! \n");
            mysql = NULL;
            return -1;
        } else{
            printf("connect success! \n");
            return 0;
        }

    }

    string cleanStr(string str)
    {
        string s1 = str;
        string s2 = "\\n";
        while (s1.find(s2) < s1.length()) {
            s1.erase(s1.find(s2), s2.size());
        }
        str = s1;
        str.erase(std::remove(str.begin(), str.end(), '\\'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '"'), str.end());

        return str;
    }
    string cleanStrForInsert(string str)
    {
        string s1 = str;
        string s2 = "\\n";
        while (s1.find(s2) < s1.length()) {
            s1.erase(s1.find(s2), s2.size());
        }
        str = s1;
        str.erase(std::remove(str.begin(), str.end(), '\\'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());


        return str;
    }


    void mysql_select(string sql)
    {
        if(mysql != NULL) {
            mysql_query (mysql, sql.c_str ());
            MYSQL_RES *result = mysql_store_result(mysql);
            if(result == NULL)
                cout << "fail\n";

            for(int i=0; i < mysql_num_fields(result); i++)
            {
                   field = mysql_fetch_field_direct(result,i);
                   cout << field->name << "\t\t";
            }
            cout << endl;

            row = mysql_fetch_row(result);
            while(row != NULL)
            {
                   for(int i=0; i<mysql_num_fields(result); i++)
                   {
                         cout << row[i] << "\t\t";
                   }

                cout << endl;

                row = mysql_fetch_row(result);
            }
            mysql_free_result (result);
        }
        else
        {
            cout << "mysql未建立连接！"<<endl;
        }
    }

    int mysql_select_return_json(string sql,string &resultstr)
    {
        if(mysql != NULL) {


            Json::Value jsonSelectData;
            Json::Value jsondataLines;

            //cout << "["<<sql<<"]";

            if(mysql_query (mysql,sql.c_str ())) {

                resultstr = "SQL ERROR:" + string (mysql_error (mysql));
                return -1;
            }
            MYSQL_RES *result = mysql_store_result(mysql);
            if(result == NULL) {
                cout << "fail\n";
                resultstr="No Result!";
                return -1;
            }

            string rows;
            for(int i = 0; i < mysql_num_fields(result); i++)
            {
                field = mysql_fetch_field_direct(result,i);
                rows+=string(field->name)+",";
            //   cout << field->name << "\t\t";
            }
            rows = rows.substr (0,rows.length ()-1);
            jsonSelectData["rows"] = rows;


            row = mysql_fetch_row(result);

            map <string,int> mulname;
            while(row != NULL)
            {
                Json::Value data;
                string fieldname;
                for(int i=0; i<mysql_num_fields(result); i++)
                {
                    //cout << row[i] << "\t\t";
                    field = mysql_fetch_field_direct(result,i);
                    fieldname = field->name;
                    if (mulname.find(fieldname) == mulname.end())
                    {
                        mulname[fieldname] = 1;
                    }
                    if(data[fieldname].isString()){
                        fieldname+=to_string(mulname[fieldname]);
                        mulname[fieldname]++;
                    }
                    data[fieldname] = row[i];

                }
                jsondataLines.append (data);

                //cout << endl;

                row = mysql_fetch_row(result);
            }
            jsonSelectData["tuples"] = jsondataLines;



            resultstr = jsonSelectData.toStyledString ();

            mysql_free_result (result);
            return 0;
        }
        else
        {
            cout << "mysql未建立连接！"<<endl;

            Json::Value jsonSelectData;
            jsonSelectData["rows"] = "NULL";
            jsonSelectData["tuples"] = "NULL";
            resultstr = jsonSelectData.toStyledString ();
            return -1;
        }
    }
    int mysql_insert(string table,vector<string> rows,vector<string> values)
    {
        if(rows.size () == 0 || values.size () == 0)
            return -1;

        string row_str;
        int i;
        for(i = 0; i < rows.size () - 1 ; i++)
        {
            row_str+=(rows[i]+",");
        }
        row_str+=rows[i];

        string value_str;
        for(i = 0 ; i < values.size () - 1; i++)
        {
            value_str+="'"+values[i]+"',";
        }
        value_str+="'"+values[i]+"'";
        string sql = "INSERT INTO "+table+" ("+row_str+") values("+value_str+");";

        cout << sql << endl;

        mysql_query (mysql,sql.c_str ());
        return 0;

    }


    int mysql_insert_by_jsonstr(string table,string json,string &result)
    {
        Json::Value jsonStr;
        Json::Reader reader;
        reader.parse (json,jsonStr);

        Json::Value rows = jsonStr["rows"];
        Json::Value tuples = jsonStr["tuples"];



        string str = cleanStr (rows.toStyledString ());



        for(int i = 0 ; i < tuples.size () ; i++) {

            string rowStrjson;
            string value_str;
            Json::Value::Members mem = tuples[i].getMemberNames ();

            for (auto iter = mem.begin (); iter != mem.end (); iter++) {
                std::string strKey = *iter;
                rowStrjson+=(strKey+",");
                value_str+=tuples[i][*iter].toStyledString ()+",";
            }
            rowStrjson = rowStrjson.substr (0,rowStrjson.size () - 1);
            value_str =  value_str.substr (0,value_str.length () - 1);

            value_str = cleanStrForInsert (value_str);

            string sql = "INSERT INTO "+table+" ("+rowStrjson+") values("+value_str+");";


           // cout << sql << endl;

            if(mysql_exec (sql,result))
            {

                return -1;
            }
        }
        //result = "yes";
        return 0;
        //  string sql = "INSERT INTO "+table+" ("+rows.toStyledString ()+") values("+value_str+");";
    }
    int mysql_insert_by_jsonstrBlock(string table,string json,string &result)
    {
        Json::Value jsonStr;
        Json::Reader reader;
        cout << "解析中"<<endl;
        reader.parse (json,jsonStr);
        cout << "解析完毕"<<endl;
        Json::Value rows = jsonStr["rows"];
        Json::Value tuples = jsonStr["tuples"];



        string str = cleanStr (rows.toStyledString ());

        if(tuples.size () == 0)
            return 0;

        string sql = "INSERT INTO "+table;
        cout <<tuples.size ()<<endl;
        cout<<"拼接中"<<endl;

        for(int i = 0 ; i < tuples.size () ; i++) {

            string rowStrjson;
            string value_str;
            Json::Value::Members mem = tuples[i].getMemberNames ();

            for (auto iter = mem.begin (); iter != mem.end (); iter++) {
                std::string strKey = *iter;
                rowStrjson+=(strKey+",");
                value_str+=tuples[i][*iter].toStyledString ()+",";
            }
            rowStrjson = rowStrjson.substr (0,rowStrjson.size () - 1);
            value_str =  value_str.substr (0,value_str.length () - 1);

            value_str = cleanStrForInsert (value_str);

            if(rowStrjson.compare ("count(*)") == 0)
                rowStrjson = "count";
            string addd = " ("+rowStrjson+") values";

            if(i == 0)
                sql+=addd;

        //    cout << sql<<endl;

           sql+= "("+value_str+"),";


            // cout << sql << endl;


        }

        cout<<"拼接完毕"<<endl;
        sql = sql.substr (0,sql.size ()-1);

     //  cout << sql << endl;
        if(mysql_exec (sql,result))
        {

            return -1;
        }
        //result = "yes";
        return 0;
        //  string sql = "INSERT INTO "+table+" ("+rows.toStyledString ()+") values("+value_str+");";
    }


    int mysql_exec(string sql,string &result)
    {
       // cout << sql << "#$@#$" << endl;
        if(mysql_query (mysql,sql.c_str ())) {


            result = "SQL ERROR:" + string (mysql_error (mysql));
            return -1;
        }
        else {
            result = "SQL SUCCESS!";
            return 0;
        }

    }
    void mysql_closer()
    {

        mysql_commit (mysql);
        mysql_close (mysql);
    }

};