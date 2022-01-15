//
// Created by root on 21-11-8.
//

#ifndef FFER_COMMANDUI_H
#define FFER_COMMANDUI_H

#endif //FFER_COMMANDUI_H


#include"transaction.h"
#include<iostream>


using namespace std;

/*

transactionExecutor tE;
transactionHandler tH;



//string json = "{\"rows\" : \"number,name,age\",\"tuples\" : [{\"age\" : 12,\"name\" : \"zxk\",\"number\" : 1234},{\"age\" :32,\"name\" : \"zxkk\",\"number\" :123131}]}";



tH.setDataByTransactionID (tE.remoteCreateTable ("10.77.70.70:10034","create table stu1 (number int,age int ,name char(50))"));
tH.Handle ();
cout << tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteInsert ("10.77.70.70:10034","insert into stu1 values(12,123,\"dddd\"),(123,43,\"vvvvv\")"));
tH.Handle ();
cout << tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteUpdate ("10.77.70.70:10034","update stu1 set age=12 where age=43"));
tH.Handle ();
cout << tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteSelectAll("10.77.70.70:10034","select * from stu1"));
tH.Handle ();
tH.showSelectHandleResultToFormat ();

tH.setDataByTransactionID (tE.remoteDelete ("10.77.70.70:10034","delete from stu1 where age=12"));
tH.Handle ();
cout << tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteSelectAll("10.77.70.70:10034","select * from stu1"));
tH.Handle ();
tH.showSelectHandleResultToFormat ();

tH.setDataByTransactionID (tE.remoteSelectAll("10.77.70.71:10034","select * from student"));
tH.Handle ();
tH.showSelectHandleResultToFormat ();

string insert = tH.getValue ();
string result;
tE.localCreate ("create table tmp_student(number int,name char(20),age int)",result);
cout << result<<endl;
tE.localInsertJson ("tmp_student",insert,result);
cout << result << endl;
tE.localSelect ("select * from tmp_student",result);
cout << result<<endl;
tE.localDrop ("drop table tmp_student",result);
cout << result << endl;


tH.setDataByTransactionID (tE.remoteCreateTempTableFromSelect ("10.77.70.71:10034","CREATE TABLE tmp_table(SELECT *  FROM student);"));
tH.Handle ();
tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteSelectAll("10.77.70.71:10034","select * from tmp_table"));
tH.Handle ();
tH.getHandleResult ();

tH.setDataByTransactionID (tE.remoteDropTable ("10.77.70.71:10034","drop table tmp_table"));
tH.Handle ();
tH.getHandleResult ();

*/


class commandUI
{

private:

    transactionExecutor tE;
    transactionHandler tH;


    void SplitString(const string& s, vector<string>& v, const string& c)
    {
        string::size_type pos1, pos2;
        pos2 = s.find(c);
        pos1 = 0;
        while(string::npos != pos2)
        {
            v.push_back(s.substr(pos1, pos2-pos1));

            pos1 = pos2 + c.size();
            pos2 = s.find(c, pos1);
        }
        if(pos1 != s.length())
            v.push_back(s.substr(pos1));
    }


    void SplitString2(const string& s, vector<string>& v)
    {
        int indexfront = 0,indexback = 0;
        for(indexfront = 0 ; indexfront < s.size () ; indexfront++)
        {
            if(s[indexfront]>='a' && s[indexfront] <= 'z'||s[indexfront] >= 'A' && s[indexfront] <= 'Z')
                break;
        }
        indexback = s.find (" ",indexfront);
        if (indexback == s.npos) {
            return;
        }

        v.push_back (s.substr(indexfront,indexback));

        while(1) {
            indexfront = indexback+1;
            indexfront = s.find ('\'', indexfront);

            indexback = s.find ('\'', indexfront+1);


            if(indexback >= s.size ()||indexfront >= s.size ())
                break;

            v.push_back (s.substr (indexfront+1, indexback-indexfront-1));
        }
    }


    void rCreate(vector<string> &com)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }


        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteCreateTable (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;

    }
    void rSelect(vector<string> &com)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteSelectAll(addr,sql));
        tH.Handle ();
        tH.showSelectHandleResultToFormat ();


    }
    void rSelect2rT(vector<string> &com)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }

        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteCreateTempTableFromSelect (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;
    }
    void rSelect2lT(vector<string> &com)
    {
        if(com.size () != 4) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        string tablename = com[3];
        if(addr.size ()<=0 || sql.size () <= 0 || tablename.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteSelectAll(addr,sql));
        tH.Handle ();
        string insert = tH.getValue ();

       // tH.showSelectHandleResultToFormat ();
        string result;
        tE.localInsertJson (tablename,insert,result);
        cout << "获得数据长度："<<result.size ()<<endl;
        //cout << result<<endl;

    }
    void rInsert(vector<string> &com)
    {

        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteInsert (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;


    }
    void rDelete(vector<string> &com)
    {

        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteDelete (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;

    }
    void rUpdate(vector<string> &com)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        tH.setDataByTransactionID (tE.remoteUpdate (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;

    }
    void rDrop(vector<string> &com)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }
        tH.setDataByTransactionID (tE.remoteDropTable (addr,sql));
        tH.Handle ();
        cout << tH.getHandleResult ()<<endl;
    }
    void lCreate(vector<string> &com)
    {

        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        string result;
        tE.localCreate (sql,result);
        cout << result << endl;
    }
    void lSelect(vector<string> &com)
    {

        cout << com[0] << com[1] << endl;
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }

        string result;
        tE.localSelect (sql,result);
        cout << result<<endl;
    }
    void lInsert(vector<string> &com)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string result;
        tE.localInsert (sql,result);
        cout << result << endl;

    }
    void lDelete(vector<string> &com)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string result;
        tE.localDelete (sql,result);
        cout << result << endl;
    }
    void lUpdate(vector<string> &com)
    {

        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string result;
        tE.localUpdate (sql,result);
        cout << result << endl;
    }
    void lDrop(vector<string> &com)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return;
        }
        string result;
        tE.localUpdate (sql,result);
        cout << result << endl;
    }


public:

    int resolveCommand(string command)
    {
        vector<string> com;
        SplitString2(command,com);


        if(com.size () == 0) {
            cout << "Invalid Command!"<<endl;
            return -1;
        }


        if(com[0].compare ("rcreate") == 0)
            rCreate(com);
        else if(com[0].compare ("rselect") == 0)
            rSelect(com);
        else if(com[0].compare ("rselect2rT") == 0)
            rSelect2rT(com);
        else if(com[0].compare ("rselect2lT") == 0)
            rSelect2lT(com);
        else if(com[0].compare ("rinsert") == 0)
            rInsert(com);
        else if(com[0].compare ("rdelete") == 0)
            rDelete(com);
        else if(com[0].compare ("rupdate") == 0)
            rUpdate(com);
        else if(com[0].compare ("rdrop") == 0)
            rDrop(com);
        else if(com[0].compare ("lcreate") == 0)
            lCreate(com);
        else if(com[0].compare ("lselect") == 0)
            lSelect(com);
        else if(com[0].compare ("linsert") == 0)
            lInsert(com);
        else if(com[0].compare ("ldelete") == 0)
            lDelete(com);
        else if(com[0].compare ("lupdate") == 0)
            lUpdate(com);
        else if(com[0].compare ("ldrop") == 0)
            lDrop(com);
        else if(com[0].compare ("start") == 0)
            ;
        else if(com[0].compare ("close") == 0)
            ;
        else
            cout << "Invaild Command!"<<endl;

    }

};





class commandUIInternal
{

private:

    transactionExecutor tE;
    transactionHandler tH;


    void SplitString(const string& s, vector<string>& v, const string& c)
    {
        string::size_type pos1, pos2;
        pos2 = s.find(c);
        pos1 = 0;
        while(string::npos != pos2)
        {
            v.push_back(s.substr(pos1, pos2-pos1));

            pos1 = pos2 + c.size();
            pos2 = s.find(c, pos1);
        }
        if(pos1 != s.length())
            v.push_back(s.substr(pos1));
    }


    void SplitString2(const string& s, vector<string>& v)
    {
        int indexfront = 0,indexback = 0;
        for(indexfront = 0 ; indexfront < s.size () ; indexfront++)
        {
            if(s[indexfront]>='a' && s[indexfront] <= 'z'||s[indexfront] >= 'A' && s[indexfront] <= 'Z')
                break;
        }
        indexback = s.find (" ",indexfront);
        if (indexback == s.npos) {
            return;
        }

        v.push_back (s.substr(indexfront,indexback));

        while(1) {
            indexfront = indexback+1;
            indexfront = s.find ('\'', indexfront);

            indexback = s.find ('\'', indexfront+1);


            if(indexback >= s.size ()||indexfront >= s.size ())
                break;

            v.push_back (s.substr (indexfront+1, indexback-indexfront-1));
        }
    }


    int rCreate(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }


        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteCreateTable (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());

    }
    int rSelect(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteSelectAll(addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());


    }
    int rSelect2rT(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteCreateTempTableFromSelect (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());
    }
    int rSelect2lT(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 4) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        string tablename = com[3];
        if(addr.size ()<=0 || sql.size () <= 0 || tablename.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteSelectAll(addr,sql));
        tH.Handle ();
        string insert = tH.getValue ();

        tH.showSelectHandleResultToFormat ();
        string result;
        tE.localInsertJson (tablename,insert,result);
        sqlResult = result;
        return stoi(tH.getHandleStatus ());

    }
    int rInsert(vector<string> &com,string &sqlResult)
    {

        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteInsert (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());


    }
    int rDelete(vector<string> &com,string &sqlResult)
    {

        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteDelete (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());

    }
    int rUpdate(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        tH.setDataByTransactionID (tE.remoteUpdate (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());

    }
    int rDrop(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 3) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string addr = com[1];
        string sql = com[2];
        if(addr.size ()<=0 || sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        tH.setDataByTransactionID (tE.remoteDropTable (addr,sql));
        tH.Handle ();
        sqlResult = tH.getHandleResult ();
        return stoi(tH.getHandleStatus ());
    }
    int lCreate(vector<string> &com,string &sqlResult)
    {

        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        string result;
        int ret = tE.localCreate (sql,result);
        sqlResult = result;

        return ret;

    }
    int lSelect(vector<string> &com,string &sqlResult)
    {

        cout << com[0] << com[1] << endl;
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }

        string result;
        int ret = tE.localSelect (sql,result);
        sqlResult = result;

        return ret;

    }
    int lInsert(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string result;
        int ret = tE.localInsert (sql,result);
        sqlResult = result;

        return ret;

    }
    int lDelete(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string result;
        int ret = tE.localDelete (sql,result);
        sqlResult = result;

        return ret;
    }
    int lUpdate(vector<string> &com,string &sqlResult)
    {

        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string result;
        int ret = tE.localUpdate (sql,result);
        sqlResult = result;

        return ret;
    }
    int lDrop(vector<string> &com,string &sqlResult)
    {
        if(com.size () != 2) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string sql = com[1];
        if(sql.size () <= 0) {
            cout << "Invalid Command!" << endl;
            return -1;
        }
        string result;
        int ret = tE.localUpdate (sql,result);
        sqlResult = result;

        return ret;
    }


public:

    int resolveCommand(string command,string &sqlResult)
    {
        vector<string> com;
        SplitString2(command,com);


        if(com.size () == 0) {
            cout << "Invalid Command!"<<endl;
            return -1;
        }


        if(com[0].compare ("rcreate") == 0)
            rCreate(com,sqlResult);
        else if(com[0].compare ("rselect") == 0)
            rSelect(com,sqlResult);
        else if(com[0].compare ("rselect2rT") == 0)
            rSelect2rT(com,sqlResult);
        else if(com[0].compare ("rselect2lT") == 0)
            rSelect2lT(com,sqlResult);
        else if(com[0].compare ("rinsert") == 0)
            rInsert(com,sqlResult);
        else if(com[0].compare ("rdelete") == 0)
            rDelete(com,sqlResult);
        else if(com[0].compare ("rupdate") == 0)
            rUpdate(com,sqlResult);
        else if(com[0].compare ("rdrop") == 0)
            rDrop(com,sqlResult);
        else if(com[0].compare ("lcreate") == 0)
            lCreate(com,sqlResult);
        else if(com[0].compare ("lselect") == 0)
            lSelect(com,sqlResult);
        else if(com[0].compare ("linsert") == 0)
            lInsert(com,sqlResult);
        else if(com[0].compare ("ldelete") == 0)
            lDelete(com,sqlResult);
        else if(com[0].compare ("lupdate") == 0)
            lUpdate(com,sqlResult);
        else if(com[0].compare ("ldrop") == 0)
            lDrop(com,sqlResult);
        else if(com[0].compare ("start") == 0)
            ;
        else if(com[0].compare ("close") == 0)
            ;
        else
            cout << "Invaild Command!"<<endl;

    }

};


