//
// Created by root on 21-10-31.
//

#ifndef FFER_ETCDHELPER_H
#define FFER_ETCDHELPER_H
#include "json/json.h"
#include<stdio.h>
#include<stdlib.h>
#include <iostream>

using namespace std;




class ETCDHelper
{
private:


public:


    string pipeopen(string cmd)
    {
        FILE *fp = NULL;
        char buff[102400] = {0};
        fp = popen(cmd.c_str (),"r");
        if(fp == NULL){
            return NULL;
        }
        fread(buff, 102400, 1, fp);
        //printf("%s\n",buff);

        string re = buff;
        pclose(fp);
        return re;
    }

    string base64Encode(string str)
    {
        if(str.length () != 0) {
            str.erase(std::remove(str.begin(), str.end(), '\n'),str.end());
            string cmd = "echo " + str + "|base64";
            //cout << cmd << endl;
            string re = pipeopen (cmd);
            return re;
        }
        else
            return "";
    }
    string base64Decode(string str)
    {
        if(str.length () != 0) {
            str.erase(std::remove(str.begin(), str.end(), '\n'),str.end());
            string cmd = "echo "+str+"|base64 -d";
            //cout << cmd << endl;
            string re = pipeopen (cmd);
            return re;
        }
        else
            return "";
    }
    int curltest(string key,string value)
    {


        string newKey = base64Encode (key);
        string newValue = base64Encode (value);


        newKey.erase(std::remove(newKey.begin(), newKey.end(), '\n'),newKey.end());
        newValue.erase(std::remove(newValue.begin(), newValue.end(), '\n'),newValue.end());

        //cout <<"$$$" << newKey <<" "<<newValue << "$$$"<<endl;

        string cmd1 = "curl -s -L http://10.77.70.70:2379/v3/kv/put -X POST -d '{\"key\":\""+newKey+"\", \"value\": \""+newValue+"\"}'";

        this->pipeopen (cmd1.c_str ());


        string reValue;

        string cmd2 = "curl -s -L http://10.77.70.70:2379/v3/kv/range -X POST -d '{\"key\": \""+newKey+"\"}'";
        string result1 = this->pipeopen (cmd2);


       // cout << "%%%%" << cmd1 <<"@@@@@@" << cmd2 << "^^^^^"<<endl;

       // cout << result1;
        Json::Reader reader;
        Json::Value root;
        reader.parse (result1,root);
        Json::Value kvs = root["kvs"];
        reValue =  kvs[0u]["key"].toStyledString ();
       // cout << reValue << "%%%";

        cout << base64Decode (reValue) << endl;

        return 0;
    }

    string getKey(string key)
    {

        key = "\""+key+"\"";


        string newKey = base64Encode (key);
        newKey.erase(std::remove(newKey.begin(), newKey.end(), '\n'),newKey.end());
        string reValue;




        string cmd2 = "curl -s -L http://10.77.70.71:2379/v3/kv/range -X POST -d '{\"key\": \""+newKey+"\"}'";
        string result1 = this->pipeopen (cmd2);


        //cout << result1<< endl;
        Json::Reader reader;
        Json::Value root;
        Json::Value kvs;
        reader.parse (result1,root);

        kvs = root["kvs"];

        if(kvs.toStyledString().compare("null") >= 0)
            return "null";


        reValue =  kvs[0u]["value"].toStyledString ();
        reValue = base64Decode (reValue);

        reValue.erase(std::remove(reValue.begin(),reValue.end(), '\n'),reValue.end());

        return reValue;
    }


    int setKey(string key,string value)
    {


        key = "\""+key+"\"";
        value = "\""+value+"\"";

        string newKey = base64Encode (key);
        string newValue = base64Encode (value);


        newKey.erase(std::remove(newKey.begin(), newKey.end(), '\n'),newKey.end());
        newValue.erase(std::remove(newValue.begin(), newValue.end(), '\n'),newValue.end());
        //cout <<"$$$" << newKey <<" "<<newValue << "$$$"<<endl;
        string cmd1 = "curl -s -L http://10.77.70.70:2379/v3/kv/put -X POST -d '{\"key\":\""+newKey+"\", \"value\": \""+newValue+"\"}'";
        this->pipeopen (cmd1.c_str ());

        return 0;
    }

    int deleteKey(string key)
    {


        key = "\""+key+"\"";


        string newKey = base64Encode (key);



        newKey.erase(std::remove(newKey.begin(), newKey.end(), '\n'),newKey.end());

        //cout <<"$$$" << newKey <<" "<<newValue << "$$$"<<endl;
        string cmd1 = "curl -s -L http://10.77.70.70:2379/v3/kv/deleterange -X POST -d '{\"key\":\""+newKey+"\"}'";

        this->pipeopen (cmd1.c_str ());

        return 0;
    }

    int pipeopen()
    {
        FILE *fp = NULL;
        char buf[10240] = {0};
        fp = popen("curl -s -L http://10.77.70.70:2379/v3/kv/range   -X POST -d '{\"key\": \"Zm9v\"}'","r");
        if(fp == NULL){
            return 0;
        }
        fread(buf, 10240, 1, fp);
        printf("%s\n",buf);

        Json::Reader reader;
        Json::Value root;
        reader.parse (buf,root);
        Json::Value kvs = root["kvs"];
        cout << kvs[0u]["key"].toStyledString ();
        pclose(fp);
        return 0;

    }

};




#endif //FFER_ETCDHELPER_H
