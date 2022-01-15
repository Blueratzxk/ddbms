//
// Created by root on 21-11-6.
//

#ifndef FFER_TRANSACTION_H
#define FFER_TRANSACTION_H

#endif //FFER_TRANSACTION_H


#include "json/json.h"
#include "mysql/mysql.h"
#include "ETCDHelper.h"
#include "MYSQLHelper.h"
#include"dataTransmission.h"






class dataPacker
{
public:
    dataPacker(){}

    string pack(string type,string producer,string value,string status,string info) {
        Json::Value jsondata;
        jsondata["type"] = type;
        jsondata["producer"] = producer;
        jsondata["value"] = value;
        Json::Value response;
        response["status"] = status;
        response["info"] = info;
        jsondata["response"] = response;

        return  jsondata.toStyledString ();
    }
};


class receivedDataResolver
{
    string receivedData;
    Json::Value datas;
    Json::Reader reader;
    string sourceAddr;
    string type;
    string producer;

    string transactionID;

    string status;
    string info;


    Json::Value value;

    Json::Value value2;
public:

    receivedDataResolver()
    {

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

    string cleanStrForSelect(string str)
    {
        string s1 = str;
        string s2 = "\\n";
        while (s1.find(s2) < s1.length()) {
            s1.erase(s1.find(s2), s2.size());
        }
        str = s1;

        str.erase(std::remove(str.begin(), str.end(), '\\'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
        str = str.substr (0,str.length () - 2);
        str = str.substr (2,str.length ());
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


        str = str.substr (0,str.length () - 2);
        str = str.substr (2,str.length ());
        return str;
    }

    string cleanStrReservMaoHao(string str)
    {
        string s1 = str;
        string s2 = "\\n";
        while (s1.find(s2) < s1.length()) {
            s1.erase(s1.find(s2), s2.size());
        }
        str = s1;
        str.erase(std::remove(str.begin(), str.end(), '\\'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());


        str = str.substr (0,str.length () - 2);
        str = str.substr (2,str.length ());
        return str;
    }

    int resolve(string rdata)
    {
        receivedData = rdata;
        if(reader.parse (receivedData,datas))
            ;
        else
            return -1;


        Json::Value jsonData = datas["data"];
        Json::Value responseInfo = jsonData["response"];



        Json::Value dataType = jsonData["type"];
        Json::Value producer = jsonData["producer"];
        Json::Value value = jsonData["value"];

        this->value2 = value;

        Json::Value status = responseInfo["status"];
        Json::Value info = responseInfo["info"];

        Json::Value transactionID = datas["transactionID"];

        Json::Value sourceAddress = datas["sourceAddr"];

        this->sourceAddr = sourceAddress.toStyledString ();

        this->type = dataType.toStyledString ();
        this->producer = producer.toStyledString ();
        this->info = info.toStyledString ();
        this->status = status.toStyledString ();
        this->value = value.toStyledString ();
        this->transactionID = transactionID.toStyledString ();



        //cleanStr (this->value);
    }
    string getSourceAddr(){return cleanStr(this->sourceAddr);}
    string getType(){return cleanStr(this->type);}
    string getProducer(){return cleanStr(this->producer);}
    Json::Value getValue(){return (this->value);}
    string getStatus(){return cleanStr(this->status);}
    string getInfo(){return cleanStr(this->info);}
    string getValueStr(){return cleanStr(this->value.toStyledString ());}
    string getValueStrForInsert(){return cleanStrForInsert (this->value.toStyledString ());}
    string getValueStrForSelect(){return cleanStrForSelect (this->value.toStyledString ());}
    string getTransactionID(){return cleanStr (this->transactionID);}

    string getValue2(){return this->value2.toStyledString ();}


};




class localExecutor
{
    MYSQLHelper MH;
public:
    localExecutor(){
        MH.mysql_connect (config::host.c_str (),config::user.c_str (),config::passwd.c_str (),config::db.c_str ());
    }
    int localSelect(string sql,string &result)
    {
        return MH.mysql_select_return_json (sql,result);
    }

    int localInsert(string sql,string &result)
    {
        return MH.mysql_exec (sql,result);

    }
    int localInsertByJsonStr(string table,string sql,string &result)
    {
        return MH.mysql_insert_by_jsonstrBlock (table,sql,result);
    }
    int localCreateTable(string sql,string &result)
    {
        return MH.mysql_exec (sql,result);
    }
    int localDelete(string sql,string &result)
    {
        return MH.mysql_exec (sql,result);
    }
    int localUpdate(string sql,string &result)
    {
        return MH.mysql_exec (sql,result);
    }
    ~localExecutor()
    {
        MH.mysql_closer ();
    }


};

class transactionHandler//执行外部进来的数据或者命令
{

private:
    string receivedData;

    receivedDataResolver RDR;


    string HandleStatus;
    string HandleInfo;
    string HandleResult;
public:
    transactionHandler() {}

    string getTransactionID()
    {
        return RDR.getTransactionID ();
    }

    string getHandleStatus()
    {
        return HandleStatus;
    }
    string getValue()
    {
        return RDR.cleanStrForInsert (RDR.getValue ().toStyledString ());
    }


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
    void showSelectHandleResultToFormat()
    {
        Json::Reader reader;
        Json::Value JV;
        reader.parse (HandleResult,JV);
        Json::Value rows = JV["rows"];
        Json::Value tuples = JV["tuples"];



        string str = RDR.cleanStr(rows.toStyledString ());

        vector<string> RowsSlice;

        SplitString (str,RowsSlice,",");


     //   cout << endl;

     //   for(int index = 0 ; index < RowsSlice.size () ; index++)
    //    {
     //       cout << RowsSlice[index] << '\t';
     //   }
     //   cout << endl;

        for(int i = 0 ; i < tuples.size () ; i++) {
            if (tuples.size () > 0) {
                Json::Value::Members mem = tuples[i].getMemberNames ();
                for (auto iter = mem.begin (); iter != mem.end (); iter++) {
                    cout << (*iter) << '\t';
                }
            }
            break;
        }

        cout << endl;
        for(int i = 0 ; i < tuples.size () ; i++) {

            string value_str;
            Json::Value::Members mem = tuples[i].getMemberNames ();
            for (auto iter = mem.begin (); iter != mem.end (); iter++) {
                cout << RDR.cleanStr (tuples[i][*iter].toStyledString ());
                cout << "\t";
            }
            cout << endl;


        }

    }
    string getSelectHandleResultToFormatTest(string handleresult)
    {
        Json::Reader reader;
        Json::Value JV;
        reader.parse (handleresult,JV);
        Json::Value rows = JV["rows"];
        Json::Value tuples = JV["tuples"];



        string str = RDR.cleanStr(rows.toStyledString ());

        vector<string> RowsSlice;

        SplitString (str,RowsSlice,",");



        for(int index = 0 ; index < RowsSlice.size () ; index++)
        {
            cout << RowsSlice[index] << '\t';
        }
        cout << endl;

        for(int i = 0 ; i < tuples.size () ; i++) {

            string value_str;
            Json::Value::Members mem = tuples[i].getMemberNames ();
            for (auto iter = mem.begin (); iter != mem.end (); iter++) {
                cout << RDR.cleanStr (tuples[i][*iter].toStyledString ());
                cout << "\t";
            }
            cout << endl;

           // cout << value_str;

        }

    }
    string getHandleInfo()
    {
        return HandleInfo;
    }
    string getHandleResult()
    {
        return HandleResult;
    };

    string getProducer()
    {
        return RDR.getProducer ();
    }
    string getDestAddr()
    {
        return RDR.getSourceAddr ();
    }
    string getReceivedData()
    {
        return receivedData;
    }
    void setDataByTransactionID(string transactionID)
    {


        string re;
        while(1)
        {
            int ret = transmission::getDataFromPipeByTransactionID (transactionID,re);
            if(ret == 0) {
              //  cout << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$";
                receivedData = re;
               // cout << receivedData << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$";
                break;
            }
        }
        RDR.resolve (receivedData);
    }

    void setData(string re)
    {

        receivedData = re;
        RDR.resolve (receivedData);
    }
    string getDataByTransactionID(string transactionID)
    {

        string re;
        while(1)
        {
            int ret = transmission::getDataFromPipeByTransactionID (transactionID,re);
            if(ret == 0)
                return re;
        }
    }
    string getCommandFromPipe()
    {

        string re;
        while(1)
        {
            int ret = transmission::getSQLCommandFromPipe (re);
            if(ret == 0)
                return re;
        }
    }
    int setCommandFromPipe()
    {

        int ret;
        string re;
        while(1)
        {
            ret = transmission::getSQLCommandFromPipe (re);
            if(ret == 0) {
                receivedData = re;
                break;
            }
            else
                return -1;

        }
        RDR.resolve (receivedData);
        return ret;
    }

    //两类，一个是接收命令，一个是接受结果。

    int Handle()//这里面能够做什么事情，那么executor那里就可以发什么命令
    {

        localExecutor LE;


       //cout << receivedData <<"$$$$$$" <<endl;

        if(RDR.getType ().compare ("sql") == 0)//接受命令并执行
        {
            if(RDR.getProducer ().compare ("select") == 0)
            {
                string handleresult;

                cout << RDR.getValueStrForSelect () << endl;

                HandleStatus = to_string(LE.localSelect (RDR.getValueStrForSelect (),handleresult));

                if(HandleStatus.compare ("0") == 0) {

                    HandleResult = handleresult;
                    HandleInfo = "SUCCESS!";
                }
                else
                {
                    HandleResult = handleresult;
                    HandleInfo = "FAILED";

                }
            }
            else if(RDR.getProducer ().compare ("create") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";

            }
            else if(RDR.getProducer ().compare ("insert") == 0)
            {

                cout << RDR.getValueStrForInsert ()<< endl;
                HandleStatus = to_string(LE.localInsert(RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else if(RDR.getProducer ().compare ("update") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else if(RDR.getProducer ().compare ("delete") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else if(RDR.getProducer ().compare ("createTempTableFromSelect") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else if(RDR.getProducer ().compare ("createTempTable") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else if(RDR.getProducer ().compare ("dropTable") == 0)
            {
                HandleStatus = to_string(LE.localCreateTable (RDR.cleanStrReservMaoHao (RDR.getValue ().toStyledString ()),HandleResult));
                if(HandleStatus.compare ("0") == 0)
                    HandleInfo = "SUCCESS!";
                else
                    HandleInfo = "FAILED!";
            }
            else {
                HandleStatus = "-1";
                HandleInfo = "Unknown producer!";
                HandleResult = "Unknown producer!";
            }

            return 1;
        }
        else if(RDR.getType ().compare ("sqlresult") == 0)//接受结果并输出
        {

            HandleStatus = RDR.getStatus ();

            if(HandleStatus.compare ("0") == 0)
                HandleInfo = "SUCCESS!";
            else
                HandleInfo = "FAILED!";

            HandleResult =  RDR.getValueStrForInsert ();
            return 2;
        }
        else {
            HandleStatus = "-1";
            HandleInfo = "Unknown type!";
            HandleResult = "Unknown type!";

            return -1;
        }
    }




};



pthread_mutex_t ID_mutex;



class transactionExecutor//执行本地产生的命令
{

private:

    string getTransactionID()
    {
        pthread_mutex_lock (&ID_mutex);
        time_t tt;
        int timestamp = time(&tt);
        string transactionID = to_string(config::transactionSeed)+to_string(timestamp);
        config::transactionSeed++;
        if(config::transactionSeed == config::transactionSeedEnd)
        {
            config::transactionSeed = config::transactionSeedBegin;
        }
        transactionIDRecord[transactionID] = true;

        pthread_mutex_unlock (&ID_mutex);
        return transactionID;

    }

public:
    transactionExecutor()
    {

    }

    int localCreate(string sql,string &result)
    {
        localExecutor LE;
        return LE.localCreateTable (sql,result);

    }
    int localInsertJson(string table,string jsonStr,string &result)
    {
        localExecutor LE;
        return LE.localInsertByJsonStr (table,jsonStr,result);
    }

    int localSelect(string sql,string &result)
    {
        localExecutor LE;
        return LE.localSelect (sql,result);
    }

    int localDrop(string sql,string &result)
    {
        localExecutor LE;
        return LE.localCreateTable (sql,result);
    }
    int localInsert(string sql,string &result)
    {
        localExecutor LE;
        return LE.localInsert(sql,result);
    }
    int localDelete(string sql,string &result)
    {
        localExecutor LE;
        return LE.localDelete (sql,result);
    }
    int localUpdate(string sql,string &result)
    {
        localExecutor LE;
        return LE.localUpdate (sql,result);
    }

    string remoteSelectAll(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","select",sql,"0","selectAll"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }

    string remoteSelectAll_test(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","select",sql,"0","selectAll"));

        // transmission::sendDataToPipe (dataSend);
        return dataSend;
    }

    string remoteInsert(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","insert",sql,"0","insertdata"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }

    string remoteUpdate(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","update",sql,"0","updatedata"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }
    string remoteDelete(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","delete",sql,"0","deletedata"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }
    string remoteCreateTable(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","create",sql,"0","createTable"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }
    string remoteCreateTempTableFromSelect(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","createTempTableFromSelect",sql,"0","createTempTableFromSelect"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }
    string remoteCreateTempTable(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","createTempTable",sql,"0","createTempTable"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    };
    string remoteDropTable(string destAddr,string sql)
    {
        string transactionID = getTransactionID ();

        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sql","dropTable",sql,"0","dropTable"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    };

    //-----------两类api 一个是发出命令 一个是发出结果
    string remoteSendSelectResult(string destAddr,string dataJsonStr)
    {
        string transactionID = getTransactionID ();
        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sqlresult","select",dataJsonStr,"0","selectresult"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }
    string remoteSendResult(string transactionID,string destAddr,string dataJsonStr,string producer)
    {
        dataPacker dp;
        string dataSend = transmission::formatSendedData(transactionID,config::localIpAddr,destAddr,dp.pack ("sqlresult",producer,dataJsonStr,"0",producer+"result"));
        transmission::sendDataToPipe (dataSend);
        return transactionID;
    }

};



void *transactionResponser(void *arg)
{
    while(1) {

        transactionHandler tH;

        int res = tH.setCommandFromPipe ();


        if(res == 0) {
            cout << "接受到命令！"<<endl;
            tH.Handle ();
            cout << "处理命令！" << endl;
            string result = tH.getHandleResult ();
            transactionExecutor tE;
            cout << "发送结果！" << endl;
            tE.remoteSendResult (tH.getTransactionID (),tH.getDestAddr (), result, tH.getProducer ());
        }

        usleep(100000);
    }

}