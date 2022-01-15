//
// Created by root on 21-11-2.
//

#ifndef FFER_DATATRANSMISSION_H
#define FFER_DATATRANSMISSION_H

#endif //FFER_DATATRANSMISSION_H

#include<unistd.h>
#include "json/json.h"
#include "config.h"
#include <iostream>
#include<zmq.hpp>
#include<zmq.h>
#include<stdlib.h>
#include<time.h>
#include<list>
#include<pthread.h>
#include "thpool.h"
#include <math.h>
#include "zlib.h"
using namespace std;

threadpool thpool1;


list<string> dataSendQueue;
list<string> dataReceiveQueue;

pthread_mutex_t send_mutex;
pthread_mutex_t receive_mutex;


#include <fstream>
#include <zlib.h>
static bool gzip_compress(const std::string &original_str, std::string &str) {
    z_stream d_stream = { 0 };
    if (Z_OK != deflateInit2(&d_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 9, Z_DEFAULT_STRATEGY)) {
        return false;
    }
    uLong len = compressBound(original_str.size());
    unsigned char *buf = (unsigned char*)malloc(len);
    if (!buf) {
        return false;
    }
    d_stream.next_in = (unsigned char *)(original_str.c_str());
    d_stream.avail_in  = original_str.size();
    d_stream.next_out = buf;
    d_stream.avail_out = len;
    deflate(&d_stream, Z_SYNC_FLUSH);
    deflateEnd(&d_stream);
    str.assign((char *)buf, d_stream.total_out);
    free(buf);
    return true;
}
static void gzip_uncompress(const std::string &original_str, std::string &str) {
    unsigned char buf[102400] = "";
    uLong len = 102400;
    z_stream d_stream = { 0 };
    int res = inflateInit2(&d_stream, MAX_WBITS + 16);
    d_stream.next_in = (unsigned char *)(original_str.c_str());
    d_stream.avail_in = original_str.size();
    d_stream.next_out = buf;
    d_stream.avail_out = len;
    inflate(&d_stream, Z_SYNC_FLUSH);
    inflateEnd(&d_stream);
    str.assign((char *)buf, d_stream.total_out);
}
/* Uncompress gzip data */

/* zdata 数据 nzdata 原数据长度 data 解压后数据 ndata 解压后长度 */

int gzdecompress(Byte *zdata, uLong nzdata, Byte *data, uLong *ndata) {
    int err = 0;
    z_stream d_stream = {0}; /* decompression stream */
    static char dummy_head[2] = {
            0x8 + 0x7 * 0x10,
            (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
    };
    d_stream.zalloc = NULL;
    d_stream.zfree = NULL;
    d_stream.opaque = NULL;
    d_stream.next_in  = zdata;
    d_stream.avail_in = 0;
    d_stream.next_out = data;
    //只有设置为MAX_WBITS + 16才能在解压带header和trailer的文本
    if (inflateInit2(&d_stream, MAX_WBITS + 16) != Z_OK) {
        return-1;
    }
    while(d_stream.total_out < *ndata && d_stream.total_in < nzdata) {
        d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
        if ((err = inflate(&d_stream, Z_NO_FLUSH)) == Z_STREAM_END) {
            break;
        }
        if (err != Z_OK) {
            if (err == Z_DATA_ERROR) {
                d_stream.next_in = (Bytef*) dummy_head;
                d_stream.avail_in = sizeof(dummy_head);
                if ((err = inflate(&d_stream, Z_NO_FLUSH)) != Z_OK) {
                    return -1;
                }
            }
            else {
                return -1;
            }
        }
    }
    if (inflateEnd(&d_stream) != Z_OK) {
        return -1;
    }
    *ndata = d_stream.total_out;
    return 0;
}



#define  MAXBUFFERSIZE 98096
class CZlibMgr
{
public:
    CZlibMgr();
    ~CZlibMgr();

    bool Compress(const char* pcContentBuf, char* pcCompBuf, unsigned long& ulCompLen);  // 压缩，pcContentBuf 要压缩的内容 pcCompBuf 压缩后的内容 ulCompLen 压缩后的长度
    bool UnCompress(const char* pcCompBuf, char* pcUnCompBuf, unsigned long ulCompLen); // 解压,pcCompBuf 压缩的内容, pcUnCompBuf 解压后的内容  ulCompLen 压缩内容的长度

private:
    Byte compr[MAXBUFFERSIZE];
    Byte uncompr[MAXBUFFERSIZE];

};

CZlibMgr::CZlibMgr()
{

}

CZlibMgr::~CZlibMgr()
{
}

bool CZlibMgr::Compress(const char* pcContentBuf, char* pcCompBuf, unsigned long& ulCompLen)
{
    if (pcContentBuf == NULL)
    {
        return false;
    }

    if (strlen(pcContentBuf) == 0)
    {
        return false;
    }

    memset(compr, 0, MAXBUFFERSIZE);

    uLong comprLen;
    int err;

    uLong len = strlen(pcContentBuf);
    comprLen = sizeof(compr) / sizeof(compr[0]);

    err = compress(compr, &comprLen, (const Bytef*)pcContentBuf, len);
    if (err != Z_OK)
    {
        cout << "compess error: " << err << endl;
        return false;
    }
    cout << "orignal size: " << len << " , compressed size : " << comprLen << endl;
    memcpy(pcCompBuf, compr, comprLen);
    ulCompLen = comprLen;

    return true;
}

bool CZlibMgr::UnCompress(const char* pcCompBuf, char* pcUnCompBuf, unsigned long ulCompLen)
{
    if (pcCompBuf == NULL)
    {
        cout <<__FUNCTION__ << "================> pcCompBuf is null please to check " << endl;
        return false;
    }

    if (strlen(pcCompBuf) == 0)
    {
        cout <<__FUNCTION__ << "strlen(pcCompBuf) == 0  ========================> " << endl;
        return false;
    }

    memset(uncompr, 0, MAXBUFFERSIZE);
    uLong uncomprLen = MAXBUFFERSIZE;
    int err;

    err = uncompress(uncompr, &uncomprLen, (const Bytef *)pcCompBuf, ulCompLen);
    if (err != Z_OK)
    {
        cout << "uncompess error: " << err << endl;
        return false;
    }

    cout << "compress size: " << ulCompLen << "  uncompressed size : " << uncomprLen << endl;
    memcpy(pcUnCompBuf, uncompr, uncomprLen);

    return true;
}

CZlibMgr g_kZlibMgr;


void *dataReceiverMulBlock(void *arg)
{

    zmq::context_t context (100);
    zmq::socket_t socket (context, ZMQ_REP);
    cout << "data_receiver activitied!"<<endl;
    //绑定端口



    string conip = config::localIpAddr;
    int index = conip.find (":");

    string port = conip.substr (index,conip.size ());


    cout << "listen" << "tcp://*"+port<<endl;


    socket.bind ("tcp://*"+port);//************************************************//

    // Wait for next request from client
    //printf("\nOK!\n");


    while(1) {


        //接收命令并处理
        zmq::message_t stopMessage;

        string messAll;

        while(1) {
            zmq::message_t stopMessage;

            socket.recv (&stopMessage, 0);

            std::string receivedMessage = std::string(static_cast<char*>(stopMessage.data()),stopMessage.size());

            messAll+=receivedMessage;
            int more;
            size_t more_size = sizeof (more);
            socket.getsockopt( ZMQ_RCVMORE, &more, &more_size);
            if (!more)
                break;
        }




        //cout <<"receive "<<receivedMessage<< endl;


        pthread_mutex_lock (&receive_mutex);

        dataReceiveQueue.push_back (messAll);

        pthread_mutex_unlock (&receive_mutex);

        zmq::message_t reply (2);
        memcpy (reply.data (), "ok", 2);
        socket.send (reply);



    }


}


vector<string> *fragStr(string &str,int fragLen)
{
    int nums = str.size ()/fragLen;
    if(str.size ()%fragLen != 0)
        nums++;

    vector<string> *vs = new vector<string>();

    int left = 0;
    int right = 0;


    left = 0;
    right = fragLen-1;


    for(int i = 0 ; i < nums ;i++)
    {

        vs->push_back (str.substr (left,right-left+1));
        left = right+1;
        right+=fragLen;

    }
    return vs;



}

void dataSenderMulBlock(void *arg)
{

    pthread_mutex_lock (&send_mutex);

    string sendData = dataSendQueue.front ();
    dataSendQueue.pop_front ();

    pthread_mutex_unlock (&send_mutex);

    Json::Reader reader;
    Json::Value jv;

    reader.parse (sendData, jv);

    string destAddr = jv["destAddr"].toStyledString ();
    destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\"'), destAddr.end ());
    destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\n'), destAddr.end ());

    destAddr = "tcp://" + destAddr;


    //cout << destAddr << "%%%%%%%%%";


    zmq::context_t context (100);
    zmq::socket_t socket (context, ZMQ_REQ);
    socket.connect (destAddr);









    vector<string> *vs = fragStr (sendData,1024);

    int i = 0;
    for( ; i < vs->size ()-1 ;i++) {


        string str = (*vs)[i];
        cout << "send length:" << str.length () << endl;
        zmq::message_t msg (str.size ());
        memcpy (msg.data (), str.data (), str.size ());

        bool ok = socket.send (msg,ZMQ_SNDMORE);
        if (ok) {
            std::cout << "send success " << std::endl;
        } else {
            std::cout << "send failed" << std::endl;
        }
        //std::cout << "send message finished" << std::endl;
    }

    string str = (*vs)[i];
    cout << "send length:" << str.length () << endl;
    zmq::message_t msg (str.size ());
    memcpy (msg.data (), str.data (), str.size ());
    bool ok = socket.send (msg,0);


    if (ok) {
        std::cout << "send success " << std::endl;
    } else {
        std::cout << "send failed" << std::endl;
    }

    //  等待服务器返回的响应
    zmq::message_t reply;
    socket.recv (&reply);
    //   std::string stopSignal = std::string (static_cast<char *>(reply.data ()), reply.size ());
    //cout << stopSignal << endl;

    socket.close ();
    context.close ();
    // sleep(1);

}




void *data_receiver(void *arg)
{


    zmq::context_t context (100);
    zmq::socket_t socket (context, ZMQ_REP);
    cout << "data_receiver activitied!"<<endl;
    //绑定端口



    string conip = config::localIpAddr;
    int index = conip.find (":");

    string port = conip.substr (index,conip.size ());


    cout << "listen" << "tcp://*"+port<<endl;


    socket.bind ("tcp://*"+port);//************************************************//

    // Wait for next request from client
    //printf("\nOK!\n");


    while(1) {


        //接收命令并处理
        zmq::message_t stopMessage;
        socket.recv(&stopMessage);
        std::string receivedMessage = std::string(static_cast<char*>(stopMessage.data()),stopMessage.size());


        //cout <<"receive "<<receivedMessage<< endl;

        string res;
        unsigned char *buf = (unsigned char *)malloc(10*1024*1024);
        uLong len =10*1024*1024;
        gzdecompress((unsigned char *)(receivedMessage.c_str()), receivedMessage.size(), buf, &len);
        std::string result((char *)buf, len);

        free(buf);


        pthread_mutex_lock (&receive_mutex);

        dataReceiveQueue.push_back (result);

        pthread_mutex_unlock (&receive_mutex);

       zmq::message_t reply (2);
        memcpy (reply.data (), "ok", 2);
        socket.send (reply);



    }


}





void *data_sender(void *arg) {


    cout << "data_sender activitied!" << endl;
    // 这里选择connect 具体的地址和端口号



    while (1) {

        while (dataSendQueue.size () == 0);


        pthread_mutex_lock (&send_mutex);

        string sendData = dataSendQueue.front ();
        dataSendQueue.pop_front ();

        pthread_mutex_unlock (&send_mutex);


        Json::Reader reader;
        Json::Value jv;

        reader.parse (sendData, jv);

        string destAddr = jv["destAddr"].toStyledString ();
        destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\"'), destAddr.end ());
        destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\n'), destAddr.end ());

        destAddr = "tcp://" + destAddr;


        //cout << destAddr << "%%%%%%%%%";


        zmq::context_t context (100);
        zmq::socket_t socket (context, ZMQ_REQ);
        socket.connect (destAddr);


         // 发送的内容


        string resudlt;

        gzip_compress (sendData,resudlt);

        cout << "压前"<<sendData.size ()<<endl;
        cout << "压后"<<resudlt.size ()<<endl;


        std::string str = resudlt;

        zmq::message_t msg (str.size ());
        memcpy (msg.data (), str.data (), str.size ());
        cout << "send length:" << str.length () << endl;
        bool ok = socket.send (msg);
        if (ok) {
            std::cout << "send success " << std::endl;
        } else {
            std::cout << "send failed" << std::endl;
        }
        //std::cout << "send message finished" << std::endl;

        //  等待服务器返回的响应
        zmq::message_t reply;
        socket.recv (&reply);
        //   std::string stopSignal = std::string (static_cast<char *>(reply.data ()), reply.size ());
        //cout << stopSignal << endl;

        socket.close ();
        context.close ();
        // sleep(1);
    }
}


    void data_senderMul(void *arg)
    {

        pthread_mutex_lock (&send_mutex);

        string sendData = dataSendQueue.front ();
        dataSendQueue.pop_front ();

        pthread_mutex_unlock (&send_mutex);

        Json::Reader reader;
        Json::Value jv;

        reader.parse (sendData, jv);

        string destAddr = jv["destAddr"].toStyledString ();
        destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\"'), destAddr.end ());
        destAddr.erase (std::remove (destAddr.begin (), destAddr.end (), '\n'), destAddr.end ());

        destAddr = "tcp://" + destAddr;


        //cout << destAddr << "%%%%%%%%%";


        zmq::context_t context (100);
        zmq::socket_t socket (context, ZMQ_REQ);
        socket.connect (destAddr);


        std::string str = sendData;  // 发送的内容
        zmq::message_t msg (str.size ());
        memcpy (msg.data (), str.data (), str.size ());
        cout << "send length:" << str.length () << endl;
        bool ok = socket.send (msg);
        if (ok) {
            std::cout << "send success " << std::endl;
        } else {
            std::cout << "send failed" << std::endl;
        }
        //std::cout << "send message finished" << std::endl;

        //  等待服务器返回的响应
        zmq::message_t reply;
        socket.recv (&reply);
        //   std::string stopSignal = std::string (static_cast<char *>(reply.data ()), reply.size ());
        //cout << stopSignal << endl;

        socket.close ();
        context.close ();
        // sleep(1);

    }


int a;

    void *data_senderDeamon(void *arg)
    {

        while(1)
        {
            pthread_mutex_lock (&send_mutex);

            int length = dataSendQueue.size();
            pthread_mutex_unlock (&send_mutex);

            if(length == 0)
                ;
            else
                thpool_add_work(thpool1,dataSenderMulBlock,&a);

            usleep (100000);
        }

    }

    map<string,string*> transactionID_data;

/*
void *dataNotifier(void *arg)
{

    while(dataReceiveQueue.size () == 0);


    //cout << transactionID<<endl;

    pthread_mutex_lock (&receive_mutex);


    Json::Reader reader;

    string dataReceived;
    dataReceived = dataReceiveQueue.front ();

    Json::Value value;
    reader.parse (dataReceived, value);


    Json::Value datas;

    datas = value["data"];


        // cout << value["transactionID"] << " " << transactionID<< endl;
        // cout << datas["type"] << endl;


        if (value["transactionID"].toStyledString ().compare ("\""+transactionID+"\"\n") == 0 && datas["type"].toStyledString ().compare ("\"sqlresult\"\n") == 0 ) {

            receive = value.toStyledString ();
            dataReceiveQueue.erase (itor);
            transactionIDRecord.erase (transactionID);
            pthread_mutex_unlock (&receive_mutex);
            return 0;
        }
        else
            itor++;
    }
    pthread_mutex_unlock (&receive_mutex);

    return -1;

}

*/
/*

 {"sourceAddr":"192.168.1.1:10034","destAddr":"192.168.1.100",data":"[[...]]"}


 */

map<string,bool> transactionIDRecord;

class transmission {

public:

    static void sendDataToPipe(string data) {
        pthread_mutex_lock (&send_mutex);
        dataSendQueue.push_back (data);
        pthread_mutex_unlock (&send_mutex);
    }

    static string getDataFromPipe() {
        if (dataReceiveQueue.empty ())
            return "";
        pthread_mutex_lock (&receive_mutex);
        string re = dataReceiveQueue.front ();
        dataReceiveQueue.pop_front ();
        pthread_mutex_unlock (&receive_mutex);

        return re;

    }
    /*
    static int getDataFromNotifyByTransactionID(string transactionID,string &receive) {

        if(dataReceiveQueue.size () == 0)
            return -1;


        //cout << transactionID<<endl;

        pthread_mutex_lock (&receive_mutex);


        Json::Reader reader;
        list<string>::iterator itor;
        itor = dataReceiveQueue.begin ();

        while (itor != dataReceiveQueue.end ()) {

            Json::Value value;
            reader.parse (*itor, value);

            Json::Value datas;
            datas = value["data"];

            if (transactionIDRecord.find(transactionID) == transactionIDRecord.end()) {
                dataReceiveQueue.erase (itor);
            }

            // cout << value["transactionID"] << " " << transactionID<< endl;
            // cout << datas["type"] << endl;


            if (value["transactionID"].toStyledString ().compare ("\""+transactionID+"\"\n") == 0 && datas["type"].toStyledString ().compare ("\"sqlresult\"\n") == 0 ) {

                receive = value.toStyledString ();
                dataReceiveQueue.erase (itor);
                transactionIDRecord.erase (transactionID);
                pthread_mutex_unlock (&receive_mutex);
                return 0;
            }
            else
                itor++;
        }
        pthread_mutex_unlock (&receive_mutex);

        return -1;
    }

*/
    static int getDataFromPipeByTransactionID(string transactionID,string &receive) {

        if(dataReceiveQueue.size () == 0)
            return -1;

        usleep (100000);

        //cout << transactionID<<endl;

        pthread_mutex_lock (&receive_mutex);


        Json::Reader reader;
        list<string>::iterator itor;
        itor = dataReceiveQueue.begin ();

        while (itor != dataReceiveQueue.end ()) {

            Json::Value value;
            cout << "解析中"<<endl;
            reader.parse (*itor, value);
            cout << "解析完毕"<<endl;
            Json::Value datas;
            datas = value["data"];

            if (transactionIDRecord.find(transactionID) == transactionIDRecord.end()) {
                dataReceiveQueue.erase (itor);
            }

           // cout << value["transactionID"] << " " << transactionID<< endl;
           // cout << datas["type"] << endl;


            if (value["transactionID"].toStyledString ().compare ("\""+transactionID+"\"\n") == 0 && datas["type"].toStyledString ().compare ("\"sqlresult\"\n") == 0 ) {

                receive = value.toStyledString ();
                dataReceiveQueue.erase (itor);
                transactionIDRecord.erase (transactionID);
                pthread_mutex_unlock (&receive_mutex);
                return 0;
            }
            else
                itor++;
        }
        pthread_mutex_unlock (&receive_mutex);

        return -1;
    }


    static int getSQLCommandFromPipe(string &receive) {

        if(dataReceiveQueue.size () == 0)
            return -1;

        usleep (100000);

        //cout << transactionID<<endl;

        pthread_mutex_lock (&receive_mutex);


        Json::Reader reader;
        list<string>::iterator itor;
        itor = dataReceiveQueue.begin ();

        while (itor != dataReceiveQueue.end ()) {

            Json::Value value;
            reader.parse (*itor, value);

            Json::Value datas;
            datas = value["data"];



            if (datas["type"].toStyledString ().compare ("\"sql\"\n") == 0 ) {


                receive = value.toStyledString ();
                dataReceiveQueue.erase (itor);
                pthread_mutex_unlock (&receive_mutex);


                return 0;

            }

            else
                itor++;

        }
        pthread_mutex_unlock (&receive_mutex);

        return -1;
    }

    static string formatSendedData(string transactionID,string sourceAddr,string destAddr,string data)
    {
        Json::Value jsonStr;
        transactionIDRecord[transactionID] = true;
        jsonStr["transactionID"] = transactionID;
        jsonStr["sourceAddr"] = sourceAddr;
        jsonStr["destAddr"] = destAddr;
        Json::Reader reader;
        Json::Value jsonp;
        if(!reader.parse (data,jsonp)) {
            cout << "json error!"<<endl;
            return "";
        }
        jsonStr["data"] = jsonp;

        return jsonStr.toStyledString ();
    }


};