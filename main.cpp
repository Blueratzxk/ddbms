
#include<stdio.h>
#include<stdlib.h>
#include <iostream>
#include<zmq.hpp>
#include<zmq.h>
#include<pthread.h>

#include<string>
#include "spdlog/spdlog.h"
#include "acl_cpp/acl_cpp_define.hpp"
#include "acl_cpp/lib_acl.hpp"
#include <fstream>
#include <sstream>
#include <fitsio.h>
#include<unistd.h>



#include "sqlParser_parser_interface.h"


namespace spd=spdlog;
using namespace std;
auto console = spd::stdout_color_mt("console");
//spd::set_pattern("[%H:%M:%S] [thread %t] [%l] %v");
extern std::string objectDataDir;
int process_num = 0;








string getdatetime3()
{

    struct timeval tv;
    char buf[64];
    gettimeofday (&tv,NULL);
    strftime(buf, sizeof(buf)-1, "%Y-%m-%d-%H-%M-%S", localtime(&tv.tv_sec));
    string time = buf;
    return time;
}



class chars_resolve_to_strings
{

public:


    void resovle(vector<string> *vs,char *sep,char *s)
    {
        char *p;
        string tmp;

        p = strtok(s, sep);
        tmp = p;

        vs->push_back (tmp);

        while( p!=NULL ){
            p = strtok(NULL,sep);
            tmp = p;
            vs->push_back (tmp);
            if( p==NULL ){
                break;
            }
        }

    }
    void resolve(const char *str, vector<string>& v, const string& c)
    {
        string s(str);
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

};










int test()
{

    pthread_t id;
    int ret = pthread_create(&id, NULL, data_receiver, NULL);
    pthread_t id2;
    int ret2 = pthread_create(&id2, NULL, data_sender, NULL);
    pthread_t id3;
    int ret3 = pthread_create(&id3, NULL, transactionResponser, NULL);
    pthread_t id4;
    int ret4 = pthread_create(&id4, NULL, UI, NULL);



/*


    ETCDHelper eh;
    eh.setKey ("dfrefe","21313123131313131313");
    cout << eh.getKey ("dfrefe")<<endl;

    eh.setKey ("dfrefe()*&^%#@!","caocaocaocaocao()*&^%#@!");
    cout << eh.getKey ("dfrefe()*&^%#@!")<<endl;

    eh.setKey ("splitdata","()*frefrefe:%_21331");
    cout << eh.getKey ("splitdata")<<endl;




    cout << eh.getKey ("dsfsffwfewfew")<<endl;



    MYSQLHelper mh;
    mh.mysql_connect ("10.77.70.71","root","1234567890","ddb");
    string result;
    mh.mysql_exec ("create table student(number int primary key,name char(20),age int);",result);
    cout << result << endl;
    vector<string> rows = {"number","name","age"};
    vector<string> values = {"1234","zxk","12"};
    mh.mysql_insert ("student",rows,values);
    mh.mysql_select ("select * from student");

    string jv;
    mh.mysql_select_return_json ("select * from student",jv);
    cout << jv<<endl;
    mh.mysql_closer ();


        //Json::Value jsonRoot; //定义根节点
    Json::Value jsonItem; //定义一个子对
    jsonItem["item1"] = "one"; //添加数据
    jsonItem["item2"] = 2;
        //jsonRoot.append(jsonItem);
        //jsonItem.clear(); //清除jsonItem
    jsonItem["item1.0"] = 1.0;
    jsonItem["item2.0"] = 2.0;
        //jsonRoot["item"] = jsonItem;
    cout << jsonItem.toStyledString() << endl; //输出到控制台


    localExecutor le;

    le.localSelect ("select * from student;",jv);
    cout << jv;


    */
/*
    transactionExecutor tE;
    transactionHandler tH;
    string transactionID;

*/

    /* receivedDataResolver RDR;
     RDR.resolve (data);
     cout << RDR.getValue ()<<endl;
     cout << RDR.getStatus ()<<endl;
     cout << RDR.getInfo ()<<endl;
     cout << RDR.getProducer()<<endl;
     cout << RDR.getType ()<<endl;
 */


    // tH.setDataByTransactionID (tE.remoteSelectAll ("localhost:10034","select count(name) from student"));

    //  tH.setDataByTransactionID (tE.remoteInsert ("localhost:10034","insert into student(number,name,age) values(\"123131\",\"zxkk\",\"32\")"));

    //string senddata = "{\"rows\" : \"number,name,age\",\"tuples\" : [{\"age\" : \"12\",\"name\" : \"zxk\",\"number\" : \"1234\"},{\"age\" : \"32\",\"name\" : \"zxkk\",\"number\" : \"123131\"}]}";
    // tH.setDataByTransactionID (tE.remoteSendSelectResult ("localhost:10034",senddata));

    //tE.remoteInsert ("localhost:10034","insert into student values(\"123\",\"zzz\",\"43242432\")");

    //tE.remoteSelectAll ("localhost:10034","select * from student");
    // tH.setCommandFromPipe ();

    // cout << "!@#@!#!#!#@0";

    //cout << tH.getReceivedData ();
    //tH.Handle ();
    //cout << tH.getHandleStatus ()<<endl;
    // cout << tH.getHandleInfo ()<<endl;
    //cout << tH.getHandleResult ();

    // for(;;){}

    for(;;) {
        transactionExecutor tE;
        transactionHandler tH;

    string json = "{\"rows\" : \"number,name,age\",\"tuples\" : [{\"age\" : 12,\"name\" : \"zxk\",\"number\" : 1234},{\"age\" :32,\"name\" : \"zxkk\",\"number\" :123131}]}";

        cout << "@$@#$@$"<<tH.getSelectHandleResultToFormatTest (json);



        /*
    string result;
    cout << tE.localInsertJson ("student",json,result);
    cout << result;
    tE.localSelect ("select * from student",result);
    cout << result;
*/
        exit(0);

        string command;
        getline(cin,command);
        cout << "发送命令！" << endl;
        string ID = tE.remoteSelectAll ("localhost:10034", "select count(*) from student where name=\"zxk\"");

        // string ID = tE.remoteSelectAll ("10.77.70.73:10034", command);
        tH.setDataByTransactionID (ID);
        tH.Handle ();

        cout << "1@#!@#!@#" << tH.getHandleStatus () << endl;
        cout << "处理的结果是：" << tH.getHandleResult () << endl;

    }



//




    return 0;








    //  thpool_add_work(thpool1,get_abs_and_record_docker_and_output_mission2,&a);
    // thpool_add_work(thpool1,get_mission_and_complete_gdir_and_put_childnode,&a);
    // thpool_add_work(thpool1,executor,&a);
    // thpool_add_work(thpool1,input,&a);
    // thpool_add_work(thpool1,childnode_info_receiver,&a);
    // thpool_add_work(thpool1,docker_controlinfo_sender,&a);
    // thpool_add_work(thpool1,redis_device_port_writer,&a);
    // thpool_add_work(thpool1,redis_ccd_process_status_writer,&a);
    //  thpool_add_work(thpool1,release_temp_store,&a);





    return 0;

}



int main(int argc,char * argv[]) {





    if(argc > 1)
    {
        cout << argv[1]<<endl;
        string localIp = string(argv[1]);
        config::localIpAddr = localIp;
        vector<string> ip_port;
        SplitString (localIp,ip_port,":");
        config::host = ip_port[0];

    }




    thpool1 = thpool_init(250);


    pthread_t id;
    int ret = pthread_create (&id, NULL, data_receiver, NULL);


  //   pthread_t id;
  //    int ret = pthread_create (&id, NULL, dataReceiverMulBlock, NULL);

    pthread_t id2;
    int ret2 = pthread_create (&id2, NULL, data_sender, NULL);

  //  pthread_t id2;
   // int ret2 = pthread_create (&id2, NULL, data_senderDeamon, NULL);

    pthread_t id3;
    int ret3 = pthread_create (&id3, NULL, transactionResponser, NULL);
    pthread_t id4;
    int ret4 = pthread_create (&id4, NULL, UI, NULL);

    /*
    time_t tt;
    int now = time(&tt);

    commandUI cu;
    vector<vector<string>> commands;

    commands.push_back (
            {"lcreate 'create table student_test_temp(student_test_id int,student_test_name char(20),student_test_age int)'",
             "lcreate 'create table class_temp(class_id int,class_name char(20),class_teacher char(20))'",
             "lcreate 'create table student_test_class_temp(class_id int,class_name char(20),class_teacher char(20),student_test_id int,student_test_name char(20),student_test_age int)'"
            }
    );

    for(int i = 0 ; i < commands.size () ; i++)
    {
        for(int j = 0 ; j < commands[i].size () ; j++) {
            cu.resolveCommand (commands[i][j]);
        }
    }

    vector<string> remote = {
            "rselect2lT '10.77.70.73:10034' 'select class0.id as class_id,class0.name as class_name,class0.teacher as class_teacher,student_test0.id as student_test_id,student_test0.name as student_test_name,student_test0.age as student_test_age from class0,student_test0 where student_test0.id=class0.id''student_test_class_temp'",
            "rselect2lT '10.77.70.73:10034' 'select class0.id as class_id,class0.name as class_name,class0.teacher as class_teacher from class0''class_temp'",
            "rselect2lT '10.77.70.71:10034' 'select student_test1.id as student_test_id,student_test1.name as student_test_name,student_test1.age as student_test_age from student_test1''student_test_temp'",
            "rselect2lT '10.77.70.70:10034' 'select student_test2.id as student_test_id,student_test2.name as student_test_name,student_test2.age as student_test_age from student_test2''student_test_temp'",
            "rselect2lT '10.77.70.71:10034' 'select class1.id as class_id,class1.name as class_name,class1.teacher as class_teacher from class1''class_temp'",
            "rselect2lT '10.77.70.73:10034' 'select student_test0.id as student_test_id,student_test0.name as student_test_name,student_test0.age as student_test_age from student_test0''student_test_temp'",
            "rselect2lT '10.77.70.71:10034' 'select class1.id as class_id,class1.name as class_name,class1.teacher as class_teacher,student_test1.id as student_test_id,student_test1.name as student_test_name,student_test1.age as student_test_age from class1,student_test1 where student_test1.id=class1.id''student_test_class_temp'",
    };

    int ae;


    for(int i = 0 ; i < remote.size () ; i++)
    {


        string *s  = new string();
        *s = remote[i];
        pthread_mutex_lock (&counterLock);
        counters++;
        pthread_mutex_unlock (&counterLock);
        thpool_add_work (thpool1,remoteSmallExecutor,s);
    }

    while(counters != 0);

    commands.clear ();
    commands.push_back (
            {"lcreate 'insert into student_test_class_temp(class_id,class_name,class_teacher,student_test_id,student_test_name,student_test_age) select class_id,class_name,class_teacher,student_test_id,student_test_name,student_test_age from class_temp,student_test_temp'",
             "lselect 'select count(*) from student_test_class_temp'",
             "ldrop 'drop table student_test_class_temp'",
             "ldrop 'drop table class_temp'",
             "ldrop 'drop table student_test_temp'"}
    );

    for(int i = 0 ; i < commands.size () ; i++)
    {
        for(int j = 0 ; j < commands[i].size () ; j++)
            cu.resolveCommand (commands[i][j]);
    }
    int after = time(&tt);

    cout << "##############################################################"<<after-now << endl;




     */






 //    thpool_add_work(thpool1,docker_idle_checker,&a);

  //  distributeParserTree d;

  //  d.setJoinTables ({"class","student_test"});
   // d.setJoinTables ({"student_test"});
   // d.setProjRows ({"student_test.name","student_test.id","student_test.age","class.teacher","class.name"});
  //  d.setProjRows ({"*"});

  //  d.setJoinCondtions ({{"=","student_test.id","class.id"}});
  // d.setSelecttionConditions ({{">","student_test.age","2"},{"<","student_test.age","26"},{"=","class.name","math"}});
   // d.setSelecttionConditions ({{"<","student_test.age","50"},{">","student_test.age","11"},{"=","class.name","math"}});
   // d.setSelecttionConditions ({{"<","student_test.age","9"}});
   // d.setSelecttionConditions ({});
   // d.mulTablejoinSelectProj ();


/*
    work w;

    w.dropTable ("studenterr");

    w.createTable ("studenterr","name,age,class","char(20),int,char(20)");

    w.defineFragment ("studenterr",1,{{"between","age","0&10"},{"between","age","11&20"},{"between","age","21&30"},{"between","age","31&100"}});

    w.createRealTable ("studenterr");


    w.insertTable ("studenterr",{"zxk","23","hahaaha"});
    w.insertTable ("studenterr",{"zxkk","2","hahaaha"});
    w.insertTable ("studenterr",{"zxkd","13","hahaaha"});
    w.insertTable ("studenterr",{"zxkgg","33","hahaahfrefa"});
    w.insertTable ("studenterr",{"zxkhytj","21","hahaahfrefa"});
*/

  //  distributeParserTree dt("studenterr");
  //  dt.proj_single_table ({"age","name"},"studenterr");


    // w.insertTable ("studenterr",{"zxk","23","hahaaha"});
   // w.insertTable ("studenterr",{"zxkk","2","hahaaha"});
   // w.insertTable ("studenterr",{"zxkd","13","hahaaha"});
   // w.insertTable ("studenterr",{"zxkgg","33","hahaahfrefa"});
   // w.insertTable ("studenterr",{"zxkhytj","21","hahaahfrefa"});

   // getchar();
   // w.dropTable ("studenter");

   // w.createRealTable ();


    //fragmentDefine fd("student",1,{{"between","age","14&100"},{"between","age","0&11"},{"between","age","12&13"}});
    //fd.setTuple ({"zxk","12","e3e"});
    //cout << fd.check() << endl;

   //fragmentDefine fd2("student",2,{{"=","age","13"},{"=","age","12"},{"=","age","11"}});
  // fd2.setTuple ({"zxk","15","e3e"});
   // cout << fd2.check() << endl;


    //cout << w.defineFragment ("student",2,{{"=","age","13"},{"=","age","12"},{"=","age","11"}})<<endl;




   for(;;){
       usleep(100000);
   }

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



    return 0;

    for (;;) {


    }
        string command;
        getline (cin, command);
        cout << "发送命令！" << endl;
        string ID = tE.remoteSelectAll ("localhost:10034", "select count(*) from student where name=\"zxk\"");

        // string ID = tE.remoteSelectAll ("10.77.70.73:10034", command);
        tH.setDataByTransactionID (ID);
        tH.Handle ();

        cout << "1@#!@#!@#" << tH.getHandleStatus () << endl;
        cout << "处理的结果是：" << tH.getHandleResult () << endl;



}

