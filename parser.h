//
// Created by root on 21-11-17.
//

#ifndef FFER_PARSER_H
#define FFER_PARSER_H

#endif //FFER_PARSER_H

#include <iostream>
#include "work.h"
#include<set>
using namespace std;


/*void SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c)
{
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while (std::string::npos != pos2)
    {
        v.push_back(s.substr(pos1, pos2 - pos1));

        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != s.length())
        v.push_back(s.substr(pos1));
}*/


pthread_mutex_t counterLock;


int counters = 0;

void remoteSmallExecutor(void *arg)
{
    string *a = (string *)arg;

    string str = *a;

    free(arg);
    commandUI cu;

    time_t tt;
    int now = time(&tt);
    cout << "分片查询发出>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"<<now<<"######"<<str<<endl;
    cout << str;
    cu.resolveCommand (str);
    int after = time(&tt);
    cout << "分片查询到了<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"<<after<<"######"<<str<<endl;

    cout << str << "*****用时*****   "<<after-now << "　秒"<<endl;

    pthread_mutex_lock (&counterLock);
    counters--;
    pthread_mutex_unlock (&counterLock);
}




enum nodeTypes
{
    PROJECTION,SIMPLE_PROJECTION,
    SELECT,
    COMBINE_SINGLE_TABLE,
    CHILD_TABLE_JOIN,
    COMBINE_MUL_PAR_TABLE,
    NORMAL_TABLEJOIN,
    VERTICAL_TABLEJOIN,
    PRIMARY_TABLE_NAME,
    CHILD_TABLE_NAME,
    PUSHDOWNSELECT,
    PUSHDOWNPROJECTION
};

class node;

class tableInfo
{
public:
    vector<string> attrInfo;
    vector<string> partitionInfo;
    tableInfo() {};
};


class nodePtr
{
public:
    node *n;
    nodePtr(){};
};
class node
{
public:
    int type;
    string info;
    vector<string> data;
    tableInfo *externalData;
    list<nodePtr> np;
    node *parent;
    node() {};
};

class distributeParserTree
{
private:

    map<string,string> tables_ip;

    map<string,string> tableRows_Define;


    map<string,map<string,string>> table_Row_RowDefine;

    map<string,string> table_allRowStr;


    vector<string> rows;
    vector<string> rowsDefine;
    vector<node*> nodes;



    vector<conditionDefine> selectConditions;
    vector<conditionDefine> joinConditions;
    vector<string> projRows;

    vector<string> joinTables;


    map<string,string> globalData;

    vector<string> joinTwoNodesDisribute(vector<string> table_1, vector<string> table_2)
    {
        vector<string> result;
        vector<string> vt1 = table_1;
        vector<string> vt2 = table_2;

        for(int i = 0 ; i < vt1.size(); i++)
            for (int j = 0; j < vt2.size(); j++)
            {
                result.push_back(vt1[i] + " "+ vt2[j]);

            }
        return result;
    }
    vector<string> joinVectorNodesDisribute(vector<string> &preResult, vector<string> table_2)
    {
        vector<string> result;
        vector<string> &vt1 = preResult;
        vector<string> vt2 = table_2;
        for (int i = 0; i < vt1.size(); i++)
            for (int j = 0; j < vt2.size(); j++)
            {
                result.push_back(vt1[i] + " " + vt2[j]);

            }
        return result;
    }
    vector<string> joinPushDownNodesDisribute(vector<node*> tables)
    {
        vector<vector<string>> tables_tables;
        for (int i = 0; i < tables.size(); i++)
        {
            vector<string> childTables;
            for (list<nodePtr>::iterator it = tables[i]->np.begin(); it != tables[i]->np.end(); ++it) {
                childTables.push_back(it->n->data[0] + " " + to_string(( uintptr_t)it->n));
            }


            tables_tables.push_back(childTables);
            childTables.clear();
        }

        if (tables.size() == 2)
        {
            return joinTwoNodesDisribute(tables_tables[0], tables_tables[1]);
        }
        else if (tables.size() > 2)
        {
            vector<string> re;

            re = joinTwoNodesDisribute(tables_tables[0], tables_tables[1]);
            for (int i = 2; i < tables.size(); i++) {
                re = joinVectorNodesDisribute(re, tables_tables[i]);
            }

            return re;
        }
        else if (tables.size() == 1)
            return tables_tables[0];
        else {
            vector<string> null;
            return null;
        }
    }

    void deletePointByValueFromList(list<nodePtr> &pointers,node *value)
    {

        for (list<nodePtr>::iterator it = pointers.begin (); it != pointers.end ();) {

            if (to_string((long)(*it).n).compare (to_string((long)value)) == 0) {

                //cout << "删除了"<<value->data[0] << endl;
                pointers.erase (it++); // 删除节点，并到下一个节点
            } else {

                ++it;
            }
        }
    }


public:

    distributeParserTree(){}

    ~distributeParserTree(){


        tables_ip.clear ();

        tableRows_Define.clear ();


        rows.clear ();
        rowsDefine.clear ();

        for(int i = 0 ; i < nodes.size () ; i++)
        {
            if(nodes[i]->externalData != NULL)
                free(nodes[i]->externalData);
            nodes[i]->np.clear ();
            free(nodes[i]);
        }
        nodes.clear ();


        selectConditions.clear ();
        joinConditions.clear ();
        projRows.clear ();
        joinTables.clear ();


    }
    distributeParserTree(string tableName)
    {
        vector<string> ips;
        vector<string> frags;
        getTablePartitionipsNameInfoFromETCD (tableName,frags,ips);

        tables_ip.clear ();
        for(int i = 0 ; i < frags.size (); i++)
        {

            tables_ip[frags[i]] = ips[i];
        }
        vector<string> tableRows,rowsDefine;
        getTableAttrInfoFromETCD (tableName,tableRows,rowsDefine);
        rows = tableRows;
        this->rowsDefine = rowsDefine;

        for(int i = 0 ; i < tableRows.size () ; i++)
        {
            tableRows_Define[tableRows[i]] = rowsDefine[i];
        }

    }

    void setSelecttionConditions(vector<conditionDefine> vcd)
    {
        this->selectConditions = vcd;
    }
    void setProjRows(vector<string> projRows)
    {
        this->projRows = projRows;

    }
    void setJoinTables(vector<string> joinTables)
    {
        this->joinTables = joinTables;
    }

    void setJoinCondtions(vector<conditionDefine> vcd)
    {
        this->joinConditions = vcd;
    }
    node *addNodeToTail(int type,string info,vector<string> data)
    {
        node *newnode = new node();
        newnode->type = type;
        newnode->info = info;
        newnode->data = data;
        newnode->parent = NULL;
        newnode->externalData = NULL;
        //newnode->np = NULL;
        nodes.push_back(newnode);
        return newnode;
    }
    node* addNodeBehindNode(node* n,int type, string info, vector<string> data)
    {

        node *newnode = new node();
        newnode->type = type;
        newnode->info = info;
        newnode->data = data;
        newnode->parent = NULL;
        newnode->externalData = NULL;
        //newnode->np = NULL;
        int  i = 0;
        for (vector<node *>::iterator it = nodes.begin(); it != nodes.end(); it++, i++)
        {
            if (*it == n)
            {
                nodes.insert(nodes.begin()+i+1, newnode);
                return newnode;
            }
        }
        cout << "Cannot find the node!";
        return NULL;
    }
    void addPoinerBetweenNodes(node *parent,node *child)
    {
        nodePtr np;
        np.n = child;
        parent->np.push_back(np);
        child->parent = parent;

    }
    int addNodeFrontNodeAndModifyPointer(node *n,int type,string info,vector<string>data)
    {
        node *newnode = new node();
        newnode->type = type;
        newnode->info = info;
        newnode->data = data;
        newnode->parent = NULL;
        newnode->externalData = NULL;

        int  i = 0;
        for (vector<node *>::iterator it = nodes.begin(); it != nodes.end(); it++, i++)
        {
            if (*it == n)
            {
                nodes.insert(nodes.begin()+i, newnode);
                node *parent = n->parent;
                deletePointByValueFromList(n->parent->np,n);
                this->addPoinerBetweenNodes (parent,newnode);
                n->parent = newnode;
                newnode->parent = parent;
                addPoinerBetweenNodes (newnode,n);
                return 0;
            }
        }

        free(newnode);
        cout << "Cannot find the node!";
        return -1;

    }
    node *addNodeFrontNode(node *n,int type,string info,vector<string>data)
    {
        node *newnode = new node();
        newnode->type = type;
        newnode->info = info;
        newnode->data = data;
        newnode->parent = NULL;
        newnode->externalData = NULL;

        int  i = 0;
        for (vector<node *>::iterator it = nodes.begin(); it != nodes.end(); it++, i++)
        {
            if (*it == n)
            {
                nodes.insert(nodes.begin()+i, newnode);
                return newnode;
            }
        }

        cout << "Cannot find the node!";
        return NULL;

    }
    int deleteNode(node *n)
    {
        for (vector<node *>::iterator it = nodes.begin(); it != nodes.end();it++ )
        {

            if (*it == n)
            {
                if (n->parent != NULL) {
                    for (list<nodePtr>::iterator iit = n->parent->np.begin(); iit != n->parent->np.end(); ++iit)
                    {
                        if ((*iit).n == (*it)) {
                            //iit = n->parent->np.erase(iit);
                            iit->n = NULL;
                            break;
                        }
                    }
                }
                it = nodes.erase(it);
                if (n->externalData != NULL)
                    free(n->externalData);
                free(n);
                return 0;
            }
        }
        //cout << "Cannot find this node!"<< n->data[0]<<endl;
        return -1;
    }

    void deleteANodeModifyPointer(node *nod)
    {
        node * parent;
        parent = nod->parent;
        if(parent == NULL) {
            deleteNode (nod);
        } else{

            list<nodePtr>::iterator p1;


            for(p1=nod->np.begin();p1!=nod->np.end();p1++){
                parent->np.push_back (*p1);
                (*p1).n->parent = parent;

            }
        }

        deleteNode (nod);

    }
    void backTreeTraverseDeleteNode(node *nod)
    {

        if (nod == NULL)
            return;

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {

            backTreeTraverseDeleteNode(it->n);

        }

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end();)
        {
            if (it->n == NULL)
            {
                nod->np.erase(it++); // 删除节点，并到下一个节点
            }
            else
            {
                ++it;
            }
        }


       // cout << "delete!" << nod->data[0] << endl;
        deleteNode(nod);

    }

    void preTreeTraverse(node *nod)
    {
        if (nod == NULL)
            return;

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {
            preTreeTraverse(it->n);
        }
        for (int j = 0; j < nod->data.size(); j++)
        {
            cout << nod->data[j] << " ";
        }
        cout << endl;


    }




    void TreeTraverseByJson(node *nod,Json::Value &jsonTree)
    {
        if (nod == NULL)
            return;

        string re;
        for(int i = 0 ; i < nod->data.size () ; i++)
        {
            re+=nod->data[i]+" ";
        }
        jsonTree["content"].append (nod->info+"|"+re);

        Json::Value jv;
        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {
            TreeTraverseByJson(it->n,jv);
        }

        if(jv.size () > 0)
        jsonTree["content"].append (jv);


    }


    void treeTraverse()
    {
        if (nodes.size() > 0) {
            preTreeTraverse(nodes[0]);
        }
    }

    void printVector()
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            cout << nodes[i]->info << endl;
            for (int j = 0; j < nodes[i]->data.size(); j++)
            {
                cout << nodes[i]->data[j] << " ";
            }
            cout << endl;
        }

    }
    vector<string> getTablePartitionNameInfo(string tableName)//主表找划分表名用的
    {
        if (tableName.compare("student") == 0)
            return{ "stu1","stu2"};
        if (tableName.compare("class") == 0)
            return{ "class1","class2" };
        if (tableName.compare("teacher") == 0)
            return{ "teacher1","teacher2"};
    }

    vector<string> getTablePartitionNameInfoFromETCD(string tableName)//主表找划分表名用的
    {



        ETCDHelper EH;
        work ww;

        vector<string> vrows;
        vector<string> vrowsDefine;
        ww.getTableMetaInfo (tableName,vrows,vrowsDefine);

        string allrows;
        map<string,string> rowAndRowDefine;
        for(int i = 0 ; i < vrows.size () ; i++)
        {
            rowAndRowDefine[vrows[i]] = vrowsDefine[i];
            allrows+=(tableName+"."+vrows[i]+",");
        }
        allrows = allrows.substr (0,allrows.size ()-1);
        this->table_Row_RowDefine[tableName] = rowAndRowDefine;

        this->table_allRowStr[tableName] = allrows;




        string fragdis = EH.getKey (tableName+"_fragIpDistribute");
        vector<string> frags;
        vector<string> ips;
        if(fragdis.compare ("null") == 0)
            cout << "表划分信息不存在！"<<endl;

       // tables_ip.clear ();
        ww.stringTofragAndIp (fragdis,frags,ips);
        for(int i = 0 ; i < frags.size () ; i++)
        {
            tables_ip[frags[i]] = ips[i];

        }



        return frags;
    }
    int getTablePartitionipsNameInfoFromETCD(string tableName,vector<string> &frags,vector<string> &ips)//主表找划分表名用的
    {
        ETCDHelper EH;
        work ww;
        string fragdis = EH.getKey (tableName+"_fragIpDistribute");

        if(fragdis.compare ("null") == 0) {
            cout << "表划分信息不存在！" << endl;

            return -1;
        }
        ww.stringTofragAndIp (fragdis,frags,ips);


        return 0;

    }
    vector<string> getTableAttrInfoFromETCD(string tableName)//主表划分表找属性用的
    {
        ETCDHelper EH;
        work ww;
        string tmeta = EH.getKey (tableName+"_meta");
        if(tmeta.compare ("null") == 0)
            cout << "表元信息不存在！"<<endl;
        vector<string> rows;
        vector<string> rowsDefine;
        ww.stringToRowsAndRowsDefine (tmeta,rows,rowsDefine);
        return rows;

    }
    int getTableAttrInfoFromETCD(string tableName, vector<string> &rows,vector<string> &rowsDefine)//主表划分表找属性用的
    {
        ETCDHelper EH;
        work ww;
        string tmeta = EH.getKey (tableName+"_meta");
        if(tmeta.compare ("null") == 0) {
            cout << "表元信息不存在！" << endl;
            return -1;
        }
        ww.stringToRowsAndRowsDefine (tmeta,rows,rowsDefine);
        return 0;
    }

    vector<string> getTableAttrInfo(string tableName)//主表划分表找属性用的
    {
        if (tableName.compare("student") == 0|| tableName.find("stu")  >= 0)
            return{ "id","name","age" };
        if (tableName.compare("class") == 0 || tableName.find("class")  >= 0)
            return{ "classid","studentid","grade" };
        if (tableName.compare("teacher") == 0 || tableName.find("teacher") >= 0)
            return{ "teacherid","teachername" };
    }

    //student(id,name,age)
    //class(classid,studentid,grade)
    //stu1 20< age < 25 |stu2  25 =< age
    //class1 grade < 60 |class2 grade >= 60
    vector<string> getTablePartionInfo(string tableName)//划分表找划分形式用的
    {
        if (tableName.compare("stu1") == 0)
            return{ "age > 20","age < 25" };
        if (tableName.compare("stu2") == 0)
            return{ "age > 25","age = 25"};
        if (tableName.compare("class1") == 0)
            return{ "grade < 60" };
        if (tableName.compare("class2") == 0)
            return{ "grade > 60", "grade = 60" };
    }


    string getNodeData(node *n)
    {
        string re;
        for(int i = 0 ; i < n->data.size () ; i++)
            re+=n->data[i]+",";
        re = re.substr (0,re.size ()-1);
        return re;

    }
    string getRow_define(vector<string> rows)
    {
        string re;
        for(int i = 0 ; i < rows.size () ; i++)
            re+=(rows[i]+" "+tableRows_Define[rows[i]]+",");
        re = re.substr (0,re.size ()-1);
        return re;

    }







    void localization(node *table)
    {
        vector<string> re = getTablePartitionNameInfo(table->data[0]);
        table->type = COMBINE_SINGLE_TABLE;
        table->info = "合并表表名";
        for (int i = 0; i < re.size(); i++) {
            node *child = this->addNodeBehindNode(table, CHILD_TABLE_NAME,"子表名", {re[i]});

            child->externalData = new tableInfo();
            child->externalData->attrInfo = this->getTableAttrInfo(re[i]);
            child->externalData->partitionInfo = this->getTablePartionInfo(re[i]);

            this->addPoinerBetweenNodes(table, child);
        }

    }


    void localizationFromETCD(node *table)
    {
        vector<string> re = getTablePartitionNameInfoFromETCD(table->data[0]);
        table->type = COMBINE_SINGLE_TABLE;
        table->info = "合并表表名";
        for (int i = 0; i < re.size(); i++) {
            node *child = this->addNodeBehindNode(table, CHILD_TABLE_NAME,"子表名", {re[i],table->data[0]});

          //  child->externalData = new tableInfo();
           // child->externalData->attrInfo = this->getTableAttrInfo(re[i]);
            ///child->externalData->partitionInfo = this->getTablePartionInfo(re[i]);

            this->addPoinerBetweenNodes(table, child);


        }

    }


    void pushDownJoin(node *join)
    {
        vector<node *> tables;

        for(int i = 0 ; i < nodes.size();i++)//首先找到要连接的分表的总表‘U’,就是合并表 合并的双亲往往就是join 合并的孩子就是每个分表
        {
            if (nodes[i]->type == COMBINE_SINGLE_TABLE)
                tables.push_back(nodes[i]);
        }

        vector<string> re = this->joinPushDownNodesDisribute(tables);//得到了每个表的孩子的组合情况

        int nodes_waterline = nodes.size();

        vector<string> tablenames;
        for (int i = 0; i < tables.size(); i++) {
            tablenames.push_back (tables[i]->data[0]);
        }

        node *multablecombine = addNodeToTail(COMBINE_MUL_PAR_TABLE, "多分表合并", tablenames);

        join->parent->np.clear ();
        addPoinerBetweenNodes(join->parent, multablecombine);

        for (int i = 0; i < re.size(); i++) {

            vector<string> split;
            SplitString(re[i], split, " ");

            string joinname;
            vector<uintptr_t> nodesAddr;
            for (int j = 0; j < split.size(); j += 2)
            {
                //cout << split[j] << split[j + 1] << endl;
                joinname += "_"+split[j];
                nodesAddr.push_back(stol(split[j+1]));
            }
            vector<string> joindata;
            joindata = join->data;
            //joindata.push_back(joinname);
            node *mulchildtablejoin = addNodeToTail(CHILD_TABLE_JOIN, "分表连接", joindata);
            addPoinerBetweenNodes(multablecombine, mulchildtablejoin);

            for (int i = 0; i < nodesAddr.size(); i++)
            {

                node *n = (node*)nodesAddr[i];
                node *newn  = addNodeToTail(n->type,n->info,n->data);
                addPoinerBetweenNodes(mulchildtablejoin, newn);
            }

        }
        //cout << "&&&&&&" << endl;
        //preTreeTraverse(multablecombine);
        //cout << "&&&&&&" << endl;
        //cout << endl;

        backTreeTraverseDeleteNode(join);
    }



    void pushDownSelectAfterPushDownJoin(node *sel)
    {
        vector<node *> childTables;
        work w;
        vector<vector<string>> tablesRows;
        vector<vector<string>> tablesRowsDefines;
        for(int i = 0 ; i < joinTables.size () ; i++)
        {
            vector<string> rows;
            vector<string> rowsDefines;
            w.getTableMetaInfo (joinTables[i],rows,rowsDefines);
            tablesRows.push_back (rows);
            tablesRowsDefines.push_back (rowsDefines);
        }

        for(int i = 0 ; i < nodes.size();i++)//首先要找到每个join
        {
            if (nodes[i]->type == CHILD_TABLE_NAME)
                childTables.push_back(nodes[i]);
        }

        int mulrow = 0;
        int norow = 0;
        for(int i = 0 ; i < childTables.size () ; i++)
        {
            string tablename = childTables[i]->data[1];
            vector<string> conditions;
            for(int j = 0 ; j < selectConditions.size () ; j++)
            {
                if(selectConditions[j].left.find (".") != selectConditions[j].left.npos) {
                    if (selectConditions[j].left.find (tablename) != selectConditions[j].left.npos)
                        conditions.push_back (selectConditions[j].left + "*" + selectConditions[j].op + "*" + selectConditions[j].right);
                }
            }
            if(conditions.size () > 0) {
                this->addNodeFrontNodeAndModifyPointer (childTables[i], PUSHDOWNSELECT, "分配后选择", conditions);
            }
        }

        deleteANodeModifyPointer(sel);


    }

    void getFragInfoForCompare( map<string,vector<conditionDefine>> &info)
    {
        //表名、划分条件
        work w;

        for(int i = 0 ; i < joinTables.size () ; i++) {

            vector<string> frags;
            vector<string> ips;
            string type;
            vector<conditionDefine> vcd;
            w.getTableFragmentIp (joinTables[i],frags,ips);
            w.getTableFragmentDefine (joinTables[i],type,vcd);


            for(int j = 0 ; j < frags.size () ;j++)
            {

             //   cout << frags[j] <<"fuck"<<endl;
                if(vcd[j].op.compare ("between") == 0)
                {
                    vector<string> updown;
                    vector<conditionDefine> temp;
                    SplitString (vcd[j].right,updown,"&");

                    conditionDefine cd1(">=",vcd[j].left,updown[0]);
                    conditionDefine cd2("<=",vcd[j].left,updown[1]);
                    temp.push_back (cd1);
                    temp.push_back (cd2);
                    info[frags[j]] = temp;


                }
                else if(vcd[j].op.compare ("=") == 0)
                {
                    conditionDefine cd("=",vcd[j].left,vcd[j].right);
                    vector<conditionDefine> temp;
                    temp.push_back (cd);
                    info[frags[j]] = temp;
                }
                else
                    ;
            }

        }
      //  cout << info.size () << "hahahafuckyouall!"<<endl;


    }

    void getFragInfoForCompareAdvance( vector<map<string,vector<conditionDefine>>> &infos)//记录一个子表的条件定义的数组。然后想着是把每个子表的多个条件定义的数组分别存多份，一个map只存一个子表的一个条件定义
    {
        //表名、划分条件
        work w;

        for(int i = 0 ; i < joinTables.size () ; i++) {

            vector<string> frags;
            vector<string> ips;
            vector<string> type;
            vector<vector<conditionDefine>> vcd;
            w.getTableFragmentIp (joinTables[i],frags,ips);
            w.getTableFragmentDefineAdvance (joinTables[i],type,vcd);


            cout <<joinTables[i] <<"的大小是"<<vcd.size ()<<endl;
            for(int x = 0 ; x  < vcd.size () ; x++) {
                map<string, vector<conditionDefine>> info;
                for (int j = 0; j < frags.size (); j++) {
                 //   cout << "fuck|"<<frags[j] <<"|fuck"<<endl;
                    if (vcd[x][j].op.compare ("between") == 0) {
                        vector<string> updown;
                        vector<conditionDefine> temp;
                        SplitString (vcd[x][j].right, updown, "&");

                        conditionDefine cd1 (">=", vcd[x][j].left, updown[0]);
                        conditionDefine cd2 ("<=", vcd[x][j].left, updown[1]);
                        temp.push_back (cd1);
                        temp.push_back (cd2);
                        info[frags[j]] = temp;


                    } else if (vcd[x][j].op.compare ("=") == 0) {
                        conditionDefine cd ("=", vcd[x][j].left, vcd[x][j].right);
                        vector<conditionDefine> temp;
                        temp.push_back (cd);
                        info[frags[j]] = temp;
                    }

                }
                infos.push_back (info);


            }

        }
    //     cout << infos.size () << "hahahafuckyouall!"<<endl;



    }


    void pruneAfterJoinSelectPushDown()
    {
        vector<node *> childTables;


        map<string,vector<conditionDefine>> info;
        getFragInfoForCompare(info);


        map<string, vector<conditionDefine>>::reverse_iterator   iter;
        for(iter = info.rbegin(); iter != info.rend(); iter++){
            for(int i = 0 ; i < iter->second.size () ; i++) {
                cout << iter->first << "!!!";
                cout << iter->second[i].left << iter->second[i].op << iter->second[i].right << endl;
            }
        }



        for(int i = 0 ; i < nodes.size();i++)//首先要找到每个子表
        {
            if (nodes[i]->type == CHILD_TABLE_NAME)
                childTables.push_back(nodes[i]);
        }


       map<int,bool> needPruneIndex;
        for(int i = 0 ; i < childTables.size () ; i++)
        {
            node * sel = childTables[i]->parent;
            if(sel->type != PUSHDOWNSELECT)
                return;
            vector<string> selectCon = sel->data;
            vector<conditionDefine>vc = info[childTables[i]->data[0]];

            for(int j = 0 ; j < vc.size () ; j++) {
                cout << vc[j].left << vc[j].op << vc[j].right;
               // cout << "|versus|" << selectCon << endl;

                conditionRangeJudge crj;
                if(crj.needPrune (vc,selectCon) == 1) {
                    if(needPruneIndex.count (i) == 0)
                    {
                        needPruneIndex[i] = true;
                    }
                }

            }

        }

        map<int,bool>::iterator i;
        for (i = needPruneIndex.begin();i!=needPruneIndex.end();i++) {

           backTreeTraverseDeleteNode (childTables[i->first]->parent->parent);
        }


    }

    void pruneAfterJoinSelectPushDownAdvance()
    {
        vector<node *> childTables;


        vector<map<string,vector<conditionDefine>>> infos;
        getFragInfoForCompareAdvance(infos);

        for(int j = 0 ; j < infos.size () ; j++) {

            map<string, vector<conditionDefine>>::reverse_iterator iter;
            for (iter = infos[j].rbegin (); iter != infos[j].rend (); iter++) {
                for (int i = 0; i < iter->second.size (); i++) {
                    cout << iter->first << "!!!";
                    cout << iter->second[i].left << iter->second[i].op << iter->second[i].right << endl;
                }
            }

        }


        for(int i = 0 ; i < nodes.size();i++)//首先要找到每个子表
        {
            if (nodes[i]->type == CHILD_TABLE_NAME)
                childTables.push_back(nodes[i]);
        }






        map<int,bool> needPruneIndex;
        for(int i = 0 ; i < childTables.size () ; i++)
        {

            node * sel = childTables[i]->parent;
            if(sel->type != PUSHDOWNSELECT) {
                continue;
            }
            vector<string> selectCon = sel->data;


            for(int xx = 0 ; xx < infos.size () ; xx++) {



                vector<conditionDefine> vc = infos[xx][childTables[i]->data[0]];
                cout <<"下标"<<xx<<"的"<< childTables[i]->data[0]<<"的";
                for(int i = 0 ; i < vc.size () ; i++)
                    cout << vc[i].left<<"|"<<vc[i].op<<"|"<<vc[i].right<<endl;
//select * from zxkTest,nums where zxkTest.age > 10;

                conditionRangeJudge crj;
                if (crj.needPruneAdvance(vc, selectCon) == 1) {
                    cout << "true"<<endl;
                    if (needPruneIndex.count (i) == 0) {
                        needPruneIndex[i] = true;
                    }
                }

                    /*
                else if(crj.needPruneAdvance(vc, selectCon) == -1)
                {
                    cout << "false"<<endl;
                    if (needPruneIndex.count (i) == 0) {
                        needPruneIndex[i] = false;
                    } else {
                      //  needPruneIndex[i] *= false;
                    }
                }
                else;
                     */

            }
            cout << "["<<i<<"]"<<needPruneIndex[i]<<endl;
            cout << "#############################################################" << endl;

        }

        map<int,bool>::iterator ii;
        for (ii = needPruneIndex.begin();ii!=needPruneIndex.end();ii++) {

            if(ii->second == true)
                backTreeTraverseDeleteNode (childTables[ii->first]->parent->parent);
        }


    }

    void pushDownProAfterPrune(node *pro)
    {
        vector<node *> tables;

        for(int i = 0 ; i < nodes.size();i++)//首先找到要连接的分表的总表‘U’,就是合并表 合并的双亲往往就是join 合并的孩子就是每个分表
        {
            if (nodes[i]->type == CHILD_TABLE_NAME)
                tables.push_back(nodes[i]);
        }


        for(int i  = 0 ; i < tables.size () ; i++) {
            vector<string> projrows;

            if(projRows.size () == 1 && projRows[0].compare ("*") == 0)
            {

                string allrows;

                allrows = this->table_allRowStr[tables[i]->data[1]];
                //allrows = allrows.substr (0,allrows.size () -1);
                vector<string> prows;
                SplitString (allrows,prows,",");


                if(tables[i]->parent->type == PUSHDOWNSELECT)
                    addNodeFrontNodeAndModifyPointer (tables[i]->parent, PUSHDOWNPROJECTION, "下推后投影", prows);
                else
                    addNodeFrontNodeAndModifyPointer (tables[i], PUSHDOWNPROJECTION, "下推后投影",prows);
                continue;
            }

            for (int j = 0; j < this->projRows.size (); j++) {

              //  cout << projRows[j] <<"<----------------->"<<tables[i]->data[1] << endl;

                vector<string> projstr;
                SplitString (projRows[j],projstr,".");


                if(projRows[j].find(tables[i]->data[1])!=projRows[j].npos)
                {
                //    cout << projRows[j] <<"||||"<<tables[i]->data[1] << endl;
                    projrows.push_back (projRows[j]);
                }

            }

            //这里下推了一个连接的条件，那么当时为什么要下推连接条件中的列呢？

            for (int j = 0; j < this->joinConditions.size (); j++) {

                string lefter;
                vector<string> pair;
                SplitString (joinConditions[j].left,pair,".");
                lefter = pair[0];
                string righter;

                pair.clear ();
                SplitString (joinConditions[j].right,pair,".");
                righter = pair[0];

                if(lefter.find(tables[i]->data[1])!=lefter.npos)
                {
                    //cout << lefter <<"||||"<<tables[i]->data[1] << endl;
                    projrows.push_back(joinConditions[j].left );
                }
                if(righter.find(tables[i]->data[1])!= righter.npos)
                {
                   // cout << righter <<"||||"<<tables[i]->data[1] << endl;
                    projrows.push_back(joinConditions[j].right );
                }

            }


            set<string>s(projrows.begin(), projrows.end());
            projrows.assign(s.begin(), s.end());



            if (projrows.size () > 0) {
                if(tables[i]->parent->type == PUSHDOWNSELECT)
                    addNodeFrontNodeAndModifyPointer (tables[i]->parent, PUSHDOWNPROJECTION, "下推后投影", projrows);
                else
                    addNodeFrontNodeAndModifyPointer (tables[i], PUSHDOWNPROJECTION, "下推后投影", projrows);
            }


        }

        deleteNode (pro);

    }


    void simpleProjPushDown(node *proj)
    {
        vector<node *> childTables;

        for(int i = 0 ; i < nodes.size();i++)//首先找到要连接的分表的总表‘U’,就是合并表 合并的双亲往往就是join 合并的孩子就是每个分表
        {
            if (nodes[i]->type == CHILD_TABLE_NAME) {
                childTables.push_back (nodes[i]);
            }


        }

        for(int i = 0 ;  i < childTables.size () ; i++)
        {
            node * n = addNodeFrontNode(childTables[i],SIMPLE_PROJECTION,"投影",proj->data);
            addPoinerBetweenNodes (childTables[i]->parent,n);
            addPoinerBetweenNodes (n,childTables[i]);
        }


    }



    void sentence()
    {
        //投影+单表
        //投影+单表+选择
        //投影+多表+join
        //投影+多表+join+选择
    }

    void proj_single_table(vector<string> proj,string tableName)
    {
        node *pro = this->addNodeToTail(PROJECTION, "投影", proj);
        node *table = this->addNodeToTail(PRIMARY_TABLE_NAME, "主表名", {tableName});
        this->addPoinerBetweenNodes (pro,table);
        this->localizationFromETCD (table);
        simpleProjPushDown (pro);
        vector<string> result;
        psj_parse(pro,result);



        commandUI cu;
        for(int i = 0 ; i < result.size () ; i++) {
            cout << result[i]<<endl;
        }
        for(int i = 0 ; i < result.size () ; i++) {
            cu.resolveCommand (result[i]);
        //    cout << result[i]<<endl;
        }

        //this->printVector ();
    }

    void psj_parse(node *nod,vector<string> &result)
    {
       // cout  << "fuck!" << endl;
        if (nod == NULL)
            return;

        if(nod->type == COMBINE_SINGLE_TABLE) {
            if(getNodeData (nod->parent).compare ("*") != 0) {
                string sql =  "lcreate 'create table " +getNodeData (nod) + "temp" + " ("
                     + getRow_define (nod->parent->data)
                     + ")'" ;
                result.push_back (sql);
            } else
            {
                string allDefine;

                for(int i = 0 ; i < rows.size () ; i++)
                {
                    allDefine+=(rows[i]+" "+rowsDefine[i]+",");
                }
                allDefine = allDefine.substr (0,allDefine.size ()-1);

                string sql = "lcreate 'create table " + getNodeData (nod) + "temp" + " ("
                     + allDefine
                     + ")'";
                result.push_back (sql);
            }
        }

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {
            psj_parse(it->n,result);
        }
        if(nod->type == SIMPLE_PROJECTION)
        {
            string ip = tables_ip[getNodeData (nod->np.front ().n)];


            string sql =  "rselect2lT '"+ip+"' 'select "+getNodeData (nod)+" from " + getNodeData (nod->np.front ().n) +"' '" + getNodeData(nod->parent)+"temp'";

            result.push_back (sql);
        }


        if(nod->type == COMBINE_SINGLE_TABLE) {
            string sql = "lselect 'select " + getNodeData (nod->parent) + " from " + getNodeData (nod) + "temp'";
            result.push_back (sql);
            sql = "ldrop 'drop table "+getNodeData (nod) + "temp'";
            result.push_back (sql);
        }

    }


    void mulTablejoinSelectProj_Single()
    {



        Json::Value json;

        node *pro = this->addNodeToTail(PROJECTION, "投影", this->projRows);

        vector<string>selections;
        for(int i = 0 ; i < this->selectConditions.size () ; i++)
        {
            selections.push_back (this->selectConditions[i].left+this->selectConditions[i].op+this->selectConditions[i].right);
        }


        node *sel = this->addNodeToTail(SELECT, "选择条件", selections);

        vector<string>joinCons;

        for(int i = 0 ; i < this->joinConditions.size () ; i++)
        {
            joinCons.push_back (this->joinConditions[i].left+this->joinConditions[i].op+this->joinConditions[i].right);
        }

        node *join = this->addNodeToTail(NORMAL_TABLEJOIN, "表连接",joinCons);


        vector<node *>tables;
        for(int i = 0 ; i < joinTables .size () ; i++)
        {
            node *table = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", {joinTables[i]});
            tables.push_back (table);
        }
        this->addPoinerBetweenNodes(pro, sel);
        this->addPoinerBetweenNodes(sel, join);

        for(int i = 0 ; i <tables.size () ; i++) {
            this->addPoinerBetweenNodes (join, tables[i]);
        }

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();


        for(int i = 0 ; i < tables.size () ; i++)
        {
            this->localizationFromETCD(tables[i]);
        }

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();


        pushDownJoin(join);

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();




        pushDownSelectAfterPushDownJoin(sel);

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();

        pruneAfterJoinSelectPushDownAdvance();
        pushDownProAfterPrune(pro);


        this->printVector();
        cout << "##################" << endl;
        this->treeTraverse();
        cout << "#################" << endl;


        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;



        vector<string> result_distinctCount;
        vector<string> countResult;




        cout <<"<!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>"<<endl;

        vector<string> result;
        mulTablejoinSelectProj_Parse(nodes[0],result);


        for(int i = 0 ; i < result.size () ; i++)
            cout <<result[i] <<endl;


        vector<string> result_distinct;
        map<string,string> distinct;
        for(int i = 0 ; i < result.size () ; i++)
        {
            if(distinct.count (result[i]) == 0) {
                result_distinct.push_back (result[i]);
                distinct[result[i]] = "yes";
            }
        }


        cout <<"<------------------------------------>"<<endl;
        for(int i = 0 ; i < result_distinct.size () ; i++)
            cout <<result_distinct[i] <<endl;

        commandUI cu;
        for(int i = 0 ; i < result_distinct.size () ; i++)
        {
            cu.resolveCommand (result_distinct[i]);
        }

        /*

        for(int i = 0 ; i < post.size () ; i++)
            cout << post[i] << endl;


        cout << "**************************************"<<endl;
        for(int i = 0 ; i < bingfa.size () ;i++)
            cout << bingfa[i] << endl;
        cout << "**************************************"<<endl;

        for(int i = 0 ; i < result_distinct.size () ; i++)
            cout << result_distinct[i]<<endl;

       */

        //  for(int i = 0 ; i < result_distinct.size () ; i++)
        //     cu.resolveCommand (result_distinct[i]);
    }

    void mulTablejoinSelectProj()
    {



        Json::Value json;

        node *pro = this->addNodeToTail(PROJECTION, "投影", this->projRows);

        vector<string>selections;
        for(int i = 0 ; i < this->selectConditions.size () ; i++)
        {
            selections.push_back (this->selectConditions[i].left+this->selectConditions[i].op+this->selectConditions[i].right);
        }


        node *sel = this->addNodeToTail(SELECT, "选择条件", selections);

        vector<string>joinCons;

        for(int i = 0 ; i < this->joinConditions.size () ; i++)
        {
            joinCons.push_back (this->joinConditions[i].left+this->joinConditions[i].op+this->joinConditions[i].right);
        }

        node *join = this->addNodeToTail(NORMAL_TABLEJOIN, "表连接",joinCons);


        vector<node *>tables;
        for(int i = 0 ; i < joinTables .size () ; i++)
        {
            node *table = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", {joinTables[i]});
            tables.push_back (table);
        }
        this->addPoinerBetweenNodes(pro, sel);
        this->addPoinerBetweenNodes(sel, join);

        for(int i = 0 ; i <tables.size () ; i++) {
            this->addPoinerBetweenNodes (join, tables[i]);
        }

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();


        for(int i = 0 ; i < tables.size () ; i++)
        {
            this->localizationFromETCD(tables[i]);
        }

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();


        pushDownJoin(join);

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();




        pushDownSelectAfterPushDownJoin(sel);

        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;
        json.clear ();

        pruneAfterJoinSelectPushDownAdvance();
        pushDownProAfterPrune(pro);


        this->printVector();
        cout << "##################" << endl;
        this->treeTraverse();
        cout << "#################" << endl;


        TreeTraverseByJson(nodes[0],json);
        cout << json.toStyledString () << endl;



        vector<string> result_distinctCount;
        vector<string> countResult;




        cout <<"<!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>"<<endl;

        vector<string> result;
        mulTablejoinSelectProj_Parse(nodes[0],result);


        for(int i = 0 ; i < result.size () ; i++)
            cout <<result[i] <<endl;


        vector<string> result_distinct;
        map<string,string> distinct;
        for(int i = 0 ; i < result.size () ; i++)
        {
            if(distinct.count (result[i]) == 0) {
                result_distinct.push_back (result[i]);
                distinct[result[i]] = "yes";
            }
        }


        cout <<"<------------------------------------>"<<endl;
        for(int i = 0 ; i < result_distinct.size () ; i++)
            cout <<result_distinct[i] <<endl;


        vector<string> bingfa;


        vector<string> post;

        for(int i = 0 ; i < result_distinct.size ();)
        {

            if(result_distinct[i].find ("rselect") !=result_distinct[i].npos )
            {
                break;
            }

            post.push_back (result_distinct[0]);

            result_distinct.erase (result_distinct.begin ()+0);

        }


        for(int i = 0 ; i < result_distinct.size ();)
        {

            if(result_distinct[i].find ("rselect") !=result_distinct[i].npos )
            {

                bingfa.push_back (result_distinct[i]);
                result_distinct.erase (result_distinct.begin ()+i);
            }
            else
                i++;
        }

        commandUI cu;
        for(int i = 0 ; i < post.size () ; i++)
        {
            cu.resolveCommand (post[i]);
        }


        for(int i = 0 ; i < bingfa.size () ; i++)
        {


            string *s  = new string();
            *s = bingfa[i];
            pthread_mutex_lock (&counterLock);
            counters++;
            pthread_mutex_unlock (&counterLock);
            thpool_add_work (thpool1,remoteSmallExecutor,s);
        }





        while(counters != 0);

        for(int i = 0 ; i < result_distinct.size () ; i++)
        {
            cu.resolveCommand (result_distinct[i]);
        }

        /*

        for(int i = 0 ; i < post.size () ; i++)
            cout << post[i] << endl;


        cout << "**************************************"<<endl;
        for(int i = 0 ; i < bingfa.size () ;i++)
            cout << bingfa[i] << endl;
        cout << "**************************************"<<endl;

        for(int i = 0 ; i < result_distinct.size () ; i++)
            cout << result_distinct[i]<<endl;

       */

      //  for(int i = 0 ; i < result_distinct.size () ; i++)
       //     cu.resolveCommand (result_distinct[i]);
    }


    void mulTablejoinSelectProjCount()
    {



        Json::Value json;

        node *pro = this->addNodeToTail(PROJECTION, "投影", this->projRows);

        vector<string>selections;
        for(int i = 0 ; i < this->selectConditions.size () ; i++)
        {
            selections.push_back (this->selectConditions[i].left+this->selectConditions[i].op+this->selectConditions[i].right);
        }


        node *sel = this->addNodeToTail(SELECT, "选择条件", selections);

        vector<string>joinCons;

        for(int i = 0 ; i < this->joinConditions.size () ; i++)
        {
            joinCons.push_back (this->joinConditions[i].left+this->joinConditions[i].op+this->joinConditions[i].right);
        }

        node *join = this->addNodeToTail(NORMAL_TABLEJOIN, "表连接",joinCons);


        vector<node *>tables;
        for(int i = 0 ; i < joinTables .size () ; i++)
        {
            node *table = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", {joinTables[i]});
            tables.push_back (table);
        }
        this->addPoinerBetweenNodes(pro, sel);
        this->addPoinerBetweenNodes(sel, join);

        for(int i = 0 ; i <tables.size () ; i++) {
            this->addPoinerBetweenNodes (join, tables[i]);
        }
        for(int i = 0 ; i < tables.size () ; i++)
        {
            this->localizationFromETCD(tables[i]);
        }

        pushDownJoin(join);


        pushDownSelectAfterPushDownJoin(sel);

        pruneAfterJoinSelectPushDownAdvance();
        pushDownProAfterPrune(pro);


        vector<string> result_distinctCount;
        vector<string> countResult;


        mulTablejoinSelectProj_Parse_count(nodes[0],countResult);

        map<string,string> distinctCount;
        for(int i = 0 ; i < countResult.size () ; i++)
        {
            if(distinctCount.count (countResult[i]) == 0) {
                result_distinctCount.push_back (countResult[i]);
                distinctCount[countResult[i]] = "yes";
            }
        }

        for(int i = 0 ; i < result_distinctCount.size () ; i++) {
            cout << result_distinctCount[i] << endl;

        }


        commandUI cc;
        for(int i = 0 ; i < result_distinctCount.size () ; i++) {
            cout << result_distinctCount[i] << endl;
            cc.resolveCommand (result_distinctCount[i]);
        }


        cout <<"<!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>"<<endl;
    }


    int isOnSameAddress(vector<string> ipset)
    {
        vector<string> ips;
        for(int i = 0 ; i < ipset.size ()  ;i++)
        {
            vector<string> iptemp;
            SplitString (ipset[i],iptemp,":");
            ips.push_back (iptemp[0]);
        }
        set<string>s(ips.begin(), ips.end());
        ips.assign(s.begin(), s.end());
        if(ips.size () == 1)
            return 1;
        else
            return -1;

    }

    string rowModify(string realTableName,string row)
    {
        string mod;
        vector<string> modify;
        SplitString (row,modify,"*");

        if(modify.size () == 3)
            modify[2] = "\""+modify[2]+"\"";
        for(int i = 0 ; i < modify.size (); i++)
        {

            mod+=modify[i];
            //cout << "fucker <"<<mod <<endl;
        }
        modify.clear ();

        SplitString (mod,modify,".");

        mod = realTableName+"."+modify[1];
        return mod;

    }

    string rowModifyForSelect(string realTableName,string row,string &anothername)
    {
        string mod;
        vector<string> modify;
        SplitString (row,modify,"*");

        for(int i = 0 ; i < modify.size (); i++)
        {
            mod+=modify[i];
            //cout << "fucker <"<<mod <<endl;
        }
        modify.clear ();

        SplitString (mod,modify,".");

        mod = realTableName+"."+modify[1]+" as " + modify[0]+"_"+modify[1];
        anothername = modify[0]+"_"+modify[1];

        return mod;

    }
    string replaceJoinConditionTableName(vector<string> tablenames,string &tableCon)
    {
        vector<string> row;
        string op;
        if(tableCon.find ("=")!=tableCon.npos) {
            SplitString (tableCon, row, "=");
            op = "=";
        }
        else if(tableCon.find (">")!=tableCon.npos) {
            SplitString (tableCon, row, ">");
            op = ">";
        }
        else if(tableCon.find ("<")!=tableCon.npos) {
            SplitString (tableCon, row, "<");
            op = "<";
        }
        else if(tableCon.find (">=")!=tableCon.npos) {
            SplitString (tableCon, row, "<=");
            op = "<=";
        }
        else if(tableCon.find (">=")!=tableCon.npos) {
            SplitString (tableCon, row, ">=");
            op = ">=";
        }
        else if(tableCon.find ("!=")!=tableCon.npos) {
            SplitString (tableCon, row, "!=");
            op = "!=";
        }


        vector<vector<string>> tnames;

        for(int i = 0 ; i < row.size () ; i++) {
            vector<string> tname;
            SplitString (row[i], tname, ".");
            tnames.push_back (tname);


        }

        for(int i = 0 ; i < tnames.size () ; i++)
        {

            for(int j = 0 ; j < tablenames.size () ; j++)
            {
               // cout << tablenames[j] <<"$$$$$$$"<< tnames[i][0] << endl;
                if(tablenames[j].find (tnames[i][0]) != tablenames[j].npos)
                {

                    tnames[i][0] = tablenames[j];
                    break;
                }
            }
        }
        tableCon = tnames[0][0]+"."+tnames[0][1]+op+tnames[1][0]+"."+tnames[1][1];
        return tableCon;
    }
    string createJoinTableStr(string rows,string &storeTableName)
    {
        string result;

        vector<string> rowsNeedUse;
        SplitString (rows,rowsNeedUse,",");

        string jointablename;
        for(int j = 0 ; j < joinTables.size () ; j++)
        {
            jointablename+=joinTables[j]+"_";
        }
        jointablename = jointablename.substr (0,jointablename.size ()-1);
        jointablename+="_temp";
        storeTableName = jointablename;
        result+="lcreate 'create table "+jointablename+"(";
        for(int i = 0 ; i < rowsNeedUse.size () ; i++)
        {
            vector<string> pair;
            SplitString (rowsNeedUse[i],pair,".");
            string tablename;
            for(int j = 0 ; j < joinTables.size () ; j++)
            {
                if(pair[0].find (joinTables[j])!=pair[0].npos) {
                    tablename = joinTables[j];

                }
            }
            string kind  =  table_Row_RowDefine[tablename][pair[1]];
            result+=(tablename+"_"+pair[1]+" "+kind+ ",");

        }
        result = result.substr (0,result.size ()-1);
        result+=")'";

        return result;

    }
    string createJoinTableStrCount(string rows,string &storeTableName)
    {
        string result;

        vector<string> rowsNeedUse;
        SplitString (rows,rowsNeedUse,",");

        string jointablename;
        for(int j = 0 ; j < joinTables.size () ; j++)
        {
            jointablename+=joinTables[j]+"_";
        }
        jointablename = jointablename.substr (0,jointablename.size ()-1);
        jointablename+="_temp";
        storeTableName = jointablename;
        result+="lcreate 'create table "+jointablename+"(";

        result+="count int";

        result+=")'";


        return result;

    }

    string createSingleTableStr(string rows,string &storeTableName)
    {
        string result;
        vector<string> rowsNeedUse;
        SplitString (rows,rowsNeedUse,",");

        string jointablename;
        for(int j = 0 ; j < joinTables.size () ; j++)
        {
            if(rowsNeedUse[0].find (joinTables[j])!=rowsNeedUse[0].npos)
                jointablename = joinTables[j]+"_temp";
        }
        storeTableName = jointablename;
       // jointablename = jointablename.substr (0,jointablename.size ()-1);
        result+="lcreate 'create table "+jointablename+"(";
        for(int i = 0 ; i < rowsNeedUse.size () ; i++)
        {
            vector<string> pair;
            SplitString (rowsNeedUse[i],pair,".");
            string tablename;
            for(int j = 0 ; j < joinTables.size () ; j++)
            {
                if(pair[0].find (joinTables[j])!=pair[0].npos)
                    tablename = joinTables[j];
            }
            string kind  =  table_Row_RowDefine[tablename][pair[1]];
            result+=(tablename+"_"+pair[1]+" "+kind+ ",");

        }
        result = result.substr (0,result.size ()-1);
        result+=")'";

        return result;

    }
    string createSingleTableStrCount(string rows,string &storeTableName)
    {
        string result;
        vector<string> rowsNeedUse;
        SplitString (rows,rowsNeedUse,",");

        string jointablename;
        for(int j = 0 ; j < joinTables.size () ; j++)
        {
            if(rowsNeedUse[0].find (joinTables[j])!=rowsNeedUse[0].npos)
                jointablename = joinTables[j]+"_temp";
        }
        storeTableName = jointablename;
        // jointablename = jointablename.substr (0,jointablename.size ()-1);
        result+="lcreate 'create table "+jointablename+"(";
        result+="count int";
        result+=")'";
        cout << result<<"@!#!@#!#!";

        return result;

    }
    void mulTablejoinSelectProj_Parse(node * nod,vector<string> &result)
    {

        if(nod == NULL)
            return;

        if(nod->type == CHILD_TABLE_JOIN)//如果遇到了表链接节点
        {

            string JointoName;
            vector<string> joinTableAnotherNames;
            map<string,vector<string>> childTableAnotherNames;
            vector<string> tempTableNames;
            cout << nod->np.size ()<<"个节点！"<<endl;
            if(nod->np.size () > 1) //说明存在表连接查询，说明是多个表在查询
            {
                vector<node *>childTables;
                node * childtable;
                vector<string> ctnames;
                for (list<nodePtr>::iterator it =nod->np.begin(); it !=nod->np.end(); ++it) {
                    childtable = (*it).n;
                    while(childtable->type!=CHILD_TABLE_NAME)
                        childtable = childtable->np.front ().n;
                    childTables.push_back (childtable);
                    ctnames.push_back (childtable->data[0]);
                }

                vector<string> ips;
                string rows;
                string rowsForSelect;
                string wheres;
                string tables;
                for(int i = 0 ; i < nod->data.size () ; i++)//加入链接条件
                    wheres+=replaceJoinConditionTableName(ctnames,nod->data[i])+" and ";



                for(int i = 0 ; i < childTables.size () ; i++) {

                    ips.push_back (tables_ip[childTables[i]->data[0]]);
                    if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }
                    else if(childTables[i]->parent->type == PUSHDOWNSELECT)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                            wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";
                        for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }


                    tables+=childTables[i]->data[0]+",";
                }


                rows = rows.substr (0,rows.size () - 1);
                rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                result.insert (result.begin (),createJoinTableStr(rows,JointoName));

                if(isOnSameAddress (ips) == 1 )
                {

                    wheres = wheres.substr (0,wheres.size () - 5);
                    tables = tables.substr (0,tables.size () - 1);

                    vector<string> tablesstr;
                    SplitString (tables,tablesstr,",");

                    string re;
                    re+="rselect2lT '"+tables_ip[tablesstr[1]]+"' 'select ";
                    re+=rowsForSelect;
                    re+=" from "+tables;
                    if(wheres.size () != 0)
                        re+=" where "+wheres+"'";
                    else
                        re+="'";

                    globalData["dropAJoinTable"] = JointoName;

                    re+="'"+JointoName+"'";


                    if(selectConditions.size () > 0)
                        result.push_back (re);
                    cout << "$%$%$%$%"<<re<<"$%$%$%$%$"<<endl;



                } else
                {
                    for(int i = 0 ; i < childTables.size () ;i++)
                    {

                        string re;
                        string wheres;
                        string rows;
                        string rowsForSelect;

                        if(childTables[i]->parent->type == PUSHDOWNSELECT)
                        {
                            for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                                wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";

                            vector<string> anothernames;
                            for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                                string anothername;
                                rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                                rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anothername) + ",";
                                anothernames.push_back (anothername);
                            }

                            rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                            rows = rows.substr (0,rows.size () - 1);
                            wheres = wheres.substr (0,wheres.size () - 5);


                            string toName;

                            re+="rselect2lT '"+tables_ip[childTables[i]->data[0]]+"' 'select ";
                            re+=rowsForSelect;

                            result.insert (result.begin (),createSingleTableStr(rows,toName));
                            re+=" from "+childTables[i]->data[0];

                            childTableAnotherNames[toName] = anothernames;

                            tempTableNames.push_back (toName);

                            if(wheres.size () != 0)
                                re+=" where "+wheres+"'";
                            else
                                re+="'";

                            re+=" '"+toName+"'";

                            result.push_back (re);
                        }
                        else if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                        {
                            vector<string> anothernames;


                            cout << childTables[i]->data[0]<<"@@@@@@@@@@@@@@@@" << endl;
                            for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                                string anothername;
                                rows += rowModify (childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                                rowsForSelect+= rowModifyForSelect(childTables[i]->data[0], childTables[i]->parent->data[j],anothername) + ",";
                                anothernames.push_back (anothername);
                            }

                            rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                            rows = rows.substr (0,rows.size () - 1);
                            wheres = wheres.substr (0,wheres.size () - 5);

                            string toName;
                            re+="rselect2lT '"+tables_ip[childTables[i]->data[0]]+"' 'select ";
                            result.insert (result.begin (),createSingleTableStr(rows,toName));
                            tempTableNames.push_back (toName);

                            childTableAnotherNames[toName] = anothernames;


                            re+=rowsForSelect;
                            re+=" from "+childTables[i]->data[0]+"'";
                            re+="'"+toName+"'";

                            result.push_back (re);
                        }

                    }

                }


            } else//说明就是单个表在做查询
            {


                vector<node *>childTables;
                node * childtable;
                vector<string> ctnames;
                for (list<nodePtr>::iterator it =nod->np.begin(); it !=nod->np.end(); ++it) {
                    childtable = (*it).n;
                    while(childtable->type!=CHILD_TABLE_NAME)
                        childtable = childtable->np.front ().n;
                    childTables.push_back (childtable);
                    ctnames.push_back (childtable->data[0]);
                }

                vector<string> ips;
                string rows;
                string rowsForSelect;
                string wheres;
                string tables;
                for(int i = 0 ; i < nod->data.size () ; i++)//加入链接条件
                    wheres+=replaceJoinConditionTableName(ctnames,nod->data[i])+" and ";




                for(int i = 0 ; i < childTables.size () ; i++) {

                    ips.push_back (tables_ip[childTables[i]->data[0]]);
                    if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);

                        }
                    }
                    else if(childTables[i]->parent->type == PUSHDOWNSELECT)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                            wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";
                        for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }


                    tables+=childTables[i]->data[0]+",";


                }


                rows = rows.substr (0,rows.size () - 1);
                rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                result.insert (result.begin (),createJoinTableStr(rows,JointoName));

                wheres = wheres.substr (0,wheres.size () - 5);
                tables = tables.substr (0,tables.size () - 1);






                string re;
                re+="rselect2lT '"+tables_ip[tables]+"' 'select ";
                re+=rowsForSelect;
                re+=" from "+tables;
                if(wheres.size () != 0)
                    re+=" where "+wheres+"'";
                else
                    re+="'";


                re+="'"+JointoName+"'";

                result.push_back (re);

                globalData["singleTableCount"] = "lselect 'select count(*) from "+JointoName+"'";
                globalData["singleTableSelect"] = "lselect 'select * from "+JointoName+" limit 5'";
                globalData["singleTableDrop"] = "ldrop 'drop table "+JointoName+"'";


            }



            if(joinTableAnotherNames.size () > 0 && tempTableNames.size () > 0) {
                if (globalData.count ("childToJoinTableStr") == 0) {
                    string resultstr;
                    string temp;
                    for (int i = 0; i < joinTableAnotherNames.size (); i++) {
                        temp += joinTableAnotherNames[i] + ",";
                    }
                    temp = temp.substr (0, temp.size () - 1);
                    resultstr += "lcreate 'insert into " + JointoName + "(" + temp + ")" + " select ";

                    temp.clear ();
                    string tabletemps;
                    string tablesnames;
                    for (int i = 0; i < tempTableNames.size (); i++) {
                        for (int j = 0; j < childTableAnotherNames[tempTableNames[i]].size (); j++) {
                            temp += childTableAnotherNames[tempTableNames[i]][j] + ",";
                            tablesnames += tempTableNames[i] + "*";
                        }
                        tabletemps += tempTableNames[i] + ",";

                    }
                    tablesnames = tablesnames.substr (0, tablesnames.size () - 1);
                    temp = temp.substr (0, temp.size () - 1);
                    tabletemps = tabletemps.substr (0, tabletemps.size () - 1);

                    string joinConditionStr;
                    for(int con =  0 ; con < joinConditions.size () ; con ++)
                    {
                        vector<string> lefts;
                        SplitString (joinConditions[con].left,lefts,".");
                        string leftRaw = lefts[0];
                        lefts[0] = lefts[0]+"_temp";


                        vector<string> rights;
                        SplitString (joinConditions[con].right,rights,".");
                        string rightRaw = rights[0];
                        rights[0] = rights[0]+"_temp";

                        joinConditionStr+=(lefts[0]+"."+leftRaw+"_"+lefts[1]+joinConditions[con].op+rights[0]+"."+rightRaw+"_"+rights[1]+" and ");
                    }

                    joinConditionStr = joinConditionStr.substr (0,joinConditionStr.size ()-5);


                    resultstr += temp;

                    resultstr += " from " + tabletemps;

                    if( joinConditionStr.size () != 0)
                        resultstr+=" where "+joinConditionStr+"'";
                    else
                        resultstr+="'";

                    //resultstr+="'";


                    //result.push_back (resultstr);


                    globalData["childToJoinTableStr"] = resultstr;
                    globalData["dropTables"] = JointoName + "*" + tablesnames;
                }
            }



        }

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {
            mulTablejoinSelectProj_Parse(it->n,result);
        }

        if(nod == nodes[0])
        {
            if(globalData.count("childToJoinTableStr") > 0 && globalData.count("dropTables") > 0) {

                result.push_back (globalData["childToJoinTableStr"]);
                vector<string> tablesNeedDrop;
                SplitString (globalData["dropTables"], tablesNeedDrop, "*");
                result.push_back ("lselect 'select count(*) from " + tablesNeedDrop[0]+"'");
                result.push_back ("lselect 'select * from " + tablesNeedDrop[0]+" limit 5 '");///你这儿有个问题，就是你没有加上表的连接条件！！！！！！！！！！！！！！！！！

                for (int i = 0; i < tablesNeedDrop.size (); i++) {
                    result.push_back ("ldrop 'drop table " + tablesNeedDrop[i]+"'");
                }
                globalData.erase ("childToJoinTableStr");
                globalData.erase("dropTables");

            }
            else if(globalData.count ("dropAJoinTable") > 0)
            {
                result.push_back ("lselect 'select count(*) from " + globalData["dropAJoinTable"]+"'");
                result.push_back ("lselect 'select * from " + globalData["dropAJoinTable"]+" limit 5 '");
                result.push_back ("ldrop 'drop table " + globalData["dropAJoinTable"]+"'");


                globalData.erase ("dropAJoinTable");
            }

            if(globalData.count ("singleTableSelect") > 0)
            {
                result.push_back (globalData["singleTableSelect"]);
                result.push_back (globalData["singleTableCount"]);
                globalData.erase ("singleTableSelect");
                globalData.erase ("singleTableCount");
            }
            if(globalData.count ("singleTableDrop") > 0)
            {
                result.push_back (globalData["singleTableDrop"]);
                globalData.erase ("singleTableDrop");
            }

        }



    }

    void mulTablejoinSelectProj_Parse_count(node * nod,vector<string> &result)
    {



        if(nod == NULL)
            return;

        if(nod->type == CHILD_TABLE_JOIN)//如果遇到了表链接节点
        {

            string JointoName;
            vector<string> joinTableAnotherNames;
            map<string,vector<string>> childTableAnotherNames;
            vector<string> tempTableNames;
            cout << nod->np.size ()<<"个节点！"<<endl;
            if(nod->np.size () > 1) //说明存在表连接查询，说明是多个表在查询
            {
                vector<node *>childTables;
                node * childtable;
                vector<string> ctnames;
                for (list<nodePtr>::iterator it =nod->np.begin(); it !=nod->np.end(); ++it) {
                    childtable = (*it).n;
                    while(childtable->type!=CHILD_TABLE_NAME)
                        childtable = childtable->np.front ().n;
                    childTables.push_back (childtable);
                    ctnames.push_back (childtable->data[0]);
                }

                vector<string> ips;
                string rows;
                string rowsForSelect;
                string wheres;
                string tables;
                for(int i = 0 ; i < nod->data.size () ; i++)//加入链接条件
                    wheres+=replaceJoinConditionTableName(ctnames,nod->data[i])+" and ";



                for(int i = 0 ; i < childTables.size () ; i++) {

                    ips.push_back (tables_ip[childTables[i]->data[0]]);
                    if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }
                    else if(childTables[i]->parent->type == PUSHDOWNSELECT)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                            wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";
                        for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }


                    tables+=childTables[i]->data[0]+",";
                }


                rows = rows.substr (0,rows.size () - 1);
                rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                result.insert (result.begin (),createJoinTableStrCount (rows,JointoName));

                if(isOnSameAddress (ips) == 1 )
                {

                    wheres = wheres.substr (0,wheres.size () - 5);
                    tables = tables.substr (0,tables.size () - 1);

                    vector<string> tablesstr;
                    SplitString (tables,tablesstr,",");

                    string re;
                    re+="rselect2lT '"+tables_ip[tablesstr[1]]+"' 'select ";
                    re+=" count(*) ";
                    re+=" from "+tables;
                    if(wheres.size () != 0)
                        re+=" where "+wheres+"'";
                    else
                        re+="'";

                    globalData["dropAJoinTable"] = JointoName;

                    re+="'"+JointoName+"'";


                    if(selectConditions.size () > 0)
                        result.push_back (re);
                    cout << "$%$%$%$%"<<re<<"$%$%$%$%$"<<endl;



                } else
                {
                    for(int i = 0 ; i < childTables.size () ;i++)
                    {

                        string re;
                        string wheres;
                        string rows;
                        string rowsForSelect;

                        if(childTables[i]->parent->type == PUSHDOWNSELECT)
                        {
                            for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                                wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";

                            vector<string> anothernames;
                            for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                                string anothername;
                                rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                                rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anothername) + ",";
                                anothernames.push_back (anothername);
                            }

                            rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                            rows = rows.substr (0,rows.size () - 1);
                            wheres = wheres.substr (0,wheres.size () - 5);


                            string toName;

                            re+="rselect2lT '"+tables_ip[childTables[i]->data[0]]+"' 'select ";
                            re+=" count(*) ";

                            result.insert (result.begin (),createSingleTableStrCount (rows,toName));
                            re+=" from "+childTables[i]->data[0];

                            childTableAnotherNames[toName] = anothernames;

                            tempTableNames.push_back (toName);

                            if(wheres.size () != 0)
                                re+=" where "+wheres+"'";
                            else
                                re+="'";

                            re+=" '"+toName+"'";


                            result.push_back (re);
                        }
                        else if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                        {
                            vector<string> anothernames;


                            cout << childTables[i]->data[0]<<"@@@@@@@@@@@@@@@@" << endl;
                            for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                                string anothername;
                                rows += rowModify (childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                                rowsForSelect+= rowModifyForSelect(childTables[i]->data[0], childTables[i]->parent->data[j],anothername) + ",";
                                anothernames.push_back (anothername);
                            }

                            rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                            rows = rows.substr (0,rows.size () - 1);
                            wheres = wheres.substr (0,wheres.size () - 5);

                            string toName;
                            re+="rselect2lT '"+tables_ip[childTables[i]->data[0]]+"' 'select ";
                            result.insert (result.begin (),createSingleTableStrCount(rows,toName));
                            tempTableNames.push_back (toName);

                            childTableAnotherNames[toName] = anothernames;


                            re+=" count(*) ";
                            re+=" from "+childTables[i]->data[0]+"'";
                            re+="'"+toName+"'";

                            result.push_back (re);
                        }

                    }

                }


            } else//说明就是单个表在做查询
            {



                vector<node *>childTables;
                node * childtable;
                vector<string> ctnames;
                for (list<nodePtr>::iterator it =nod->np.begin(); it !=nod->np.end(); ++it) {
                    childtable = (*it).n;
                    while(childtable->type!=CHILD_TABLE_NAME)
                        childtable = childtable->np.front ().n;
                    childTables.push_back (childtable);
                    ctnames.push_back (childtable->data[0]);
                }

                vector<string> ips;
                string rows;
                string rowsForSelect;
                string wheres;
                string tables;
                for(int i = 0 ; i < nod->data.size () ; i++)//加入链接条件
                    wheres+=replaceJoinConditionTableName(ctnames,nod->data[i])+" and ";




                for(int i = 0 ; i < childTables.size () ; i++) {

                    ips.push_back (tables_ip[childTables[i]->data[0]]);
                    if(childTables[i]->parent->type == PUSHDOWNPROJECTION)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);

                        }
                    }
                    else if(childTables[i]->parent->type == PUSHDOWNSELECT)
                    {
                        for(int j = 0 ; j < childTables[i]->parent->data.size () ; j++)
                            wheres+=rowModify(childTables[i]->data[0],childTables[i]->parent->data[j])+" and ";
                        for(int j = 0 ; j < childTables[i]->parent->parent->data.size () ; j++) {
                            rows += rowModify(childTables[i]->data[0], childTables[i]->parent->parent->data[j]) + ",";
                            string anotherName;
                            rowsForSelect += rowModifyForSelect (childTables[i]->data[0], childTables[i]->parent->parent->data[j],anotherName) + ",";
                            joinTableAnotherNames.push_back (anotherName);
                        }
                    }


                    tables+=childTables[i]->data[0]+",";


                }


                rows = rows.substr (0,rows.size () - 1);
                rowsForSelect = rowsForSelect.substr (0,rowsForSelect.size () - 1);
                result.insert (result.begin (),createJoinTableStrCount(rows,JointoName));

                wheres = wheres.substr (0,wheres.size () - 5);
                tables = tables.substr (0,tables.size () - 1);






                string re;
                re+="rselect2lT '"+tables_ip[tables]+"' 'select ";
                re+=" count(*) ";
                re+=" from "+tables;
                if(wheres.size () != 0)
                    re+=" where "+wheres+"'";
                else
                    re+="'";



                re+="'"+JointoName+"'";

                result.push_back (re);

                globalData["singleTableSelect"] = "lselect 'select * from "+JointoName+"'";
                globalData["singleTableDrop"] = "ldrop 'drop table "+JointoName+"'";


            }



            if(joinTableAnotherNames.size () > 0 && tempTableNames.size () > 0) {
                if (globalData.count ("childToJoinTableStr") == 0) {
                    string resultstr;
                    string temp;
                    for (int i = 0; i < joinTableAnotherNames.size (); i++) {
                        temp += joinTableAnotherNames[i] + ",";
                    }
                    temp = temp.substr (0, temp.size () - 1);
                    resultstr += "lcreate 'insert into " + JointoName + "(count)" + " select ";

                    temp.clear ();
                    string tabletemps;
                    string tablesnames;
                    for (int i = 0; i < tempTableNames.size (); i++) {
                        for (int j = 0; j < childTableAnotherNames[tempTableNames[i]].size (); j++) {
                            temp += childTableAnotherNames[tempTableNames[i]][j] + ",";
                            tablesnames += tempTableNames[i] + "*";
                        }
                        tabletemps += tempTableNames[i] + ",";

                    }
                    tablesnames = tablesnames.substr (0, tablesnames.size () - 1);
                    temp = temp.substr (0, temp.size () - 1);
                    tabletemps = tabletemps.substr (0, tabletemps.size () - 1);

                    string joinConditionStr;
                    for(int con =  0 ; con < joinConditions.size () ; con ++)
                    {
                        vector<string> lefts;
                        SplitString (joinConditions[con].left,lefts,".");
                        string leftRaw = lefts[0];
                        lefts[0] = lefts[0]+"_temp";


                        vector<string> rights;
                        SplitString (joinConditions[con].right,rights,".");
                        string rightRaw = rights[0];
                        rights[0] = rights[0]+"_temp";

                        joinConditionStr+=(lefts[0]+"."+leftRaw+"_"+lefts[1]+joinConditions[con].op+rights[0]+"."+rightRaw+"_"+rights[1]+" and ");
                    }

                    joinConditionStr = joinConditionStr.substr (0,joinConditionStr.size ()-5);


                    resultstr += " * ";

                    resultstr += " from " + tabletemps;

                    if( joinConditionStr.size () != 0)
                        resultstr+=" where "+joinConditionStr+"'";
                    else
                        resultstr+="'";

                    //resultstr+="'";


                    //result.push_back (resultstr);


                    globalData["childToJoinTableStr"] = resultstr;
                    globalData["dropTables"] = JointoName + "*" + tablesnames;
                }
            }



        }

        for (list<nodePtr>::iterator it = nod->np.begin(); it != nod->np.end(); ++it) {
            mulTablejoinSelectProj_Parse_count(it->n,result);
        }

        if(nod == nodes[0])
        {
            if(globalData.count("childToJoinTableStr") > 0 && globalData.count("dropTables") > 0) {

                result.push_back (globalData["childToJoinTableStr"]);
                vector<string> tablesNeedDrop;
                SplitString (globalData["dropTables"], tablesNeedDrop, "*");
                result.push_back ("lselect 'select sum(count) from " + tablesNeedDrop[0]+"'");///你这儿有个问题，就是你没有加上表的连接条件！！！！！！！！！！！！！！！！！

                for (int i = 0; i < tablesNeedDrop.size (); i++) {
                    result.push_back ("ldrop 'drop table " + tablesNeedDrop[i]+"'");
                }
                globalData.erase ("childToJoinTableStr");
                globalData.erase("dropTables");

            }
            else if(globalData.count ("dropAJoinTable") > 0)
            {
                result.push_back ("lselect 'select sum(count) from " + globalData["dropAJoinTable"]+"'");
                result.push_back ("ldrop 'drop table " + globalData["dropAJoinTable"]+"'");


                globalData.erase ("dropAJoinTable");
            }

            if(globalData.count ("singleTableSelect") > 0)
            {
                result.push_back (globalData["singleTableSelect"]);
                globalData.erase ("singleTableSelect");
            }
            if(globalData.count ("singleTableDrop") > 0)
            {
                result.push_back (globalData["singleTableDrop"]);
                globalData.erase ("singleTableDrop");
            }

        }



    }

    void nodetest()
    {
        //其实是不是你的节点上也不需要带信息，你把从sql获得的信息存储下来，然后直接从存储的地方获得信息，然后根据信息来分配到每个节点。。
        //就是总的信息的那个节点可以不携带信息，但是分的那些节点需要携带信息
        //选择的下推，主要就是得吧选择条件一个个拿出来，然后挨个看选择条件列属于哪些表
        //然后把选择的条件挨个分配到相应表的每个子表上
        //然后要获得每个子表的划分，然后将选择的条件，和子表的划分进行比对，从而知道是否要剪枝
        //然后分配投影，根据列名知道要把投影分配到哪些表上面
        //然后分别建临时表，在本地获得数据，并进行join
        //最后把信息输出
        //也就是说，你需要在本地建立多个临时表，在让多个join对进行join。还得搞一个大表，加入每个join对的结果。

        //你可以直接获得每个子表定义，遍历每个子表，然后从选择里面拿出对应的选择条件，生成节点
        //投影也是，遍历每个子表，然后拿出相应的列生成节点

        node *pro = this->addNodeToTail(PROJECTION, "投影", { "name","grade" });
        node *sel = this->addNodeToTail(SELECT, "选择条件", { "name='123'","grade > 80" });
        node *join = this->addNodeToTail(NORMAL_TABLEJOIN, "表连接", { "student.id","class.stuid" });
        node *table1 = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", { "student" });


      //  table1->externalData = new tableInfo();
      //  table1->externalData->attrInfo = this->getTableAttrInfo("student");



        node *table2 = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", { "class" });
        //node *table3 = this->addNodeBehindNode(join, PRIMARY_TABLE_NAME, "主表名", { "teacher" });

      //  table2->externalData = new tableInfo();

       // table1->externalData->attrInfo = this->getTableAttrInfo("student");



        this->addPoinerBetweenNodes(pro, sel);
        this->addPoinerBetweenNodes(sel, join);
        this->addPoinerBetweenNodes(join, table1);
        this->addPoinerBetweenNodes(join, table2);
        //this->addPoinerBetweenNodes(join, table3);

        this->printVector();
        cout << "---------------" << endl;
        this->treeTraverse();
        cout << "---------------" << endl;
        this->localization(table1);
        this->localization(table2);
        //this->localization(table3);

        this->printVector();
        cout << "---------------" << endl;
        this->treeTraverse();
        cout << "---------------" << endl;

        pushDownJoin(join);

        //backTreeTraverseDeleteNode(table3->np.front().n);
        this->printVector();
        cout << "##################" << endl;
        this->treeTraverse();
        cout << "#################" << endl;
        exit(0);

    }


};

