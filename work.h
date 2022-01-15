//
// Created by root on 21-11-16.
//


#include "commandUI.h"
#include "ETCDHelper.h"
#include<set>
//#include "config.h"
#ifndef FFER_WORK_H
#define FFER_WORK_H

#endif //FFER_WORK_H




void SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c)
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
}

class conditionDefine
{
    bool isNumber(const string& str)
    {
        for (char const &c : str) {
            if (isdigit(c) == 0) return false;
        }
        return true;
    }
    bool isFloat(const char * str)
    {
        bool isE = false,
                isPoint = false,
                numBefore = false,
                numBehind = false;

        int index = 0;
        for (; *str != '\0'; str++, index++)
        {
            switch (*str)
            {
                case '0':case'1':case'2':case'3':case'4':case'5':
                case'6':case'7':case'8':case'9':
                    if (isE)
                    {
                        numBehind = true;
                    }
                    else
                    {
                        numBefore = true;
                    }
                    break;
                case '+':case '-':
                    if (index != 0)
                    {
                        return false;
                    }
                    break;
                case 'e':case 'E':
                    if (isE || !numBefore)
                    {
                        return false;
                    }
                    else
                    {
                        isPoint = true;
                        index = -1;
                        isE = true;
                    }
                    break;
                case '.':
                    if (isPoint)
                    {
                        return false;
                    }
                    else
                    {
                        isPoint = true;
                    }
                    break;
                default:
                    return false;
            }
        }

        if (!numBefore)
        {
            return false;
        }
        else if (isE && !numBehind)
        {
            return false;
        }

        return true;
    }


public:
    string op;
    string left;
    string right;
    int optype;

    conditionDefine(string op, string left, string right)
    {
        this->op = op;
        this->left = left;
        this->right = right;
    }

    int check()
    {

        if (op.compare("=") == 0)
        {
            optype = 1;
        }
        else if (op.compare("!=") == 0)
        {
            optype = 2;
        }
        else if (op.compare(">") == 0)
        {
            optype = 3;
        }
        else if (op.compare("<") == 0)
        {
            optype = 4;
        }
        else if (op.compare(">=") == 0)
        {
            optype = 5;
        }
        else if (op.compare("<=") == 0)
        {
            optype = 6;
        }
        else if (op.compare("between") == 0)
        {
            optype = 7;
        }
        else
            return -1;
        if (isNumber(left) == true)
        {
            return -1;
        }

        return 0;
    }

    int compare(string valueType,string valueLeft,string valueRight)
    {
        string left = valueLeft;
        string right = valueRight;


        cout <<valueLeft << " "<<valueRight<<endl;
        if(optype == 7)
        {

            if(valueType.compare ("int") == 0 || valueType.compare ("float") == 0 || valueType.compare ("double") == 0)
            {
                if(valueType.compare ("int") == 0)
                {
                    vector<string> leftright;
                    SplitString (valueRight,leftright,"&");
                    if(leftright.size () != 2)
                    {
                        cout << "between操作数不够！"<<endl;
                    }

                    if (isNumber(leftright[0]) == false || isNumber(leftright[1]) == false)
                        return -1;

                    int leftv = stoi(leftright[0]);
                    int rightv = stoi(leftright[1]);
                    int mid = stoi(valueLeft);

                   // cout << leftv <<" " << rightv << " " << mid << endl;
                    if(leftv <= mid && mid <= rightv)
                        return 1;
                    else
                        return 0;

                }
                else if(valueType.compare ("float") == 0)
                {
                    vector<string> leftright;
                    SplitString (valueRight,leftright,"&");
                    if(leftright.size () != 2)
                    {
                        cout << "between操作数不够！"<<endl;
                    }

                    if (isNumber(leftright[0]) == false || isNumber(leftright[1]) == false)
                        return 0;

                    float leftv = stof(leftright[0]);
                    float rightv = stof(leftright[1]);
                    float mid = stof(valueLeft);

                    if(leftv <= mid && mid <= rightv)
                        return 1;
                    else
                        return 0;

                }
                if(valueType.compare ("double") == 0)
                {
                    vector<string> leftright;
                    SplitString (valueRight,leftright,"&");
                    if(leftright.size () != 2)
                    {
                        cout << "between操作数不够！"<<endl;
                    }

                    if (isNumber(leftright[0]) == false || isNumber(leftright[1]) == false)
                        return -1;

                    double leftv = stod(leftright[0]);
                    double rightv = stod(leftright[1]);
                    double mid = stod(valueLeft);

                    if(leftv <= mid && mid <= rightv)
                        return 1;
                    else
                        return 0;

                }
            }
            else
                return -1;
        }



        if (valueType.find("char") != string::npos)
        {
            ;
            if (optype != 1 && optype != 2)
            {
                return -1;
            }
            else {
                int re = valueLeft.compare(valueRight);
                if (optype == 1)
                {
                    if (re == 0)
                        return 1;
                    else
                        return 0;
                }
                else
                {
                    if (re != 0)
                        return 1;
                    else
                        return 0;
                }
            }
        }
        else if (valueType.compare("int") == 0)
        {


            if (isNumber(left) == false || isNumber(right) == false)
                return -1;


            int leftv = stoi(left);
            int rightv = stoi(right);


            switch (optype)
            {
                case 1:return leftv == rightv ? 1 : 0; break;
                case 2:return leftv != rightv ? 1 : 0; break;
                case 3:return leftv > rightv ? 1 : 0; break;
                case 4:return leftv < rightv ? 1 : 0; break;
                case 5:return leftv >= rightv ? 1 : 0; break;
                case 6:return leftv <= rightv ? 1 : 0; break;
                default:return -1;
            }
        }
        else if (valueType.compare("float") == 0)
        {


            if (isFloat(left.c_str()) == false || isFloat(right.c_str()) == false)
                return -1;

            float leftv = stof(left);
            float rightv = stof(right);


            switch (optype)
            {
                case 1:return leftv == rightv ? 1 : 0; break;
                case 2:return leftv != rightv ? 1 : 0; break;
                case 3:return leftv > rightv ? 1 : 0; break;
                case 4:return leftv < rightv ? 1 : 0; break;
                case 5:return leftv >= rightv ? 1 : 0; break;
                case 6:return leftv <= rightv ? 1 : 0; break;
                default:return -1;
            }
        }
        else if (valueType.compare("double") == 0)
        {
            if (isFloat(left.c_str()) == false || isFloat(right.c_str()) == false)
                return -1;
            double leftv = stod(left);
            double rightv = stod(right);
            switch (optype)
            {
                case 1:return leftv == rightv ? 1 : 0; break;
                case 2:return leftv != rightv ? 1 : 0; break;
                case 3:return leftv > rightv ? 1 : 0; break;
                case 4:return leftv < rightv ? 1 : 0; break;
                case 5:return leftv >= rightv ? 1 : 0; break;
                case 6:return leftv <= rightv ? 1 : 0; break;
                default:return -1;
            }
        }
        else if (valueType.compare("date") == 0)
        {
            vector<string> lefts;
            vector<string> rights;
            SplitString(left, lefts, "-");
            SplitString(right, rights, "-");
            if (lefts.size() == 3)
            {
                if (lefts[0].size() == 4 && lefts[1].size() == 2 && lefts[2].size() == 2)
                    ;
                else
                    return -1;
            }
            else
                return -1;
            if (rights.size() == 3)
            {
                if (rights[0].size() == 4 && rights[1].size() == 2 && rights[2].size() == 2)
                    ;
                else
                    return -1;
            }
            else
                return -1;

            switch (optype)
            {
                case 1:return (left.compare(right) == 0) ? 1 : 0; break;
                case 2:return (left.compare(right) != 0) ? 1 : 0; break;
                case 3:return (left.compare(right) == 1) ? 1 : 0; break;
                case 4:return (left.compare(right) == -1) ? 1 : 0; break;
                case 5:return (left.compare(right) == 1|| left.compare(right) == 0) ? 1 : 0; break;
                case 6:return (left.compare(right) == -1 || left.compare(right) == 0) ? 1 : 0; break;
                default:return -1;
            }
        }
    }



};

class conditionRangeJudge
{

public:
    conditionRangeJudge()
    {

    }

    int needPrune(vector<conditionDefine> range,string needJudge )//range是确定的，字段也是确定的.needjudge字段是不确定的。有相同字段才能够去判断，没有相同字段不需要剪枝
    {
        //没有交集就可以剪枝，有交集不能剪枝
        vector<conditionDefine> needJudges;
        vector<string> judge;
            SplitString (needJudge,judge,"*");
            conditionDefine cd(judge[1],judge[0],judge[2]);
            needJudges.push_back (cd);


        if(range.size () == 1)
        {
            for(int i = 0 ; i < needJudges.size () ; i++)
                if(needJudges[i].left.compare (range[0].left) == 0)
                {
                    if(range[0].right.compare (needJudges[i].right) == 0)
                        return -1;
                }
        }

        if(range.size () == 2)
        {
            string min = range[0].right;
            string max = range[1].right;
            int minnum = stoi(min);
            int maxnum = stoi(max);

            for(int i = 0 ; i < needJudges.size () ; i++)
            {
                if(needJudges[i].left.compare (range[0].left) == 0)
                {
                    if(needJudges[i].op.compare ("=") == 0)
                    {
                        int judgenum = stoi(needJudges[i].right);
                        if(judgenum >= minnum && judgenum <= maxnum)
                        {
                            return -1;
                        }

                    }
                    else if(needJudges[i].op.compare (">") == 0)
                    {
                        int judgenum = stoi(needJudges[i].right);
                        if(judgenum <= minnum)
                            return -1;

                    }
                    else if(needJudges[i].op.compare ("<") == 0)
                    {
                        int judgenum = stoi(needJudges[i].right);
                        if(judgenum >= maxnum)
                            return -1;

                    }
                    else if(needJudges[i].op.compare (">=") == 0)
                    {
                        int judgenum = stoi(needJudges[i].right);
                        if(judgenum < minnum)
                            return -1;

                    }
                    else if(needJudges[i].op.compare ("<=") == 0)
                    {
                        int judgenum = stoi(needJudges[i].right);
                        if(judgenum > maxnum)
                            return -1;
                    }
                    else
                        return -1;
                }
            }
        }

        return 1;
    }

    int needPrune(vector<conditionDefine> range,vector<string> needJudge )//range是确定的，字段也是确定的.needjudge字段是不确定的。有相同字段才能够去判断，没有相同字段不需要剪枝
    {
        //没有交集就可以剪枝，有交集不能剪枝

        if(range.size () == 0 || needJudge.size () == 0)
            return -1;


        int count = 0;

        //select * from nums,zxkTest where zxkTest.age > 6;
        for(int i  = 0 ; i < range.size () ; i++)
            cout <<"@@"<< range[i].left<<" "<< range[i].op<<" "<<range[i].right<<endl;

        for(int i  = 0 ; i < needJudge.size () ; i++)
            cout << "$$"<<needJudge[i]<<endl;

        for(int i = 0 ; i < needJudge.size () ; i++) {
            vector<string> judge;

            SplitString (needJudge[i], judge, "*");


            conditionDefine needJudges (judge[1], judge[0], judge[2]);


            if (range.size () == 1) {
                //cout << needJudges.left<<"@@@!@" <<range[0].left<<"@@@@"<<endl;
                if (needJudges.left.find (range[0].left) != needJudges.left.npos) {
                    // cout << needJudges.right << "%%%%" << range[0].right << endl;
                    if (range[0].right.compare (needJudges.right) == 0)
                        count++;
                }
            }

            if (range.size () == 2) {
                string min = range[0].right;
                string max = range[1].right;
                int minnum = stoi (min);
                int maxnum = stoi (max);


                cout << "最小" << minnum << "最大" << maxnum << endl;
                if (needJudges.left.find (range[0].left) != needJudges.left.npos) {
                    if (needJudges.op.compare ("=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum >= minnum && judgenum <= maxnum) {
                            count++;
                        }

                    } else if (needJudges.op.compare (">=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum <= maxnum) {
                            count++;
                        }

                    } else if (needJudges.op.compare ("<=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum >= minnum) {

                            count++;
                        }

                    } else if (needJudges.op.compare (">") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum < maxnum) {

                            count++;
                        }

                    } else if (needJudges.op.compare ("<") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum > minnum) {

                            count++;
                        }
                    } else
                        return -1;
                }
            }


        }

        if(range.size () == 2) {
            if (count == needJudge.size ())
                return -1;//不能剪枝
            else
                return 1;//可以剪枝
        }
        if(range.size () == 1)
        {
            if(count > 0)
                return -1;//有对上的，不能剪枝
            else
                return 1;//没有对上的可以剪枝

        }
    }
    int needPruneAdvance(vector<conditionDefine> range,vector<string> needJudge )//range是确定的，字段也是确定的.needjudge字段是不确定的。有相同字段才能够去判断，没有相同字段不需要剪枝
    {
        //没有交集就可以剪枝，有交集不能剪枝

        if(range.size () == 0 || needJudge.size () == 0) {

            if(range.size () == 0)
                cout << "range 是空的！"<<endl;
            if(needJudge.size () == 0)
                cout << "needjudge 是空的！"<<endl;
            return -2;
        }


        int count = 0;

        //select * from nums,zxkTest where zxkTest.age > 6;
        for(int i  = 0 ; i < range.size () ; i++)
            cout <<"@@"<< range[i].left<<" "<< range[i].op<<" "<<range[i].right<<endl;

        for(int i  = 0 ; i < needJudge.size () ; i++)
            cout << "$$"<<needJudge[i]<<endl;


        int isfind = 0;
        int nofindnum=0;
        for(int i = 0 ; i < needJudge.size () ; i++) {
            vector<string> judge;

            SplitString (needJudge[i], judge, "*");


            conditionDefine needJudges (judge[1], judge[0], judge[2]);


            if (range.size () == 1) {
               // cout << needJudges.left<<"@@@!@" <<range[0].left<<"@@@@"<<endl;
                if (needJudges.left.find (range[0].left) != needJudges.left.npos) {
                    // cout << needJudges.right << "%%%%" << range[0].right << endl;
                    if(needJudges.right.front ()=='\'')
                    {
                        needJudges.right  = needJudges.right.substr (1,needJudges.right.size ()-2);

                    }
                    if (range[0].right.compare (needJudges.right) == 0)
                        count++;
                    isfind=1;
                }
                else
                    nofindnum++;


            }

            if (range.size () == 2) {
                string min = range[0].right;
                string max = range[1].right;
                int minnum = stoi (min);
                int maxnum = stoi (max);


                cout << "最小" << minnum << "最大" << maxnum << endl;
                if (needJudges.left.find (range[0].left) != needJudges.left.npos) {

                    cout << "$$$$$$$"<<needJudges.op<<"|"<<needJudges.right<<endl;
                    isfind=1;
                    if (needJudges.op.compare ("=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum >= minnum && judgenum <= maxnum) {
                            count++;
                        }


                    } else if (needJudges.op.compare (">=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum <= maxnum) {
                            count++;
                        }


                    } else if (needJudges.op.compare ("<=") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum >= minnum) {

                            count++;
                        }


                    } else if (needJudges.op.compare (">") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum < maxnum) {

                            count++;
                        }


                    } else if (needJudges.op.compare ("<") == 0) {
                        int judgenum = stoi (needJudges.right);
                        if (judgenum > minnum) {

                            count++;
                        }

                    }
                } else
                    nofindnum++;

            }


        }
        if(isfind == 0)
            return -2;

        if(range.size () == 2) {
            if (count == (needJudge.size ()-nofindnum)) {
                cout << "不能剪枝！"<<endl;
                return -1;//不能剪枝
            }
            else {
                cout << "能剪枝！"<<endl;
                return 1;//可以剪枝
            }
        }
        if(range.size () == 1)
        {
            if(count == (needJudge.size ()-nofindnum)) {
                cout << "不能剪枝！"<<endl;
                return -1;//有对上的，不能剪枝
            }
            else {
                cout << "能剪枝！"<<endl;
                return 1;//没有对上的可以剪枝
            }

        }
    }
};

class fragmentDefine
{
private:
    ETCDHelper EH;
    int type;

    vector<string> tableRows;
    vector<string> tableRowDefines;
    vector<conditionDefine> conditions;
    vector<string> tuple;
    vector<vector<string>> tuples;
    string tableinfo;


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

public:
    fragmentDefine(string tableName,int type,vector<conditionDefine> conditions)
    {
        this->type = type;
        this->conditions = conditions;
        tableinfo = EH.getKey (tableName+"_meta");

        vector<string> re;
        SplitString (tableinfo,re,"_");
        if(re.size () != 2)
        {
            cout << "table元数据有错误！"<<endl;
        }
        SplitString (re[0],tableRows,",");
        SplitString (re[1],tableRowDefines,",");

    }

    fragmentDefine(string tableName)
    {

        tableinfo = EH.getKey (tableName+"_meta");

        vector<string> re;
        SplitString (tableinfo,re,"_");
        if(re.size () != 2)
        {
            cout << "table元数据有错误！"<<endl;
        }
        SplitString (re[0],tableRows,",");
        SplitString (re[1],tableRowDefines,",");

    }

    void setTypeConditions(int type,vector<conditionDefine> conditions)
    {
        this->type = type;
        this->conditions = conditions;
    }

    int conditionsCheck()
    {
        for (int i = 0; i < this->conditions.size(); i++)
        {

            if(this->conditions[i].check() == -1)
                return -1;
        }
        return 0;
    }
    int conditionChecker(conditionDefine cd)
    {


        if(type == 1)
        {
            if(cd.op.compare ("=") == 0||cd.op.compare ("!=") == 0)
                return -1;
        }
        else if(type == 2)
        {
            if(cd.op.compare ("=") != 0)
            {
                return -1;
            }
        }
        else
        {
            cout << "未定义的划分方式！"<<endl;
            return -1;
        }
        int index = -1;
        for (int i = 0; i < tableRows.size(); i++)
        {
            if (tableRows[i].compare(cd.left) == 0)
            {
                index = i;
                break;
            }
        }
        if (index == -1) {
            cout << "未定义列!"<< endl;
            return -1;
        }

        string type = tableRowDefines[index];
        string valueLeft = tuple[index];
        string valueRight = cd.right;

        return cd.compare(type, valueLeft, valueRight);

    }

    int fragmentDefineCheck()
    {
        for(int i = 0 ; i < conditions.size ();i++) {
            conditionDefine cd = conditions[i];
            if (type == 1) {
                if (cd.op.compare ("=") == 0 || cd.op.compare ("!=") == 0)
                    return -1;
            } else if (type == 2) {
                if (cd.op.compare ("=") != 0) {
                    return -1;
                }
            } else {
                cout << "未定义的划分方式！" << endl;
                return -1;
            }
        }
    }

    void setTuple(vector<string> tuple)
    {
        this->tuple = tuple;
    }

    int check()
    {

        if(conditionsCheck() == -1)
            return -1;

        if(tableinfo.compare ("null") == 0)
            return -1;

        for(int i = 0 ; i < conditions.size () ;i++)
        {
            if(conditionChecker (conditions[i]) == 1)
                return i;
        }
        return -2;//未找到满足的划分

    }
    int checkAll(vector<int> &selectors)
    {

        if(conditionsCheck() == -1)
            return -1;

        if(tableinfo.compare ("null") == 0)
            return -1;

        for(int i = 0 ; i < conditions.size () ;i++)
        {
            if(conditionChecker (conditions[i]) == 1) {
                selectors.push_back (i);
            }
        }
        return 0;//未找到满足的划分

    }
};


class work {
    ETCDHelper EH;
    commandUIInternal cu;






public:

    work() {}


    void stringToRowsAndRowsDefine(string str,vector<string> &rows,vector<string> &rowsDefine)
    {

        //cout << str<<endl;
        vector<string> tbinfo;
        SplitString (str,tbinfo,"_");
        SplitString (tbinfo[0],rows,",");
        SplitString (tbinfo[1],rowsDefine,",");

    }


    void stringTofragDefine(string str,string &type,vector<conditionDefine> &fragDefine)
    {

        //cout << str<<endl;
        vector<string> typef;

        SplitString (str,typef,"*");

        if(typef.size () > 1)
            type = typef[0];
        else
            type = "-1";

        vector<string> fragd;

        if(typef.size () > 1)
            SplitString (typef[1],fragd,"_");
        else
            SplitString (typef[0],fragd,"_");

        for(int  i = 0 ; i < fragd.size () ; i++)
        {
            vector<string> fd;
            SplitString (fragd[i],fd,",");
            conditionDefine cd(fd[0],fd[1],fd[2]);
            fragDefine.push_back (cd);
        }
    }
    void stringTofragDefineAdvance(string str,vector<string> &type,vector<vector<conditionDefine>> &fragDefine)
    {

        vector<string> singleFragDefine;
        SplitString (str,singleFragDefine,"_");

        vector<vector<string>> childDefines;





        for(int i = 0 ; i < singleFragDefine.size () ; i++)
        {
            vector<string> childDefine;
            SplitString (singleFragDefine[i],childDefine,"|");
            childDefines.push_back (childDefine);
        }




        vector<vector<string>> rowFragDefines;

        int len = childDefines[0].size ();



        for(int i = 0 ; i < len ; i ++)
        {
            vector<string> rowFrag;
            for(int j = 0 ; j < childDefines.size () ; j++)
            {
                rowFrag.push_back (childDefines[j][i]);
            }
            rowFragDefines.push_back (rowFrag);
            rowFrag.clear ();
        }


        for(int i = 0 ; i < rowFragDefines.size () ; i++)
        {

            vector<conditionDefine> cds;

            for(int j = 0 ; j < rowFragDefines[i].size () ;j++)
            {

                vector<string> temp;
                SplitString (rowFragDefines[i][j],temp,",");
                conditionDefine cd(temp[0],temp[1],temp[2]);
                cds.push_back (cd);
            }
            fragDefine.push_back (cds);

            if(cds[0].op.compare ("=") == 0)
                type.push_back ("2");
            else
                type.push_back ("1");
            cds.clear ();
        }



       // cout <<type.size ()<<endl;
       // cout << "你在获取信息这里打了断点！"<<endl;
      //  exit(0);
    }
    void stringTofragAndIp(string str,vector<string> &frags,vector<string> &ips)
    {
        //cout << str<<endl;
        vector<string> tbinfo;
        SplitString (str,tbinfo,"*");

        for(int i = 0 ; i < tbinfo.size () ; i++) {
            vector<string> fi;
            SplitString (tbinfo[i], fi, ",");
            frags.push_back (fi[0]);
            ips.push_back (fi[1]);

        }
    }

    int addTableList(string tableName)
    {
        string key = "tableList";

        string tableList = EH.getKey (key);
        if(tableList.compare ("null") == 0)
        {
            EH.setKey (key,tableName);
        } else
        {
            string tableListStr = EH.getKey (key)+"-"+tableName;
           // cout << "%%%" << tableListStr<<endl;
            EH.setKey (key,tableListStr);
           // cout << "###" << EH.getKey (key);
        }


    }

    void showTables(vector<string> &tables)
    {
        string tableStr = EH.getKey ("tableList");
      //  cout << "&&&"<<tableStr<<endl;
        SplitString (tableStr,tables,"-");
    }
    void deleteTableFromList(string tableName)
    {
        string key = "tableList";
        string tableList = EH.getKey (key);
        vector<string> tables;
        SplitString (tableList,tables,"-");

        string tableListStr;
        for(int i = 0 ; i < tables.size () ; i++)
        {
            if(tables[i].compare (tableName) != 0)
            {
                tableListStr+=(tables[i]+"-");
            }
        }
        tableListStr = tableListStr.substr (0,tableListStr.size ()-1);

       // cout << tableListStr << endl;
        EH.setKey (key,tableListStr);

    }


    void createTable(string tableName, string rowsName, string rowsDefine) {


        //tablename_meta  name,age,class_int,char(20),char(20)
        string key = tableName + "_meta";
        string value = rowsName + "_" + rowsDefine;

        string re = EH.getKey (tableName+"_meta");
        if(re.compare ("null") == 0) {
            EH.setKey (key, value);
            addTableList(tableName);
        }
        else
        {
            cout << "操作失败，表已存在！"<<endl;
        }
    }

    void createRealTable(string tableName) {


        vector<string> ips;
        vector<string> frags;
        vector<string> rows;
        vector<string> rowsDefine;
        string tableinfo = EH.getKey (tableName + "_meta");

        string disResult;
        distributeFragment (tableName,disResult);

        stringTofragAndIp (disResult,frags,ips);

        stringToRowsAndRowsDefine (tableinfo,rows,rowsDefine);


        for(int i = 0 ; i < ips.size () ; i++)
            cout << ips[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < frags.size () ; i++)
            cout << frags[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < rows.size () ; i++)
            cout << rows[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < rowsDefine.size () ; i++)
            cout <<rowsDefine[i] <<" ";
        cout << endl;

        string decl;

        for(int i = 0 ; i < rows.size () ; i++)
            decl+=(rows[i]+" "+rowsDefine[i]+",");

        decl = decl.substr (0,decl.size ()-1);

        for(int i = 0 ; i < ips.size () ; i++) {

            string sql = "rcreate '" + ips[i] + "' '" + "create table " + frags[i] + " (" + decl + ")'";
            cout << sql << endl;
            string result;
            cu.resolveCommand (sql,result);
            cout << result<<endl;
        }
    }
    void createRealTableAdvance(string tableName) {


        vector<string> ips;
        vector<string> frags;
        vector<string> rows;
        vector<string> rowsDefine;
        string tableinfo = EH.getKey (tableName + "_meta");



        getTableFragmentIp(tableName,frags,ips);

        stringToRowsAndRowsDefine (tableinfo,rows,rowsDefine);


        for(int i = 0 ; i < ips.size () ; i++)
            cout << ips[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < frags.size () ; i++)
            cout << frags[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < rows.size () ; i++)
            cout << rows[i] <<" ";
        cout << endl;
        for(int i = 0 ; i < rowsDefine.size () ; i++)
            cout <<rowsDefine[i] <<" ";
        cout << endl;

        string decl;

        for(int i = 0 ; i < rows.size () ; i++)
            decl+=(rows[i]+" "+rowsDefine[i]+",");

        decl = decl.substr (0,decl.size ()-1);

        for(int i = 0 ; i < ips.size () ; i++) {

            string sql = "rcreate '" + ips[i] + "' '" + "create table " + frags[i] + " (" + decl + ")'";
            cout << sql << endl;
            string result;
            cu.resolveCommand (sql,result);
            cout << result<<endl;
        }
    }

    void getTableMetaInfo(string tableName,vector<string> &rows,vector<string> &rowsDefine)
    {
        string tableinfo = EH.getKey (tableName + "_meta");
        stringToRowsAndRowsDefine (tableinfo,rows,rowsDefine);
    }

    void getTableFragmentIp(string tableName,vector<string> &frags,vector<string> &ips)
    {
        string fragIpDis = EH.getKey(tableName+"_fragIpDistribute");
        stringTofragAndIp (fragIpDis,frags,ips);
    }
    void getTableFragmentDefine(string tableName,string &type,vector<conditionDefine> &vcd)
    {
        string fragDef = EH.getKey(tableName + "_fragmentDefine");
        stringTofragDefine (fragDef,type,vcd);
    }

    void getTableFragmentDefineAdvance(string tableName,vector<string> &type,vector<vector<conditionDefine>> &vcd)
    {
        string fragDef = EH.getKey(tableName + "_fragmentDefineAdvance");
        stringTofragDefineAdvance (fragDef,type,vcd);
    }


    void getTableInfo(string tableName)
    {
        string tableinfo = EH.getKey (tableName + "_meta");
        string fragDef = EH.getKey(tableName + "_fragmentDefine");
        string fragIpDis = EH.getKey(tableName+"_fragIpDistribute");
        if(fragDef.compare ("null") == 0)
            fragDef = EH.getKey(tableName + "_fragmentDefineAdvance");


        cout <<"------元数据------"<<endl;
        cout <<"表元数据:"<<tableinfo <<endl;
        cout <<"划分定义:"<<fragDef << endl;
        cout <<"划分子表和ip:"<<fragIpDis << endl;


        vector<string> ips;
        vector<string> frags;
        vector<string> rows;
        vector<string> rowsDefine;

        if(fragIpDis.size () > 0 &&fragIpDis.compare ("null") != 0)
        stringTofragAndIp (fragIpDis,frags,ips);
        if(tableinfo.size () > 0 &&tableinfo.compare ("null") != 0)
        stringToRowsAndRowsDefine (tableinfo,rows,rowsDefine);

        cout << "------结构化------"<<endl;

        if(frags.size () > 0) {
            cout << "子表划分及ip:" << endl;
            for (int i = 0; i < frags.size (); i++)
                cout << frags[i] << " " << ips[i] << endl;
        }
        if(rows.size () > 0) {
            cout << "表字段和类型:" << endl;
            for (int i = 0; i < rows.size (); i++)
                cout << rows[i] << " " << rowsDefine[i] << endl;
        }
        vector<conditionDefine> vcd;
        string type;
        if(fragDef.size () > 0&&fragDef.compare ("null")!=0)
        stringTofragDefine (fragDef,type,vcd);
        cout << "划分类型:"<< type<<endl;
        cout << "划分定义:" << endl;
        for(int i = 0 ; i < vcd.size () ; i++)
        {
            cout << vcd[i].left <<" "<< vcd[i].op <<" "<<vcd[i].right << endl;
        }

    }

    void dropTable(string tableName)
    {
        vector<string> ips;
        vector<string> frags;
        vector<string> rows;
        vector<string> rowsDefine;
        string tableinfo = EH.getKey (tableName + "_meta");


        if(tableinfo.compare ("null") == 0) {
            cout << "表不存在！"<<endl;
            return;
        }

        string disResult;
      //  distributeFragment (tableName,disResult);

        disResult = EH.getKey (tableName+"_fragIpDistribute");
        stringTofragAndIp (disResult,frags,ips);

        stringToRowsAndRowsDefine (tableinfo,rows,rowsDefine);


        deleteTableFromList (tableName);

        for(int i = 0 ; i < ips.size () ; i++) {

            string sql = "rdrop '" + ips[i] + "' '" + "drop table " + frags[i]+"'";
            cout << sql << endl;
            string result;
            cu.resolveCommand (sql,result);
            cout << result << endl;

        }

        EH.deleteKey (tableName+"_meta");
        EH.deleteKey (tableName+"_fragmentDefine");
        EH.deleteKey (tableName+"_fragIpDistribute");
        EH.deleteKey (tableName+"_fragmentDefineAdvance");
    }


    int defineFragment(string tableName, int type, vector<conditionDefine> fdefine) {

        //tablename_fragmentDefine 1*between,age,12&13_between,age,12&13_between,age,12&13_between,age,12&13
        fragmentDefine fd (tableName, type, fdefine);
        int re = fd.fragmentDefineCheck ();
        if (re == -1)
            return -1;

        string key = tableName + "_fragmentDefine";
        string value;
        value=to_string(type)+"*";
        for (int i = 0; i < fdefine.size (); i++) {
            string singleDefine = fdefine[i].op + "," + fdefine[i].left + "," + fdefine[i].right;
            value = value + singleDefine + "_";
        }

        value = value.substr (0, value.size () - 1);
        EH.setKey (key, value);


        return 1;
    }

    int defineFragmentAdvance(string tableName,string fragIp,string fragDefine)
    {

        string key = tableName + "_fragmentDefineAdvance";
        EH.setKey (key,fragDefine);

        string fikey = tableName+"_fragIpDistribute";
        EH.setKey (fikey,fragIp);


        vector<string> type;
        vector<vector<conditionDefine>> vcd;

        getTableFragmentDefineAdvance (tableName,type,vcd);

        createRealTableAdvance(tableName);

    }


    int distributeFragment(string tableName,string &result)
    {


        //tablename_fragIpDistribute 子表名,ip*子表名,ip*子表名,ip*子表名,ip*子表名,ip*子表名,ip
        string key = tableName + "_fragmentDefine";


        string fragmentDefine = EH.getKey (key);
        string ipset = EH.getKey ("ipSet");

        if(fragmentDefine.size () == 0) {
            cout << "未定义划分！" << endl;
            return -1;
        }
        if(ipset.size () == 0) {
            cout << "无可用节点！" << endl;
            return -1;
        }
        vector<string> ips;
        vector<string> fdefines;
        SplitString (ipset,ips,",");
        SplitString (fragmentDefine,fdefines,"_");


        int j = 0;
        vector<string> frag_ip;

        for(int i = 0 ; i < fdefines.size () ; i++)
        {


            string fi = tableName+to_string (i)+","+ips[j];
            frag_ip.push_back (fi);
            j++;
            if(j == ips.size ())
                j = 0;

        }

        string fragiptotal;
        for(int i = 0 ; i < frag_ip.size (); i++)
        {
            fragiptotal+=frag_ip[i]+"*";
        }
        fragiptotal = fragiptotal.substr (0,fragiptotal.size ()-1);


        string fikey = tableName+"_fragIpDistribute";

        EH.setKey (fikey,fragiptotal);
        result = fragiptotal;

        return 1;

    }


    int insertTableLoadFiles(string tableName,string FilePath)
    {

        vector<string> values;


        FilePath = FilePath.substr (1,FilePath.size ()-2);
        cout << "load " + FilePath+" to "+tableName<<endl;

        ifstream fin(FilePath);

        if(!fin.good ())
        {
            cout << "找不到该文件！"<<endl;
            return -1;
        }

        const int LINE_LENGTH = 1000;
        char str[LINE_LENGTH];

        vector<vector<string>> allLines;
        while (fin.getline(str, LINE_LENGTH))
        {
           // cout<< str << endl;

            vector<string> lines;
            SplitString (str,lines,"\t");

            for(int i = 0 ; i < lines.size (); i++)
                cout << lines[i]<<" ";
            cout << endl;

            allLines.push_back (lines);
        }





        string kkkk = tableName+"_meta";
        string re = EH.getKey (kkkk);

        if(re.compare ("null") == 0) {
            cout << "表不存在！"<<endl;
            return -1;
        }

        int mode = 0;
        string fragdis = EH.getKey (tableName+"_fragIpDistribute");
        string fragdefine = EH.getKey (tableName+"_fragmentDefine");

        if(fragdefine.compare ("null") == 0) {
            fragdefine = EH.getKey (tableName + "_fragmentDefineAdvance");
            mode = 1;
        }
        if(mode == 0) {

            vector<string> ips;
            vector<string> frags;
            stringTofragAndIp (fragdis, frags, ips);
            vector<conditionDefine> vc;
            string type;
            stringTofragDefine (fragdefine, type, vc);


            fragmentDefine fd (tableName, stoi (type), vc);

            fd.setTuple (values);

            int select = fd.check ();
            if (select == -2) {
                cout << "插入数据找不到合适的划分？？？" << endl;
                return -1;
            }
            string valueStr;
            for (int i = 0; i < values.size (); i++) {
                string v = "\"" + values[i] + "\"";
                valueStr += (v + ",");
            }

            valueStr = valueStr.substr (0, valueStr.size () - 1);

            string sql = "rinsert '" + ips[select] + "'" + " " + "'" + "insert into " + frags[select] + " values(" +
                         valueStr + ")'";

            cout << sql << endl;
            string result;
            cu.resolveCommand (sql, result);
            cout << result << endl;
        } else {
            vector<string> ips;
            vector<string> frags;
            stringTofragAndIp (fragdis, frags, ips);
            vector<vector<conditionDefine>> vc;
            vector<string> type;
            stringTofragDefineAdvance (fragdefine, type, vc);



            for(int i = 0 ; i < vc.size () ; i++)
            {
                for(int j = 0 ; j < vc[i].size () ; j++)
                cout << vc[i][j].left<<"***"<<vc[i][j].op<<"***"<<vc[i][j].right<<endl;
            }


            fragmentDefine fd (tableName);

            for (int xx = 0; xx < allLines.size (); xx++) {
                int select;
                vector<int> selects;
                for (int i = 0; i < type.size (); i++) {


                    fd.setTypeConditions (stoi (type[i]), vc[i]);
                    cout << "!!!" << type[i] << endl;
                    for (int j = 0; j < vc[i].size (); j++)
                        cout << vc[i][j].op << " " << vc[i][j].left << " " << vc[i][j].right << endl;

                    fd.setTuple (allLines[xx]);

                    vector<int> selectors;

                    fd.checkAll (selectors);

                    for(int n = 0 ; n < selectors.size () ; n++)
                        selects.push_back (selectors[n]);

                }

                for(int i = 0 ; i < selects.size () ; i++)
                    cout << selects[i]<<endl;
              //  set<int> setVec (selects.begin (), selects.end ());
              //  selects.assign (setVec.begin (), setVec.end ());

                map<int,int> counter;
                for(int i = 0 ; i < selects.size () ; i++)
                {
                    if(counter.count (selects[i]) > 0)
                    {
                        counter[selects[i]]++;
                    } else
                        counter[selects[i]] = 1;
                }

                map<int,int>::iterator it;

                it = counter.begin();

                vector<int> selectReal;
                while(it != counter.end())
                {
                    if(it->second == type.size ())
                        selectReal.push_back (it->first);
                    it ++;
                    cout << "["<<it->first<< "|"<< it->second<<"]";
                }


                if(selectReal.size () == 0)
                {
                    cout << "没有找到合适的划分！"<<endl;
                    return -1;
                }
                else if(selectReal.size () > 1)
                {
                    cout <<"满足合适的划分多于一个？是不是划分有问题"<<endl;
                    return -1;
                }

                select = selectReal[0];
                cout << "选择的划分是"<<select << endl;

                values = allLines[xx];
                string valueStr;
                for (int i = 0; i < values.size (); i++) {
                    string v = "\"" + values[i] + "\"";
                    valueStr += (v + ",");
                }
                valueStr = valueStr.substr (0, valueStr.size () - 1);

                string sql = "rinsert '" + ips[select] + "'" + " " + "'" + "insert into " + frags[select] + " values(" +
                             valueStr + ")'";

                cout << sql << endl;
                string result;
                cu.resolveCommand (sql, result);
                cout << result << endl;
            }

        }


    }


    int insertTable(string tableName,vector<string> values)
    {
        string kkkk = tableName+"_meta";
        string re = EH.getKey (kkkk);

        if(re.compare ("null") == 0) {
            cout << "表不存在！"<<endl;
            return -1;
        }

        int mode = 0;
        string fragdis = EH.getKey (tableName+"_fragIpDistribute");
        string fragdefine = EH.getKey (tableName+"_fragmentDefine");

        if(fragdefine.compare ("null") == 0) {
            fragdefine = EH.getKey (tableName + "_fragmentDefineAdvance");
            mode = 1;
        }
        if(mode == 0) {

            vector<string> ips;
            vector<string> frags;
            stringTofragAndIp (fragdis, frags, ips);
            vector<conditionDefine> vc;
            string type;
            stringTofragDefine (fragdefine, type, vc);


            fragmentDefine fd (tableName, stoi (type), vc);

            fd.setTuple (values);

            int select = fd.check ();
            if (select == -2) {
                cout << "插入数据找不到合适的划分！" << endl;
                return -1;
            }
            string valueStr;
            for (int i = 0; i < values.size (); i++) {
                string v = "\"" + values[i] + "\"";
                valueStr += (v + ",");
            }

            valueStr = valueStr.substr (0, valueStr.size () - 1);

            string sql = "rinsert '" + ips[select] + "'" + " " + "'" + "insert into " + frags[select] + " values(" +
                         valueStr + ")'";

            cout << sql << endl;
            string result;
            cu.resolveCommand (sql, result);
            cout << result << endl;
        } else
        {
            vector<string> ips;
            vector<string> frags;
            stringTofragAndIp (fragdis, frags, ips);
            vector<vector<conditionDefine>> vc;
            vector<string> type;
            stringTofragDefineAdvance (fragdefine, type, vc);

            fragmentDefine fd (tableName);
            int select;
            vector<int> selects;
            for (int i = 0; i < type.size (); i++) {


                fd.setTypeConditions (stoi (type[i]), vc[i]);
                cout << "!!!" << type[i] << endl;
                for (int j = 0; j < vc[i].size (); j++)
                    cout << vc[i][j].op << " " << vc[i][j].left << " " << vc[i][j].right << endl;

                fd.setTuple (values);

                vector<int> selectors;

                fd.checkAll (selectors);

                for(int n = 0 ; n < selectors.size () ; n++)
                    selects.push_back (selectors[n]);

            }

            for(int i = 0 ; i < selects.size () ; i++)
                cout << selects[i]<<endl;
            //  set<int> setVec (selects.begin (), selects.end ());
            //  selects.assign (setVec.begin (), setVec.end ());

            map<int,int> counter;
            for(int i = 0 ; i < selects.size () ; i++)
            {
                if(counter.count (selects[i]) > 0)
                {
                    counter[selects[i]]++;
                } else
                    counter[selects[i]] = 1;
            }

            map<int,int>::iterator it;

            it = counter.begin();

            vector<int> selectReal;
            while(it != counter.end())
            {
                if(it->second == type.size ())
                    selectReal.push_back (it->first);
                it ++;
                cout << "["<<it->first<< "|"<< it->second<<"]";
            }


            if(selectReal.size () == 0)
            {
                cout << "没有找到合适的划分！"<<endl;
                return -1;
            }
            else if(selectReal.size () > 1)
            {
                cout <<"满足合适的划分多于一个？是不是划分有问题"<<endl;
                return -1;
            }

            select = selectReal[0];
            cout << "选择的划分是"<<select << endl;

          ///  values = allLines[xx];
            string valueStr;
            for (int i = 0; i < values.size (); i++) {
                string v = "\"" + values[i] + "\"";
                valueStr += (v + ",");
            }
            valueStr = valueStr.substr (0, valueStr.size () - 1);

            string sql = "rinsert '" + ips[select] + "'" + " " + "'" + "insert into " + frags[select] + " values(" +
                         valueStr + ")'";

            cout << sql << endl;
            string result;
            cu.resolveCommand (sql, result);
            cout << result << endl;

        }




    }

    string replaceALL(const char* src, const string& target,const string& subs)
    {
        string tmp(src);
        string::size_type pos =tmp.find(target),targetSize =target.size(),resSize =subs.size();
        while(pos!=string::npos)//found
        {
            tmp.replace(pos,targetSize,subs);
            pos =tmp.find(target, pos + resSize);
        }
        return tmp;
    }

    int distributeDelete(string tableName,string sql)
    {
        string kkkk = tableName+"_meta";
        string re = EH.getKey (kkkk);

        if(re.compare ("null") == 0) {
            cout << "表不存在！"<<endl;
            return -1;
        }

        string fragdis = EH.getKey (tableName+"_fragIpDistribute");
        string fragdefine = EH.getKey (tableName+"_fragmentDefineAdvance");

        vector<string> ips;
        vector<string> frags;
        stringTofragAndIp (fragdis, frags, ips);
        vector<vector<conditionDefine>> vc;
        vector<string> type;
        stringTofragDefineAdvance (fragdefine, type, vc);


        vector<string> disSqlStr;

        for(int i = 0 ; i < frags.size () ; i++)
        {
            string temp = sql;

            string tt = replaceALL(temp.c_str (),tableName,frags[i]);
            disSqlStr.push_back (tt);
        }

        for(int i = 0 ; i < disSqlStr.size () ; i++)
            cout << disSqlStr[i] << endl;


        vector<string> result;
        for(int i = 0 ; i < ips.size () ; i++)
        {
            string s = "rdelete '"+ips[i]+"' '"+disSqlStr[i]+"'";
            result.push_back (s);
        }


        commandUI cu;
        for(int i = 0 ; i < result.size () ; i++) {
            cout << result[i] << endl;
            cu.resolveCommand (result[i]);
        }



    }




    int distributeupdate(string tableName,string sql)
    {
        string kkkk = tableName+"_meta";
        string re = EH.getKey (kkkk);

        if(re.compare ("null") == 0) {
            cout << "表不存在！"<<endl;
            return -1;
        }

        string fragdis = EH.getKey (tableName+"_fragIpDistribute");
        string fragdefine = EH.getKey (tableName+"_fragmentDefineAdvance");

        vector<string> ips;
        vector<string> frags;
        stringTofragAndIp (fragdis, frags, ips);
        vector<vector<conditionDefine>> vc;
        vector<string> type;
        stringTofragDefineAdvance (fragdefine, type, vc);


        vector<string> disSqlStr;

        for(int i = 0 ; i < frags.size () ; i++)
        {
            string temp = sql;

            string tt = replaceALL(temp.c_str (),tableName,frags[i]);
            disSqlStr.push_back (tt);
        }

        for(int i = 0 ; i < disSqlStr.size () ; i++)
            cout << disSqlStr[i] << endl;


        vector<string> result;
        for(int i = 0 ; i < ips.size () ; i++)
        {
            string s = "rupdate '"+ips[i]+"' '"+disSqlStr[i]+"'";
            result.push_back (s);
        }


        commandUI cu;
        for(int i = 0 ; i < result.size () ; i++) {
            cout << result[i] << endl;
            cu.resolveCommand (result[i]);
        }


    }

    void ipUpload()
    {


        string key = "ipSet";
        string ipset = EH.getKey (key);
        cout << ipset<<endl;
        if(ipset.compare ("null") == 0)
        {
            EH.setKey (key,config::localIpAddr);
        }
        else{
            vector<string> ipsets;
            SplitString (ipset,ipsets,",");

            for(int i = 0 ; i < ipsets.size () ; i++)
            {
                if(ipsets[i].compare (config::localIpAddr) == 0)
                    return;
            }

            ipset+=","+config::localIpAddr;
            EH.setKey (key,ipset);


        }



        ipset = EH.getKey (key);
        cout << ipset<<endl;


    }

};