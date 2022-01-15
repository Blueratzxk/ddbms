//
// Created by root on 21-11-27.
//

#ifndef FFER_SQLPARSER_PARSER_INTERFACE_H
#define FFER_SQLPARSER_PARSER_INTERFACE_H

#endif //FFER_SQLPARSER_PARSER_INTERFACE_H

#include "parser.h"
#include "sqlParser.h"
#include <vector>


class sqlp_p_interface
{

    vector<string> sqlSemaResult;


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

        while(1) {
            indexfront = indexback;
            indexfront = s.find ('\'', indexfront);

            indexback = s.find ('\'', indexfront+1);


            if(indexback >= s.size ()||indexfront >= s.size ())
                break;

            v.push_back (s.substr (indexfront+1, indexback-indexfront-1));
        }
    }
    void SplitString3(const string& s, vector<string>& v)
    {
        int indexfront = 0,indexback = 0;


        while(1) {
            indexfront = indexback;
            indexfront = s.find ('[', indexfront);

            indexback = s.find (']', indexfront+1);


            if(indexback >= s.size ()||indexfront >= s.size ())
                break;

            v.push_back (s.substr (indexfront+1, indexback-indexfront-1));
        }
    }

    string cleanStr(string &str)
    {

        if (str.size() > 0 && str.front() == '\'')
        {
            str = str.substr(1,str.size() - 1);
        }
        if (str.size() > 0 && str.back() == '\'')
        {
            str = str.substr(0, str.size() - 1);
        }
        return str;
    }


    void SplitStringByCha(string s,vector<string> &v)
    {
        int indexfront = 0;
        int indexback = 0;
        while (1) {
            indexfront = s.find('[', indexfront);

            indexback = s.find(']', indexfront + 1);


            if (indexback >= s.size() || indexfront >= s.size())
                break;

            v.push_back(s.substr(indexfront + 1, indexback - indexfront - 1));

            indexfront = indexback + 1;
        }
    }


    void SplitStringByCha2(string s, vector<string> &v)
    {

        //cout << s << endl;
        int indexfront = 0;
        int indexback = 0;
        while (1) {
            indexfront = s.find('\'', indexfront);

            indexback = s.find('\'', indexfront + 1);


            if (indexback >= s.size() || indexfront >= s.size())
                break;

            v.push_back(s.substr(indexfront + 1, indexback - indexfront - 1));

            indexfront = indexback + 1;
        }
    }

public:
    sqlp_p_interface(vector<string> output)
    {
        sqlSemaResult = output;
    }


    int resolveSqlResult()
    {
        if(sqlSemaResult.size () != 0) {
            string sqlType = sqlSemaResult[0];
            vector<string> vs;
            SplitString (sqlType,vs," ");
            if(vs[1].compare ("select") == 0)
                return resolveSelect ();
            else if(vs[1].compare ("create") == 0)
                return resolveCreate ();
            else if(vs[1].compare ("insert") == 0)
                return resolveInsert ();
            else if(vs[1].compare ("delete") == 0)
                return resolveDelete ();
            else if(vs[1].compare ("drop") == 0)
                return resolveDrop ();
            else if(vs[1].compare ("fragment") == 0)
                return resolveFragment ();
            else if(vs[1].compare ("manualFrag") == 0)
                return resolveManualFragment ();
            else if(vs[1].compare ("desc") == 0)
                return resolveDesc ();
            else if(vs[1].compare ("update") == 0)
                resolveUpdate();
            else if(vs[1].compare ("show") == 0)
                return resolveShow ();
            else if(vs[1].compare ("addip") == 0)
                return resolveAddIp ();
            else if(vs[1].compare ("loadfile") == 0)
                return resolveLoadFile ();
            else {
                cout << "当前未支持的操作！"<<endl;
                return -1;
            }
        } else
            return -1;



    }

    int resolveDelete ()
    {

        vector<string> tablenames;
        SplitString (sqlSemaResult[1],tablenames," ");



        string tableRawCons = sqlSemaResult[2];

        vector<string> conditions;
        vector<string> trueConditions;
        SplitString2 (tableRawCons,conditions);
        SplitString3 (tableRawCons,trueConditions);


        vector<conditionDefine> normalConditions;
        vector<conditionDefine> joinConditions;


        for(int i = 0 ; i < trueConditions.size () ;i++) {

            vector<string> cons;

            SplitString (trueConditions[i], cons, " ");
            if (cons[0].compare ("compare") == 0) {
                conditionDefine cd (cons[1], cons[2], cons[3]);
                normalConditions.push_back (cd);
            } else {
                conditionDefine cd (cons[1], cons[2], cons[3]);
                joinConditions.push_back (cd);
            }

        }

        string sqlstr;
        sqlstr+="delete from "+tablenames[1];

        if(trueConditions.size () > 0)
        {
            sqlstr+=" where ";
            for(int i = 0 ; i < normalConditions.size () ; i++) {
                sqlstr += (normalConditions[i].left + " " + normalConditions[i].op + " " + normalConditions[i].right+" and ");
            }
            for(int i = 0 ; i < joinConditions.size () ;i++)
            {
                sqlstr += (joinConditions[i].left+" "+joinConditions[i].op+" "+joinConditions[i].right+" and ");
            }
            sqlstr = sqlstr.substr (0,sqlstr.size ()-5);
        }


        sqlstr+=";";
        cout << sqlstr<<endl;

        work w;
        w.distributeDelete (tablenames[1],sqlstr);





    }


    int resolveUpdate()
    {
        vector<string> parseResult = sqlSemaResult;

        vector<string> tableName;
        SplitString(parseResult[1], tableName, " ");
        string tablename = tableName[1];

        vector<string> assign;
        SplitString(parseResult[2], assign, " ");


        vector<string> assigns;
        SplitString(assign[1], assigns, ",");

        vector<string> conditions;
        SplitStringByCha(parseResult[3], conditions);


        vector<conditionDefine> conditionUse;

        for (int i = 0; i < conditions.size(); i++) {
            vector<string> condition;

            vector<string> strs;

            SplitStringByCha2(conditions[i], strs);

            //cout << conditions[i] << "$$$$$$" << endl;

            SplitString(conditions[i], condition, " ");


            vector<string>::iterator k = condition.begin();
            condition.erase(k);


            if (strs.size() > 0)
                condition[2] = "'" + strs[0] + "'";

            conditionDefine cd(condition[0], cleanStr(condition[1]), cleanStr(condition[2]));
            //cout << condition[0] << " " << condition[1] << " " << condition[2] << endl;
            conditionUse.push_back(cd);
        }

        for(int i = 0 ; i < assigns.size () ; i++) {

            vector<string> ass;
            SplitString (assigns[i],ass,"=");

            if(ass[1].find ("'")!=ass[1].npos)
            {
                ass[1] = ass[1].substr (1,ass[1].size ()-2);
                ass[1]="\""+ass[1]+"\"";
            }
            assigns[i] = ass[0]+"="+ass[1];

            cout << assigns[i] << endl;
        }

        for(int i = 0 ; i < conditions.size () ; i++)
        {

            cout << conditionUse[i].left+conditionUse[i].op+conditionUse[i].right<<endl;

        }



        string sqlstr;
        sqlstr+="update "+tablename+" set ";


        string set;
        for(int i = 0 ; i < assigns.size () ; i++)
        {
            set+=assigns[i] +",";
        }
        set = set.substr (0,set.size ()-1);

        sqlstr+=set;

        if(conditionUse.size () > 0)
        {
            sqlstr+=" where ";
            for(int i = 0 ; i < conditionUse.size () ; i++) {
                sqlstr += (conditionUse[i].left + " " + conditionUse[i].op + " " + conditionUse[i].right+" and ");
            }

            sqlstr = sqlstr.substr (0,sqlstr.size ()-5);
        }


        sqlstr+=";";
        cout << sqlstr<<endl;
        work w;
        w.distributeupdate (tablename,sqlstr);

    }

    int resolveShow ()
    {
        work w;
        vector<string> tables;
        w.showTables (tables);

        for(int i = 0 ; i < tables.size () ; i++)
            cout << tables[i] << endl;

        return 0;
    }
    int resolveLoadFile()
    {
        work w;

        w.insertTableLoadFiles (sqlSemaResult[1],sqlSemaResult[2]);
        return 0;
    }

    int resolveAddIp ()
    {
        work w;
        w.ipUpload ();
        return 0;
    }
    int resolveMulSelect()
    {
        string tableRawNames = sqlSemaResult[1];
        string tableRawRows = sqlSemaResult[2];
        string tableRawCons = sqlSemaResult[3];


        vector<string> tables;
        vector<string> rows;
        vector<string> conditions;
        vector<string> trueConditions;

        SplitString2 (tableRawNames,tables);
        SplitString2 (tableRawRows,rows);
        SplitString2 (tableRawCons,conditions);

        SplitString3 (tableRawCons,trueConditions);

        vector<string> tableNames;
        SplitString (tables[0],tableNames," ");
        //if(tableNames.size () < 2)
         //   return -1;


        vector<string> tablesRows;
        SplitString (rows[0],tablesRows,"&");

        for(int i = 0 ; i < tablesRows.size () ; i++)
        {
            vector<string> temp;
            SplitString (tablesRows[i],temp,"!");
            tablesRows[i] = temp[1];
        }


        vector<conditionDefine> normalConditions;
        vector<conditionDefine> joinConditions;

        for(int i = 0 ; i < trueConditions.size () ;i++) {

            vector<string> cons;

            SplitString (trueConditions[i], cons, " ");
            if (cons[0].compare ("compare") == 0) {
                conditionDefine cd (cons[1], cons[2], cons[3]);
                normalConditions.push_back (cd);
            } else {
                conditionDefine cd (cons[1], cons[2], cons[3]);
                joinConditions.push_back (cd);
            }

        }


        for(int i = 0 ; i < tableNames.size (); i++)
            cout << tableNames[i] << endl;
        for(int i = 0 ; i < tablesRows.size (); i++)
            cout << tablesRows[i] << endl;
        for(int i = 0 ; i < normalConditions.size (); i++)
            cout << normalConditions[i].left << normalConditions[i].op<<normalConditions[i].right << endl;
        for(int i = 0 ; i <  joinConditions.size (); i++)
            cout << joinConditions[i].left << joinConditions[i].op << joinConditions[i].right << endl;

        distributeParserTree d;

        d.setJoinTables (tableNames);
        // d.setJoinTables ({"student_test"});
        // d.setProjRows ({"student_test.name","student_test.id","student_test.age","class.teacher","class.name"});
        d.setProjRows (tablesRows);
        //if(joinConditions.size () == 0)
      //  {
       //     cout << "必须输入连接条件！！！"<<endl;
      //      return 1;
      //  }

        d.setJoinCondtions (joinConditions);
        d.setSelecttionConditions (normalConditions);


      //  d.mulTablejoinSelectProjCount ();
        d.mulTablejoinSelectProj_Single ();




        return 1;
    }


    int resolveSelect()
    {

        if(resolveMulSelect() == 1)
            return 0;
        string tableNameStr = sqlSemaResult[1];

        string tableName;
        vector<string> temp;
        SplitString (tableNameStr,temp," ");
        tableName = temp[1].substr (1,temp[1].size () - 2 );

        temp.clear ();


        vector<string> temp2;

        SplitString (temp[1],temp2,"&");


       // for(int i = 0 ; i < temp2.size () ; i++)
        //    cout << temp2[i] << " ";
       // cout << endl;
        vector<string> rows;
        for(int i = 0 ; i < temp2.size () ; i++)
        {
            vector<string> vs;
            SplitString (temp2[i],vs,"_");
            if(vs[0].compare ("normal") == 0)
            {
                rows.push_back (vs[1]);
            } else
            {
                cout << "不支持的操作！"<<endl;
                return -1;
            }

        }
       cout << tableName <<endl;
       // for(int i = 0 ; i < rows.size () ; i++)
        //    cout << rows[i] << " ";
       // cout <<endl;

       // distributeParserTree dt(tableName);
       // dt.proj_single_table (rows,tableName);

        return 0;
    }



    int resolveCreate()
    {
        string tableName;
        string rows;
        string rowsDefine;

        string tableNameStr = sqlSemaResult[1];
        string rowStr = sqlSemaResult[2];
        string rowDefineStr = sqlSemaResult[3];

        vector<string> vs;
        SplitString (tableNameStr,vs," ");
        tableName = vs[1];
        vs.clear ();
        SplitString (rowStr,vs," ");
        rows = vs[1];
        vs.clear ();
        SplitString (rowDefineStr,vs," ");
        rowsDefine = vs[1];


        work w;
        w.createTable (tableName,rows,rowsDefine);

        //cout << "w.createTable(\""<<tableName<<"\",\""<<rows<<"\",\""<<rowsDefine<<"\");";


        return 0;
    }

    int resolveManualFragment()
    {


        work w;

        string tableName = sqlSemaResult[1];
        string fragsip = sqlSemaResult[2];
        string fragsdefines = sqlSemaResult[3];

        w.defineFragmentAdvance (tableName,fragsip,fragsdefines);

        return 0;
    }

    int resolveFragment()
    {
        string tableName;
        string fragmethod;
        vector<string> frags;

        string tableNameStr = sqlSemaResult[1];
        string fragmethodStr = sqlSemaResult[3];
        string fragmentsStr = sqlSemaResult[4];




        vector<string> vs;
        SplitString (tableNameStr,vs," ");
        tableName = vs[1];
        vs.clear ();

        SplitString (fragmethodStr,vs," ");
        fragmethod = vs[1];
        vs.clear ();


        SplitString (fragmentsStr,vs," ");

        vector<string> vs2;
        SplitString (vs[1],vs2,"_");
        vs = vs2;
        vector<vector<string>> vvs;

        for(int i = 0 ; i < vs.size (); i++)
        {
            vector<string> vv;
            SplitString (vs[i],vv,",");
            vvs.push_back (vv);
        }


        cout << tableName<<endl;
        cout << fragmethod << endl;

        vector<conditionDefine> vcd;
        for(int i = 0 ; i < vvs.size () ; i++) {
            conditionDefine cd(vvs[i][0],vvs[i][1],vvs[i][2]);
            cout <<vvs[i][0] <<" "<< vvs[i][1] <<" "<< vvs[i][2]<<" " << endl;
            vcd.push_back (cd);
        }


        int select;
        if(fragmethod.compare ("range") == 0)
            select = 1;
        else
            select = 2;

       work w;
        w.defineFragment (tableName,select,vcd);
        w.createRealTable (tableName);
        //cout << "w.createTable(\""<<tableName<<"\",\""<<rows<<"\",\""<<rowsDefine<<"\");";


        return 0;
    }


    void modifyValueStr(vector<string> &vs)
    {
        for (int i = 0; i < vs.size(); i++)
        {
            int num = count(vs[i].begin(), vs[i].end(), '\'');
            if (num % 2 != 0)
            {
                int in = i;
                i++;
                while (i < vs.size() && count(vs[i].begin(), vs[i].end(), '\'') %2 != 1)
                {
                    vs[in] += "_" + vs[i];
                    vs.erase(vs.begin() + i);
                }
                vs[in] += '_' + vs[i];
                vs.erase(vs.begin() + i);
            }
        }

    }
    int resolveInsert()
    {
       // work w;
        string tableName;
        vector<string> values;


        string tableNameStr = sqlSemaResult[1];
        string rowListStr = sqlSemaResult[2];
        string valuesStr = sqlSemaResult[3];

        vector<string> vs;
        SplitString (tableNameStr,vs," ");
        tableName = vs[1];



        int index = valuesStr.find ("values");

        string realValueStr = valuesStr.substr (index+7,valuesStr.size ());



        vector<string> vs3;
        SplitString (realValueStr,vs3,"_");

        modifyValueStr(vs3);



        vector<vector<string>> vvs;
        for(int i = 0 ; i < vs3.size () ; i++)
        {
            cout << vs3[i]<<endl;
            vector<string> vv;
            SplitString (vs3[i],vv,",");
            vvs.push_back (vv);
        }


        for(int j = 0 ; j < vvs.size () ; j++) {

            for(int i = 0 ; i < vvs[j].size () ; i++){
                if(vvs[j][i][0] == '\'')
                    vvs[j][i] = vvs[j][i].substr (1,vvs[j][i].size () - 2);

            }

            for(int  i = 0 ; i < vvs[j].size () ; i++)
                cout << vvs[j][i]<<" ";
            cout << endl;
            work w;
            w.insertTable (tableName,vvs[j]);
        }
        //work w;
        //w.insertTable (tableName,values);
        return 0;
    }
    int resolveDrop()
    {

        string tableName;

        vector<string> vs;
        SplitString (sqlSemaResult[1],vs," ");

        tableName = vs[1];



        work w;
        w.dropTable (tableName);

        return 0;
    }

    int resolveDesc ()
    {
        string tableName;

        vector<string> vs;
        SplitString (sqlSemaResult[1],vs," ");

        tableName = vs[1];

        work w;
        w.getTableInfo (tableName);

    }


};
void *UI(void * arg)
{

    commandUI cu;

    while(1)
    {
        start2:
        string cmd;
        cout << "sql>";
        getline (cin,cmd);
        if(cmd.size () == 0)
            goto start2;
        if(cmd.compare("shell") == 0)
        {
            goto start;
        }
        parser p(cmd);
        vector<string> result;
        p.parse (result);

        for(int i = 0 ; i < result.size () ; i++)
            cout << result[i] << " ";
        cout << endl;

        sqlp_p_interface spi(result);
        spi.resolveSqlResult ();

        usleep(10000);
    }

    while(1)
    {
        start:
        string cmd;
        cout << "shell>";
        getline (cin,cmd);
        if(cmd.size () == 0)
            goto start;
        if(cmd.compare("exit") == 0)
        {
            goto start2;
        }
        usleep(10000);
        cu.resolveCommand (cmd);
    }



}
