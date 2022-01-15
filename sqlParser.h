// ddb.cpp : 定义控制台应用程序的入口点。
//

#include <iostream>
#include <map>
#include <list>
#include<string>
#include<vector>
using namespace std;



class reserve
{
private:
    map<string, int> reserve_word;
public:
    reserve()
    {
        reserve_word["select"] = 0;
        reserve_word["from"] = 1;
        reserve_word["where"] = 2;
        reserve_word["update"] = 3;
        reserve_word["delete"] = 4;
        reserve_word["and"] = 5;
        reserve_word["or"] = 6;
        reserve_word["between"] = 7;
        reserve_word["count"] = 8;
        reserve_word["sum"] = 9;
        reserve_word["avg"] = 10;

        reserve_word["create"] = 11;
        reserve_word["insert"] = 12;
        reserve_word["primaryKey"] = 13;
        reserve_word["foreignKey"] = 14;
        reserve_word["unique"] = 15;
        reserve_word["table"] = 16;
        reserve_word["notNull"] = 17;
        reserve_word["references"] = 18;



        reserve_word["int"] = 4;
        reserve_word["tinyint"] = 1;
        reserve_word["smallint"] = 2;
        reserve_word["float"] = 4;
        reserve_word["double"] = 8;
        reserve_word["char"] = 1;
        reserve_word["varchar"] = 106;

    }
    bool is_reserved(string in)
    {

        if (reserve_word.count(in) > 0)
            return true;
        else
            return false;
    }
    int getReservedValue(string in)
    {
        if (is_reserved(in))
            return reserve_word[in];
        else
            return -1;
    }

}reserve_words;


//--------------------------------------------------------LEXER-----------------------------------------------------------------------//

class token
{
public:
    string tokenName;
    int tokenClass;
    token(string tokenName, int Class) { this->tokenName = tokenName; this->tokenClass = Class; }
    token(){}

};

enum token_s
{
    t_Reserve, t_String, t_Number, t_Character, t_Word,t_End
};
class lexer
{
private:
    list<token> tokens;
    int index;


    int getspace(const string &str, int i)
    {
        //cout << "space!";
        if (i >= str.length())
            return i;

        if (str[i] == ' ')
        {
            i++;
            //return i;
        }


        return i;
    }

    int getword(const string &str, int i)
    {
        //cout << "word!";
        if (i >= str.length())
            return i;

        int down = i;
        if (str[i] >= 'a'&&str[i] <= 'z' || str[i] >= 'A'&&str[i] <= 'Z'|| str[i]=='_') {

            while ((str[i] >= 'a'&&str[i] <= 'z' || str[i] >= 'A'&&str[i] <= 'Z' || str[i] == '_' || str[i] >= '0'&&str[i] <= '9') && i < str.length())
                i++;
            token tok(str.substr(down, i - down), t_Word);
            if (reserve_words.is_reserved(tok.tokenName))
                tok.tokenClass = t_Reserve;
            this->tokens.push_back(tok);
            return i;
        }
        else
            return i;

    }
    int getnumber(const string &str, int i)
    {
        //cout << "number!";
        if (i >= str.length())
            return i;
        int down = i;
        if (str[i] >= '0'&&str[i] <= '9') {
            while (str[i] >= '0'&&str[i] <= '9' && i < str.length())
                i++;
            token tok(str.substr(down, i - down), t_Number);
            this->tokens.push_back(tok);
            return i;
        }
        else
            return i;
    }
    int getstr(const string &str, int i)
    {
        //cout << "str!";
        if (i >= str.length())
            return i;
        int down = i;

        if (str[i] != '\'')
            return i;
        i++;
        while (str[i] != '\'' && i < str.length())
        {
            i++;
        }
        i++;
        token tok(str.substr(down, i - down), t_String);
        this->tokens.push_back(tok);
        return i;
    }

    int getother(const string &str, int i)
    {
        //cout << "other!";
        if (i >= str.length())
            return i;
        int down = i;
        if (str[i] == '*' || str[i] == '(' || str[i] == ')' || str[i] == ';' || str[i] == '='|| str[i] == '+' || str[i] == '-'|| str[i] == ',' || str[i] == '%' || str[i] == '@' || str[i] == '!' || str[i] == '^' || str[i] == '&' || str[i] == '\\' || str[i] == '/' || str[i] == '<' || str[i] == '>' || str[i] == ']' || str[i] == '[' || str[i] == '|' || str[i] == '?' || str[i] == '.' || str[i] == '~' || str[i] == '`' || str[i] == '#'|| str[i] == '$' || str[i] == '"'|| str[i] == ':'|| str[i] == '{'|| str[i] == '}')
        {
            if (str[i] == '>' || str[i] == '<')
            {
                if (str[i + 1] == '=')
                {
                    token tok(str.substr(down, i - down + 2), t_Character);
                    this->tokens.push_back(tok);
                    i+=2;
                    return i;
                }

            }
            if (str[i] == '!')
            {
                if (str[i + 1] == '=')
                {
                    token tok(str.substr(down, i - down + 2), t_Character);
                    this->tokens.push_back(tok);
                    i += 2;
                    return i;
                }

            }
            token tok(str.substr(down, i - down + 1), t_Character);
            this->tokens.push_back(tok);
            i++;
            return i;
        }
        else {
            return i;
        }


    }
public:
    lexer()
    {
        index = 0;
    }
    lexer(string text)
    {
        getTokensFromStr(text);
        index = 0;

    }
    int getCurrentIndex()
    {
        return this->index;
    }

    int getTokensNum()
    {
        return tokens.size() - 1;
    }
    void getTokensFromStr(const string &str)
    {

        int state = 0;
        int up = 0;
        for (int i = 0; i < str.length(); )
        {
            if (i >= str.length())
                break;
            i = getspace(str, i);
            i = getword(str, i);
            i = getnumber(str, i);
            i = getstr(str, i);
            i = getother(str, i);
        }
        token tok;
        tok.tokenName = "";
        tok.tokenClass = t_End;
        this->tokens.push_back(tok);
    }
    token lex()
    {
        list<token>::iterator iter = tokens.begin();
        for (int ix = 0; ix < index; ++ix) ++iter;
        if (index < getTokensNum())
            index++;

        return *iter;
    }
    void indexBack(int num)
    {
        if (index - num >= 0)
            this->index -= num;
        else
            index = 0;
    }
    token peekCurrentToken()
    {
        list<token>::iterator iter = tokens.begin();

        int indexhead = index;
        if (indexhead >= getTokensNum())
            indexhead = getTokensNum() - 1;

        for (int ix = 0; ix < indexhead; ++ix) ++iter;

        return *iter;
    }
    token peekahead(int n)
    {


        list<token>::iterator iter = tokens.begin();

        int indexhead = index + n - 1;
        if (indexhead >= getTokensNum())
            indexhead = getTokensNum() - 1;

        for (int ix = 0; ix < indexhead; ++ix) ++iter;

        return *iter;
    }
    void printAll()
    {
        list<token>::iterator iter;

        for (iter = tokens.begin(); iter != tokens.end(); iter++)

        {
            //std::cout << iter->tokenName << "$"  << iter->tokenClass << std::endl;
            std::cout << iter->tokenName << std::endl;

        }
    }
    void printStrWithErrorClue(int index)
    {
        list<token>::iterator iter;

        int errorBlacklength = 0;
        int in = 0;
        for (iter = tokens.begin(); iter != tokens.end(); iter++,in++)
        {
            if (in < index) {
                errorBlacklength += (iter->tokenName.length());
                errorBlacklength += 1;
            }
            std::cout << iter->tokenName << " ";
        }
        cout << endl;

        for (int i = 0; i < errorBlacklength-1; i++)
            cout << " ";
        cout << "^" << endl;

    }
};

//--------------------------------------------------------PARSER-----------------------------------------------------------------------//
//translationUnit->select_s|delete_s|insert_s|update_s|create_s
//selects->$select Rows $from ID;|$select Rows $from ID $where expression;
//delete_s->$delete $from ID $where expression;
//Rows->IDlist|count(id)|sum(id)|*
//IDlist->IDlist,id|id
//expr->exprOr
//exprOr->exprOr or exprAnd|exprAnd
//exprAnd->exprAnd and exprRow|exprRow
//exprRow -> Row = Row|Row < Row|Row > Row | Row <> Row
//Row->string|number

class result
{
private:

public:
    string resultString;
    int code;
    result() { resultString = ""; code = 0; }
};


class parser
{
private:
    lexer *lex;
public:
    parser(string in)
    {
        lex = new lexer(in);
    }
    ~parser()
    {
        free(lex);
    }

    void clueofError(int index)
    {
        cout << "语法错误！" << endl;
        lex->printStrWithErrorClue(index);
        //exit(0);
        return ;
    }


    token consumeToken()
    {
        token tok = lex->lex();
        //cout << "consume :" << tok.tokenName << endl;

        return tok;
    }
    int consumeToken(string t)
    {
        token tok = lex->lex();
        //cout << "consume :" << tok.tokenName << endl;
        if (t.compare(tok.tokenName) != 0)
        {
            //cout << "语法错误！" << tok.tokenName << "应该是:" << t<< endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        return 1;
    }

//---------------------------------------------------------------select_parser----------------------------------------//


    int parseTableIdList2(string &IdResults)
    {
        token tok = lex->peekahead(1);
        if (tok.tokenName.compare(",") == 0) {
            int re = consumeToken(",");
            if (re == -1)
                return re;


            tok = consumeToken();
            IdResults = IdResults + " " + tok.tokenName;

            if (tok.tokenClass != t_Word)
            {
                cout << "此处应该是单词！" << endl;
                clueofError(lex->getCurrentIndex() - 1);
                return -1;
            }
            re = parseTableIdList2(IdResults);
            if (re == -1)
                return re;
        }
        else if (tok.tokenName.compare(";") == 0|| tok.tokenName.compare("where") == 0)
        {
            return 0;
        }
        else {

            clueofError(lex->getCurrentIndex() - 1);
            return -1;
        }


        return 0;

    }
    int parseTableIdList(string &IdResults)
    {

        token tok = consumeToken();
        IdResults += (tok.tokenName);
        int re = parseTableIdList2(IdResults);
        if (re == -1)
            return re;

        return 0;

    }



    //PSRL->selectRowList|*
    //selectRowList->selectRowList,selectRow|selectRow
    //selectRow->word|$function(simpleRow)|word.word
    //funcParameter->word|*
    //function->sum|avg|count

    int parseFuncParameter(string &paramaterRe)
    {
        token peek = lex->peekahead(2);
        if (peek.tokenName.compare(".") == 0) {
            int re = parseExprRowRow(paramaterRe);
            if (re == -1)
                return re;
        }
        else
        {
            token tok = consumeToken();
            if (tok.tokenClass == t_Word)
            {
                paramaterRe += tok.tokenName;
            }
            else if (tok.tokenName.compare("*") == 0)
            {
                paramaterRe += tok.tokenName;
            }
            else
            {
                cout << "语法错误！" << "函数参数输入不正确！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }

        return 0;
    }
    int parseFunction(string &Re)
    {

        string functionP;
        token functionName = consumeToken();
        int re = consumeToken("(");
        if (re == -1)
        {
            //clueofError(lex->getCurrentIndex());
            return -1;
        }
        re = parseFuncParameter(functionP);
        if (re == -1)
        {
            //clueofError(lex->getCurrentIndex());
            return -1;
        }
        re = consumeToken(")");
        if (re == -1)
        {
            //clueofError(lex->getCurrentIndex());
            return -1;
        }
        Re = "[Compute " + functionName.tokenName+" "+functionP+"]";

        return 0;
    }

    int parseSelectRow(string &sRe)
    {
        string RowResult;
        token peek = lex->peekahead(1);
        if (peek.tokenClass == t_Word)
        {
            token peek = lex->peekahead(2);
            if (peek.tokenName.compare(".") == 0) {
                int re = parseExprRowRow(RowResult);
                RowResult = "normal!" + RowResult;
                if (re == -1)
                    return -1;


            }
            else {
                token tok = consumeToken();
                RowResult = "normal!" + tok.tokenName;
            }


        }
        else if (peek.tokenName.compare("sum") == 0 || peek.tokenName.compare("avg") == 0|| peek.tokenName.compare("count") == 0)
        {
            int re = parseFunction(RowResult);
            if (re == -1)
            {
                //clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        else
        {
            cout << "语法错误！" << "此处应该输入列名或函数调用！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        sRe = RowResult;
        return 0;

    }

    int parseSelectOnlyRowRow(string &sRe)
    {
        string RowResult;
        token peek = lex->peekahead(1);
        if (peek.tokenClass == t_Word)
        {
            token peek = lex->peekahead(2);
            if (peek.tokenName.compare(".") == 0) {
                int re = parseExprRowRow(RowResult);
                RowResult = "normal!" + RowResult;
                if (re == -1)
                    return -1;


            }
            else {
                cout << "语法错误！必须按照表名.列名的方式来写，不能只写列名！"<<endl;
                this->clueofError (lex->getCurrentIndex ());
               return -1;
            }


        }
        else if (peek.tokenName.compare("sum") == 0 || peek.tokenName.compare("avg") == 0|| peek.tokenName.compare("count") == 0)
        {
            int re = parseFunction(RowResult);
            if (re == -1)
            {
                //clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        else
        {
            cout << "语法错误！" << "此处应该输入列名或函数调用！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        sRe = RowResult;
        return 0;

    }


    int parseSelectRowList2(string &ResultAll)
    {
        string Re;
        token peek = lex->peekahead(1);
        if (peek.tokenName.compare(",") == 0)
        {
            int re = consumeToken(",");
            if (re == -1)
                return re;

            re = parseSelectOnlyRowRow(Re);
            if (re == -1)
                return re;
            ResultAll += "&"+Re;


            re = parseSelectRowList2(ResultAll);
            if (re == -1)
                return re;
        }
        else if(peek.tokenName.compare("from") == 0)
        {
            return 0;
        }
        else
        {
            clueofError(lex->getCurrentIndex() - 1);
            return -1;
        }
        return 0;

    }
    int parseSelectRowList(string &selectRowRe)
    {
        string ResultAll, Re;
        int re = parseSelectOnlyRowRow(Re);
        if (re == -1)
            return -1;
        ResultAll += Re;
        re = parseSelectRowList2(ResultAll);
        if (re == -1)
            return re;
        selectRowRe = ResultAll;
        return 0;
    }



    int parseRow(string &RowRe)
    {
        string RowResult;
        token tok = lex->peekahead(1);
        if (tok.tokenClass == t_Word|| tok.tokenClass == t_Reserve) {
            int re = parseSelectRowList(RowResult);
            if (re == -1)
                return re;
        }
        else if (tok.tokenClass == t_Character) {
            int re = consumeToken("*");
            if (re == -1)
                return re;
            RowResult += "normal!*";
        }
        else
        {


        }
        RowRe = RowResult;
        return 0;

    }

    int parseSelect(vector<string> &sqlResult)
    {
        //lex->printAll();
        int re = consumeToken("select");
        if (re == -1)
            return -1;

        string RowRe;
        re = parseRow(RowRe);
        if (re == -1)
            return -1;

        re = consumeToken("from");
        if (re == -1)
            return re;

        string tablelist;
        token table_id = lex->peekahead(1);
        if (table_id.tokenClass != t_Word) {
            cout << "语法错误！应该后面跟表名\n" <<endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        else
        {
            int re = parseTableIdList(tablelist);
            if (re == -1)
                return re;
        }

        string whereRe;
        token tok = lex->peekahead(1);
        if (tok.tokenName.compare("where") == 0) {
            int re = parseWhereExpression(whereRe);
            if (re == -1)
                return re;
            re = consumeToken(";");//这里可以发现有多个consume ';'的情况。实际上可以把;放到外面，这样consume一次就行了。这个就是看文法怎么设计。
            if (re == -1)
                return re;
        }
        else if (tok.tokenName.compare(";") == 0) {
            int re = consumeToken(";");
            if (re == -1)
                return re;
        }
        else {
            cout << "语法错误！缺少;或者继续写where子句"<<endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }

        vector<string> semaResult;

        semaResult.push_back("sql select");
        semaResult.push_back("GetTable '"+tablelist+"'");
        semaResult.push_back("GetRow '" + RowRe + "'");
        semaResult.push_back("Condition '" + whereRe + "'");

        sqlResult = semaResult;

        return 0;
    }

    int parseExprRowRow(string &RowRow)
    {
        token tokleft = consumeToken();
        if(tokleft.tokenClass!=t_Word)
        {
            clueofError(lex->getCurrentIndex());
            return -1;
        }

        int re = consumeToken(".");
        if (re == -1) {
            cout << "必须在列名之前加 .表名！"<<endl;
            return re;
        }

        token tokright = consumeToken();

        if (tokright.tokenClass != t_Word)
        {
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        RowRow = (tokleft.tokenName + "." + tokright.tokenName);
        return 0;
    }

    int parseExprOnlyRowRow(string &exprRowResult)
    {

        string tokenleft;
        string tokenright;
        string tokenop;

        token tokleft;
        token tokop;
        token tokright;

        token tokpeek;

        int isjoin = 0;



        int re = parseExprRowRow(tokenleft);

        if (re == -1)
            return re;

        isjoin++;

        tokop = consumeToken();
        if(tokop.tokenName.compare("=") == 0)
        {

        }
        else if (tokop.tokenName.compare("<") == 0||tokop.tokenName.compare("<=") == 0)
        {
            token tok = lex->peekahead(1);
            if (tok.tokenClass == t_String) {
                cout << "字符串不能参与大小比较！" << endl;
                clueofError(lex->getCurrentIndex() + 1);
                return -1;
            }
        }
        else if (tokop.tokenName.compare(">") == 0||tokop.tokenName.compare(">=") == 0)
        {
            token tok = lex->peekahead(1);
            if (tok.tokenClass == t_String){
                cout << "字符串不能参与大小比较！"<< endl;
                clueofError(lex->getCurrentIndex() + 1);
                return -1;
            }
        }
        else if (tokop.tokenName.compare("!") == 0||tokop.tokenName.compare("!=") == 0)
        {

        }
        else
        {
            if(tokop.tokenClass== t_Character)
                cout << "语法错误!" << tokop.tokenName << "不支持的比较符！" << endl;
            else
                cout << "语法错误!" <<"需要在后面跟 > < = ! 这几个符号，而不是直接写新的单词！"<< endl;
            clueofError(lex->getCurrentIndex());
            return -1;

        }
        tokpeek = lex->peekahead(2);
        if (tokpeek.tokenName.compare(".") == 0)
        {
            int re = parseExprRowRow(tokenright);
            if (re == -1)
                return re;
            isjoin++;
        }
        else {
            tokright = consumeToken();
            tokenright = tokright.tokenName;
            if (tokright.tokenClass != t_String && tokright.tokenClass != t_Number)
            {
                cout << "语法错误!" << tokright.tokenName << "应该是单词、字符串或者数字！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }

        if(isjoin == 2) {
            if(tokenright.front ()=='\'')
            {
                tokenright = tokenright.substr (1,tokenright.size ()-2);
            }
            exprRowResult = "[compareJoin " + tokop.tokenName + " " + tokenleft + " " + tokenright + "] ";
        }
        else {
            if(tokenright.front ()=='\'')
            {
                tokenright = tokenright.substr (1,tokenright.size ()-2);
            }
            exprRowResult = "[compare " + tokop.tokenName + " " + tokenleft + " " + tokenright + "] ";
        }
        return 0;
    }

    int parseExprRow(string &exprRowResult)
    {

        string tokenleft;
        string tokenright;
        string tokenop;

        token tokleft;
        token tokop;
        token tokright;

        token tokpeek;

        int isjoin = 0;
        tokpeek = lex->peekahead(2);
        if (tokpeek.tokenName.compare(".") == 0)
        {
            int re = parseExprRowRow(tokenleft);
            if (re == -1)
                return re;
            isjoin++;
        }
        else {
            tokleft = consumeToken();
            tokenleft = tokleft.tokenName;
            if (tokleft.tokenClass != t_Word)
            {

                cout << "语法错误!" << tokleft.tokenName << "此处应该用单词写表名！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }

        tokop = consumeToken();
        if(tokop.tokenName.compare("=") == 0)
        {

        }
        else if (tokop.tokenName.compare("<") == 0)
        {
            token tok = lex->peekahead(1);
            if (tok.tokenClass == t_String) {
                cout << "字符串不能参与大小比较！" << endl;
                clueofError(lex->getCurrentIndex() + 1);
                return -1;
            }
        }
        else if (tokop.tokenName.compare(">") == 0)
        {
            token tok = lex->peekahead(1);
            if (tok.tokenClass == t_String){
                cout << "字符串不能参与大小比较！"<< endl;
                clueofError(lex->getCurrentIndex() + 1);
                return -1;
            }
        }
        else if (tokop.tokenName.compare("!") == 0)
        {

        }
        else
        {
            if(tokop.tokenClass== t_Character)
                cout << "语法错误!" << tokop.tokenName << "不支持的比较符！" << endl;
            else
                cout << "语法错误!" <<"需要在后面跟 > < = ! 这几个符号，而不是直接写新的单词！"<< endl;
            clueofError(lex->getCurrentIndex());
            return -1;

        }
        tokpeek = lex->peekahead(2);
        if (tokpeek.tokenName.compare(".") == 0)
        {
            int re = parseExprRowRow(tokenright);
            if (re == -1)
                return re;
            isjoin++;
        }
        else {
            tokright = consumeToken();
            tokenright = tokright.tokenName;
            if (tokright.tokenClass != t_Word && tokright.tokenClass != t_String && tokright.tokenClass != t_Number)
            {
                cout << "语法错误!" << tokright.tokenName << "应该是单词、字符串或者数字！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }

        if(isjoin == 2) {
            exprRowResult = "[compareJoin " + tokop.tokenName + " " + tokenleft + " " + tokenright + "] ";
        }
        else
            exprRowResult = "[compare " + tokop.tokenName + " " + tokenleft + " " + tokenright + "] ";
        return 0;
    }
    int parseExprAnd2(string &exprReAnd2)
    {
        int re = consumeToken("and");
        if (re == -1)
            return re;
        string exprRe;
        re = parseExprOnlyRowRow(exprRe);

        if (re == -1)
            return re;
        if (lex->peekahead(1).tokenName.compare("and") == 0) {

            int re = parseExprAnd2(exprReAnd2);
            if (re == -1)
                return -1;
        }
        else if (lex->peekahead(1).tokenName.compare(";") == 0|| lex->peekahead(1).tokenName.compare("or") == 0)
            ;
        else {
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        exprReAnd2 = exprReAnd2 + "&" + exprRe;
        return 0;

    }
    int parseExprAnd(string &exprAndRe)
    {
        string Result;
        string exprRe;
        int re = parseExprOnlyRowRow(exprRe);

        if (re == -1)
            return re;
        token tok = lex->peekahead(1);
        if (tok.tokenName.compare("and") == 0) {
            re = parseExprAnd2(Result);
            if (re == -1)
                return re;
        }
        else if (tok.tokenName.compare(";") == 0 || lex->peekahead(1).tokenName.compare("or") == 0)
            ;
        else {
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        Result = Result + exprRe;
        exprAndRe += Result;
        return 0;

    }
    int parseExprOr2(string &exprOr2Re)
    {
        string exprOrRe;
        int re = consumeToken("or");

        if (re == -1)
            return re;
        re = parseExprAnd(exprOrRe);


        if (re == -1)
            return re;
        if(lex->peekahead(1).tokenName.compare("or") == 0){
            int re = parseExprOr2(exprOr2Re);
            if (re == -1)
                return re;
        }
        else if (lex->peekahead(1).tokenName.compare(";") == 0)
            ;
        else {
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        exprOr2Re = exprOr2Re + "|" + exprOrRe;
        return 0;
    }
    int parseExprOr(string &exprRe)
    {
        int re;
        string Result;
        string ExprOrRe;
        re = parseExprAnd(ExprOrRe);

        if (re == -1)
            return re;
        if (lex->peekahead(1).tokenName.compare("or") == 0) {
            int re = parseExprOr2(Result);
            if (re == -1)
                return re;
        }
        else if (lex->peekahead(1).tokenName.compare(";") == 0)
            ;
        else {
            clueofError(lex->getCurrentIndex());
            return - 1;
        }

        Result += ExprOrRe;
        exprRe = Result;


        return 0;
    }

    int parseExpr(string &ExprRe)
    {
        int re = parseExprOr(ExprRe);

        return re;

    }

    int parseWhereExpression(string &whereRe)
    {

        int re = consumeToken("where");
        if (re == -1)
            return -1;

        return parseExpr(whereRe);


    }





//---------------------------------------------------------------create_parser----------------------------------------//
/*
//CREATE TABLE Orders
	(
		O_Id int NOT NULL,
		OrderNo int NOT NULL,
		P_Id int,
		PRIMARY KEY(O_Id),
		FOREIGN KEY(P_Id) REFERENCES Persons(P_Id)
		)
//

create_s->$create $table id(cParaList);
cParaList->cParaList,cPara
cPara->id specifier rowConstrain^opt|keyConstrain (id)[ references id(id)]

*/
    int parseSpecifier(string &specStr)
    {
        string specfier;
        token spec = consumeToken();
        if (spec.tokenClass != t_Reserve)
        {
            cout << "语法错误!" << "应输入类型名！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        specfier = spec.tokenName;
        map<string,string> spe;
        spe["int"] = 4;
        spe["tinyint"] = 1;
        spe["smallint"] = 2;
        spe["float"] = 4;
        spe["double"] = 8;
        spe["char"] = -1;
        spe["varchar"] = -1;
        if (spe.find(spec.tokenName) == spe.end())
        {
            //cout << "语法错误!" << "应输入类型名！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }

        if (spec.tokenName.compare("varchar") == 0|| spec.tokenName.compare("char") == 0)
        {
            int re = consumeToken("(");
            if(re == -1)
                return re;

            token length = consumeToken();
            if(length.tokenClass != t_Number)
            {
                cout << "语法错误!" << "应输入数字！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
            re = consumeToken(")");
            if (re == -1)
                return re;

            specfier = spec.tokenName+"("+length.tokenName+")";

        }
        specStr = specfier;

        return 0;

    }


    int parseCPara(vector<string> &rows,vector<string> &specifiers)
    {
        token word = lex->peekahead(1);
        if (word.tokenClass == t_Word)
        {
            token id = consumeToken();

            string specifier;


            int re = parseSpecifier(specifier);
            if (re == -1)
                return re;

            rows.push_back(id.tokenName);
            specifiers.push_back(specifier);

            token cur = lex->peekCurrentToken();
            if (cur.tokenClass == t_Reserve)
            {
                if (cur.tokenName.compare("notNull") == 0 || cur.tokenName.compare("unique") == 0)
                {
                    token RowConstrain = consumeToken();
                }
                else if(cur.tokenName.compare("primaryKey") == 0)
                {
                    token keyConstrain = consumeToken();
                }
            }

        }
        else if(word.tokenClass == t_Reserve)
        {
            if (word.tokenName.compare("primaryKey") == 0)
            {
                consumeToken("primaryKey");
                int re;
                re = consumeToken("(");
                if (re == -1)
                    return re;
                token id = consumeToken();
                if (id.tokenClass != t_Word)
                {
                    clueofError(lex->getCurrentIndex());
                    return -1;
                }

                re = consumeToken(")");
                if (re == -1)
                    return re;

            }
            else if (word.tokenName.compare("foreignKey") == 0)
            {
                consumeToken("foreignKey");
                int re;
                re = consumeToken("(");
                if (re == -1)
                    return re;
                token inRow = consumeToken();
                if (inRow.tokenClass != t_Word)
                {
                    clueofError(lex->getCurrentIndex());
                    return -1;
                }
                re = consumeToken(")");
                if (re == -1)
                    return re;
                re = consumeToken("references");
                if (re == -1)
                    return re;
                token outTable = consumeToken();
                if (outTable.tokenClass != t_Word)
                {
                    clueofError(lex->getCurrentIndex());
                    return -1;
                }
                re = consumeToken("(");
                if (re == -1)
                    return re;
                token outRow = consumeToken();
                if (outRow.tokenClass != t_Word)
                {
                    clueofError(lex->getCurrentIndex());
                    return -1;
                }
                re = consumeToken(")");
                if (re == -1)
                    return re;
            }
            else
            {
                cout << "语法错误!" << "应输入主键、外键约束关键字！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        else
        {
            cout << "语法错误!" << "应输入列名或者主键、外键约束关键字！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        return 0;

    }

    int parseCParaList2(vector<string> &rows, vector<string> &specs)
    {
        token peek = lex->peekahead(1);
        if (peek.tokenName.compare(",") == 0)
        {
            int re;
            re = consumeToken(",");
            if (re == -1)
                return re;

            re = parseCPara(rows, specs);
            if (re == -1)
                return re;

            re = parseCParaList2(rows, specs);
            if (re == -1)
                return re;

        }
        else if (peek.tokenName.compare(")") == 0)
        {
            return 0;
        }
        else
        {

            cout << "语法错误!" << "要么继续加逗号写，要么就用）结束！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        return 0;

    }

    int parseCParaList(vector<string> &rows, vector<string> &specs)
    {

        int re = parseCPara(rows, specs);
        if (re == -1)
            return re;

        re = parseCParaList2(rows, specs);
        if (re == -1)
            return re;
        return 0;

    }


    int parseCreate(vector<string> &sqlResult)
    {
        vector<string> semaResult;

        token create = consumeToken();
        token table = consumeToken();
        if (table.tokenClass == t_Reserve && table.tokenName.compare("table") == 0)
            ;
        else
        {
            cout << "应输入table关键字！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }

        token table_name = consumeToken();
        if (table_name.tokenClass != t_Word)
        {
            cout << "应输入表名！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;
        }
        semaResult.push_back("sql create");
        semaResult.push_back("tableName "+table_name.tokenName);

        int re = consumeToken("(");
        if (re == -1)
            return -1;

        vector<string> tableRows;
        vector<string> tableRowsDefines;

        re = parseCParaList(tableRows,tableRowsDefines);

        string tableRowsStr;
        for (int i = 0; i < tableRows.size(); i++)
            tableRowsStr = tableRowsStr + (tableRows[i] + ",");
        tableRowsStr = tableRowsStr.substr(0, tableRowsStr.size() - 1);

        string tableRowsDefineStr;

        for (int i = 0; i < tableRowsDefines.size(); i++)
            tableRowsDefineStr += (tableRowsDefines[i] + ",");
        tableRowsDefineStr = tableRowsDefineStr.substr(0, tableRowsDefineStr.size() - 1);
        semaResult.push_back("Rows " + tableRowsStr);
        semaResult.push_back("RowsDefine " + tableRowsDefineStr);


        if (re == -1)
            return re;
        re = consumeToken(")");
        if (re == -1)
            return -1;
        re = consumeToken(";");
        if (re == -1)
            return -1;

        sqlResult = semaResult;

        return 0;
    }

//-----------------------------------------------------------insertparser---------------------------------------------------//




    int parseSimpleIdList(vector<string> &rowList)
    {
        token tokValue = consumeToken();
        rowList.push_back(tokValue.tokenName);
        if (tokValue.tokenClass != t_Word)
        {
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }

        while (1)
        {
            token tok_split = lex->peekCurrentToken();
            if (tok_split.tokenName.compare(",") == 0)
            {
                consumeToken(",");
            }
            else if (tok_split.tokenName.compare(")") == 0)
            {
                return 0;
            }
            else
            {
                this->clueofError(lex->getCurrentIndex());
                return -1;
            }
            token tokValue = consumeToken();
            rowList.push_back(tokValue.tokenName);
            if (tokValue.tokenClass != t_Word)
            {
                this->clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        return 0;

    }
    int parseSimpleNumAndStrList(vector<string> &valueList)
    {
        token tokValue = consumeToken();
        valueList.push_back(tokValue.tokenName);
        if (tokValue.tokenClass != t_Number && tokValue.tokenClass != t_String)
        {
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }

        while (1)
        {
            token tok_split = lex->peekCurrentToken();
            if(tok_split.tokenName.compare(",") == 0)
            {
                int re = consumeToken(",");
                if(re == -1)
                    return re;
            }
            else if (tok_split.tokenName.compare(")") == 0)
            {
                return 0;
            }
            else
            {
                this->clueofError(lex->getCurrentIndex());
                return -1;
            }
            token tokValue = consumeToken();
            valueList.push_back(tokValue.tokenName);
            if (tokValue.tokenClass != t_Number && tokValue.tokenClass != t_String)
            {
                this->clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        return 0;

    }
//insert into joke (gid,name)values(0,”joker”),(1,”jhj”);

    int parseIdList(vector<string> &rowList)
    {

        int re = consumeToken("(");
        if (re == -1)
            return re;
        re = parseSimpleIdList(rowList);
        if (re == -1)
            return re;
        re = consumeToken(")");
        if (re == -1)
            return re;

        return 0;
    }

    int parseValueList(vector<string> &valueList)
    {
        int re = consumeToken("(");
        if (re == -1)
            return re;
        re = parseSimpleNumAndStrList(valueList);
        if (re == -1)
            return re;
        re = consumeToken(")");
        if (re == -1)
            return re;

        return 0;
    }
    int parseValueLists(vector<vector<string>> &valuesLists)
    {
        vector<string> valueList;
        int re = parseValueList(valueList);

        valuesLists.push_back(valueList);
        if (re == -1)
            return re;
        while (1)
        {
            token tok_split = lex->peekCurrentToken();
            if (tok_split.tokenName.compare(",") == 0)
            {
                consumeToken(",");
            }
            else if (tok_split.tokenName.compare(";") == 0)
            {
                return 0;
            }
            else
            {
                this->clueofError(lex->getCurrentIndex());
                return -1;
            }
            vector<string> valueList;
            int re = parseValueList(valueList);
            valuesLists.push_back(valueList);
            if (re == -1)
                return re;
        }
    }

    int parseInsert(vector<string> &sqlResult)
    {
        int re = consumeToken("insert");
        if (re == -1)
            return re;
        re  = consumeToken("into");
        if (re == -1)
            return re;
        token tablename = consumeToken();
        if (tablename.tokenClass != t_Word)
        {
            cout << "应使用单词输入表名！" << endl;
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }

        token tok = lex->peekahead (1);
        vector<string> rowlist;
        if(tok.tokenName.compare ("(") == 0) {
            re = parseIdList (rowlist);
            if (re == -1)
                return re;
        }
        re = consumeToken("values");
        if (re == -1)
            return re;
        vector<vector<string>> valueslists;
        re = parseValueLists(valueslists);
        if (re == -1)
            return re;
        re = consumeToken(";");
        if (re == -1)
            return re;

        vector<string> semaResult;

        string rowListStr;
        for (int i = 0; i < rowlist.size(); i++)
            rowListStr+=(rowlist[i]+",");
        rowListStr = rowListStr.substr(0, rowListStr.size() - 1);

        string valuesListStr;
        string valueStr;
        for (int i = 0; i < valueslists.size(); i++) {
            for (int j = 0; j < valueslists[i].size(); j++) {

                valueStr += (valueslists[i][j] + ",");
            }
            valueStr = valueStr.substr(0, valueStr.size() - 1);
            valuesListStr += (valueStr + "_");
            valueStr.clear();
        }
        valuesListStr = valuesListStr.substr(0, valuesListStr.size() - 1);

        semaResult.push_back("sql insert");
        semaResult.push_back("tableName " + tablename.tokenName);
        semaResult.push_back("rowList " + rowListStr);
        semaResult.push_back("values " + valuesListStr);
        sqlResult = semaResult;

        return 0;
    }


//------------------------------------------------------------updateparser-------------------------------------------------------//

/*
UPDATE test.beyond b
SET b.args1 = '001', b.args2 = '002'
WHERE b.args1 = '11' AND b.args2 = '22'
*/



    int parseSetRow(string &row)
    {
        string tokenleft;
        string tokenright;
        string tokenop;

        token tokleft;
        token tokop;
        token tokright;

        token tokpeek;
        tokpeek = lex->peekahead(2);



        if (tokpeek.tokenName.compare(".") == 0)
        {
            int re = parseExprRowRow(tokenleft);
            if (re == -1)
                return re;

        }
        else {
            tokleft = consumeToken();
            tokenleft = tokleft.tokenName;
            if (tokleft.tokenClass != t_Word)
            {

                cout << "语法错误!" << tokleft.tokenName << "此处应该用单词写表名！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }

        }

        tokop = consumeToken();
        if (tokop.tokenName.compare("=") == 0)
        {

        }
        else
        {
            if (tokop.tokenClass == t_Character)
                cout << "语法错误!" << tokop.tokenName << "只能用赋值等号！" << endl;
            else
                cout << "语法错误!" << "需要在后面跟赋值符号，而不是直接写新的单词！" << endl;
            clueofError(lex->getCurrentIndex());
            return -1;

        }
        tokpeek = lex->peekahead(2);
        if (tokpeek.tokenName.compare(".") == 0)
        {
            int re = parseExprRowRow(tokenright);
            if (re == -1)
                return re;
        }
        else {
            tokright = consumeToken();
            tokenright = tokright.tokenName;
            if (tokright.tokenClass != t_String && tokright.tokenClass != t_Number)
            {
                cout << "语法错误!" << tokright.tokenName << "应该是具体的值！" << endl;
                clueofError(lex->getCurrentIndex());
                return -1;
            }
        }
        row = tokenleft + tokop.tokenName + tokenright;


        return 0;
    }

    int parseSetStr(string &setResult)
    {
        string row;
        int re = parseSetRow(row);
        setResult += row + ",";

        if (re == -1)
            return re;
        token peek = lex->peekahead(1);
        if (peek.tokenName.compare(",") == 0)
        {
            int re = consumeToken(",");
            if (re == -1)
                return -1;
            re = parseSetStr(setResult);
            if (re == -1)
                return -1;
        }
        else if (peek.tokenName.compare("where") == 0)
            ;
        else if (peek.tokenName.compare(";") == 0)
            ;
        else
        {
            this->clueofError(lex->getCurrentIndex()+1);
            return -1;
        }


    }

    int parseUpdate(vector<string> &parseResult)
    {
        int re = consumeToken("update");
        if (re == -1)
            return re;

        token tableName;

        tableName = consumeToken();
        if (tableName.tokenClass != t_Word)
        {
            cout << "请输入正确格式的表名" << endl;
            this->clueofError(lex->getCurrentIndex());
        }
        re = consumeToken("set");
        if (re == -1)
            return re;

        string setResult;
        re = parseSetStr(setResult);
        if (re == -1)
            return re;


        token tok = lex->peekahead (1);

        string result;
        if(tok.tokenName.compare ("where") == 0) {
            re = parseWhereExpression(result);
            if (re == -1)
                return re;
        }



        re = consumeToken(";");
        if (re == -1)
            return re;
        parseResult.push_back("sql update");
        parseResult.push_back("tableName "+tableName.tokenName);
        parseResult.push_back("setAssign "+setResult.substr(0,setResult.size() - 1));
        parseResult.push_back("conditions "+result);
        return 0;

    }
//------------------------------------------------------------deleteparser-----------------------------------------------------------//

/*
DELETE orders,itrms FROM orders,items
WHERE orders.userid = items.userid
AND orders.orderid = items.orderid
AND orders.date<"2000/03/01";
*/


    int parseDelete(vector<string> &sqlResult)
    {



        int re = consumeToken("delete");

        if (re == -1)
            return re;
        re = consumeToken("from");
        if (re == -1)
            return re;


        token tableName;
        tableName = consumeToken();



        if (tableName.tokenClass == t_Word)
        {
        }
        else {
            cout << "请输入正确的表名！" << endl;
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }

        string result;
        token peek = lex->peekahead(1);
        if (peek.tokenName.compare("where") == 0) {

            int re = parseWhereExpression(result);
            if (re == -1)
                return re;
        }
        else
            ;

        re = consumeToken(";");

        if(re == -1)
            return re;



        sqlResult.push_back("sql delete");
        sqlResult.push_back("tableName "+tableName.tokenName);
        sqlResult.push_back("condition '" +result +"'");
        return 0;

    }




///------------------------------------------------------------dropparser-----------------------------------------------------------//

    int parseDrop(vector<string> &sqlResult)
    {
        int re;
        re = consumeToken("drop");
        if (re == -1)
            return -1;
        re = consumeToken("table");
        if (re == -1)
            return re;

        token tokValue = consumeToken();

        if (tokValue.tokenClass != t_Word)
        {
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }
        re = consumeToken(";");
        if (re == -1)
            return -1;
        sqlResult.push_back("sql drop");
        sqlResult.push_back("tableName " + tokValue.tokenName);

    }

    ///----------------------------------------------fragmentparser------------------------------------------------------------------//


    //frag=>fragment on <tableId> by <fragType> where <fragDefineList>
    //fragType=>horizental|vertical
    //fragDefineList=>range <rangeDefineList>|enum <enumDefineList>
    //rangeDefineList=><rangeDefne>,<rangeDefineList>|<rangeDefine>
    //enumDefineList=><enumDefine>,<enumDefineList>|<enumDefine>
    //enumDefine=>(<id>=<str>)
    //rangeDefine=>(<id> between <number>&<number>)

    int parseEnumDefine(vector<string> &pedResult)
    {
        int re = consumeToken ("(");
        if(re == -1)
            return re;


        token rowId = consumeToken ();
        if(rowId.tokenClass != t_Word)
        {
            cout << "请输入正确的列名！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("=");
        if(re == -1)
            return re;



        token str;
        str = consumeToken ();
        if(str.tokenClass !=t_Number &&str.tokenClass !=t_Word && str.tokenClass !=t_String)
        {
            cout << "请输入正确的值！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        pedResult.push_back ("=");
        pedResult.push_back (rowId.tokenName);
        pedResult.push_back (str.tokenName);



        re = consumeToken (")");
        if(re == -1)
            return re;


        return 0;

    }
    int parseRangeDefine(vector<string> &rangeResult)
    {
        int re = consumeToken ("(");
        if(re == -1)
            return re;


        token rowId = consumeToken ();
        if(rowId.tokenClass != t_Word)
        {
            cout << "请输入正确的列名！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("between");
        if(re == -1)
            return re;


        token numberLeft;
        token numberRight;

        numberLeft = consumeToken ();
        if(numberLeft.tokenClass != t_Number)
        {
            cout << "请输入数字！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }



        re = consumeToken ("&");
        if(re == -1)
            return re;



        numberRight = consumeToken ();
        if(numberRight.tokenClass != t_Number)
        {
            cout << "请输入数字！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        rangeResult.push_back ("between");
        rangeResult.push_back (rowId.tokenName);
        rangeResult.push_back (numberLeft.tokenName+"&"+numberRight.tokenName);

        re = consumeToken (")");
        if(re == -1)
            return re;


        return 0;
    }
    int parseEnumDefineList(vector<vector<string>> &pedsResult)
    {
        vector<string> pedResult;
        int re = parseEnumDefine (pedResult);
        pedsResult.push_back (pedResult);
        if(re == -1)
            return re;
        token peek = lex->peekahead (1);
        if(peek.tokenName.compare (",") == 0) {
            int re =  consumeToken (",");
            if(re == -1)
                return re;
            re = parseEnumDefineList(pedsResult);
            if(re == -1)
                return re;
        } else if(peek.tokenName.compare (";") == 0)
            ;
        else
        {
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        return 0;


    }

    int parseRangeDefineList(vector<vector<string>> &rangeResults)
    {
        vector<string> rangeResult;
        int re = parseRangeDefine(rangeResult);
        rangeResults.push_back (rangeResult);
        if(re == -1)
            return re;
        token peek = lex->peekahead (1);
        if(peek.tokenName.compare (",") == 0) {
            consumeToken (",");
            re = parseRangeDefineList (rangeResults);
            if(re == -1)
                return re;
        } else if(peek.tokenName.compare (";") == 0)
            ;
        else
        {
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        return 0;

    }
    int parseFragDefineList(string &fMethod,vector<vector<string>> &fragmentList)
    {

        token peek;
        peek = lex->peekahead (1);
        if(peek.tokenClass != t_Word) {
            token c = consumeToken ();
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        if(peek.tokenName.compare ("range") == 0)
        {
            consumeToken ("range");
            fMethod = "range";
            int re = consumeToken (":");
            if(re == -1)
                return re;
            re = parseRangeDefineList (fragmentList);
            return re;
        }
        else if(peek.tokenName.compare ("enum") == 0)
        {
            consumeToken ("enum");
            fMethod = "enum";
            int re = consumeToken (":");
            if(re == -1)
                return re;
            re = parseEnumDefineList (fragmentList);
            return re;
        } else
        {
            cout << "请输入正确的划分方法！"<<endl;
            token c = consumeToken ();
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        return 0;
    }

    int parseFragType(string &ftype)
    {
        token type ;
        type = consumeToken ();
        if(type.tokenClass != t_Word){
            cout << "请输入划分关键字！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        if(type.tokenName.compare ("horizental") != 0 && type.tokenName.compare ("vertical") != 0)
        {
            cout << "请确定是垂直还是水平划分！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        ftype = type.tokenName;

        return 0;

    }

    int parseFrag(vector<string> &sqlResult)
    {
        int re = consumeToken ("fragment");
        if(re == -1)
            return re;
      //  re = consumeToken ("on");
       // if(re == -1)
       //     return re;

        token tableName;

        tableName = consumeToken ();
        if(tableName.tokenClass != t_Word) {
            cout << "请输入正确的表名！"<<endl;
            return -1;
        }
        re = consumeToken ("by");
        if(re == -1)
            return re;

        string ftype;
        re = parseFragType (ftype);
        if(re == -1)
            return re;

        re = consumeToken ("where");
        if(re == -1)
            return re;


        vector<vector<string>> vvs;
        string fMethod;
        re = parseFragDefineList (fMethod,vvs);

        sqlResult.push_back ("sql fragment");
        sqlResult.push_back ("tableName "+tableName.tokenName);
        sqlResult.push_back ("fragType "+ftype);
        sqlResult.push_back ("fragMethod "+fMethod);

        string fragments;
        for(int i = 0 ; i < vvs.size () ; i++)
        {
            string frag;
            for(int j = 0 ; j < vvs[i].size () ; j++)
            {
                frag+=(vvs[i][j]+",");
            }
            frag = frag.substr (0,frag.size ()-1 );
            fragments+=(frag+"_");
            frag.clear ();
        }
        fragments = fragments.substr (0,fragments.size () - 1);

        sqlResult.push_back ("fragments "+fragments);


        for(int i = 0 ; i < sqlResult. size () ; i++)
            cout << sqlResult[i] << endl;

        if(re == -1)
            return re;

        re = consumeToken (";");
        if(re == -1)
            return re;

        return 0;

    }


///------------------------------------------------------------descparser-----------------------------------------------------------//

    int parseDesc(vector<string> &sqlResult)
    {
        int re;
        re = consumeToken("desc");
        if (re == -1)
            return -1;

        token tokValue = consumeToken();

        if (tokValue.tokenClass != t_Word)
        {
            this->clueofError(lex->getCurrentIndex());
            return -1;
        }
        re = consumeToken(";");
        if (re == -1)
            return -1;
        sqlResult.push_back("sql desc");
        sqlResult.push_back("tableName " + tokValue.tokenName);

    }




    ///////------------------------------------------------------------manulaFragparser-----------------------------------------------------------//



    struct manualFragResult
    {
        string tableName;
        vector<map<string,map<string,string>>> enumDefines;//哪一个子表的哪一个列的enum定义
        vector<map<string,map<string,string>>> rangeDefines;//哪一个子表的哪一个列的range定义
        map<string,string> childTable2Ip;

    };

    int parseRangeDefine(map<string,string> &row2Range)
    {
        string rangeStr;

        int re = consumeToken ("range");
        if(re == -1)
            return re;

        re = consumeToken ("(");
        if(re == -1)
            return re;

        token rowName = consumeToken ();

        if(rowName.tokenClass != t_Word)
        {
            cout << "请输入正确的列名！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("between");
        if(re == -1)
            return re;

        token min = consumeToken ();
        if(min.tokenClass != t_Number)
        {
            cout << "请输入数字！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("&");
        if(re == -1)
            return re;

        token max = consumeToken ();
        if(max.tokenClass != t_Number)
        {
            cout << "请输入数字！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        if(row2Range.count (rowName.tokenName) > 0)
        {
            cout << "列"+rowName.tokenName+"重复定义！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        rangeStr +=("between,"+rowName.tokenName+","+min.tokenName+"&"+max.tokenName);
        row2Range[rowName.tokenName] = rangeStr;


        re = consumeToken (")");

        if(re == -1)
            return re;

        return 0;
    }

    //mFrag=>manualFrag <tableName>:<fragDefines>
//fragDefines=><fragDefines>,<fragDefine>|<fragDefine>
//fragDefine=>{<conditionDefines> => <tableName> on <ip>}
//conditionDefines=><conditionDefines>,<conditionDefine>|<conditionDefine>
//conditionDefine=><rangeDefine>|<enumDefine>
//rangeDefine=>range(<row> between <number>&<number>)
//enumDefine=>enum(<row>=<string>)|enum(<row>=<number>)
    int parseEnumDefine(map<string,string> &row2Enum)
    {
        string enumStr;

        int re = consumeToken ("enum");
        if(re == -1)
            return re;

        re = consumeToken ("(");
        if(re == -1)
            return re;

        token rowName = consumeToken ();

        if(rowName.tokenClass != t_Word)
        {
            cout << "请输入正确的列名！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("=");
        if(re == -1)
            return re;


        token value = consumeToken ();

        if(value.tokenName[0]=='\'')
        {
            value.tokenName = value.tokenName.substr (1,value.tokenName.size ()-2);
        }

        if(value.tokenClass != t_Number && value.tokenClass != t_String)
        {
            cout << "请输入正确的枚举值！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        enumStr+=("=,"+rowName.tokenName+","+value.tokenName);

        if(row2Enum.count (rowName.tokenName) > 0)
        {
            cout << "列"+rowName.tokenName+"重复定义！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        row2Enum[rowName.tokenName] = enumStr;

        re = consumeToken (")");
        if(re == -1)
            return re;
    }


    int parseConditionDefine(map<string,string> &rangeDefs,map<string,string> &enumDefs)
    {
        token fragType;
        fragType = lex->peekahead (1);

        if(fragType.tokenName.compare ("range") == 0 || fragType.tokenName.compare ("enum") == 0) {
            int re;
            if(fragType.tokenName.compare ("range") == 0)
                re = parseRangeDefine (rangeDefs);
            else
                re = parseEnumDefine(enumDefs);
            if(re == -1)
                return re;
        }
        else
        {
            cout << "请输入正确的划分类型！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }



        return 0;

    }

    //manualFrag student_test:{range (age between 0&10),enum(aaa=21) =>'student_test.1' on '192.168.0.12:132'};




    int parseConditionDefines(map<string,string> &rangeDefs,map<string,string> &enumDefs)
    {

        int re;
        re = parseConditionDefine(rangeDefs,enumDefs);
        if(re == -1)
            return re;

        token peek = lex->peekahead (1);


        if(peek.tokenName.compare (",") == 0)
        {
            re = consumeToken (",");

            if(re == -1)
                return re;
            re = parseConditionDefines(rangeDefs,enumDefs);
            if(re == -1)
                return re;
        }
        else if(peek.tokenName.compare ("=") == 0)
            ;
        else
        {

            cout << "语法错误！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }


        return 0;
    }

    int parseFragDefine(manualFragResult &mfr)
    {
        int re;

        map<string,string> rangeDefs;
        map<string,string> enumDefs;

        re = consumeToken ("{");

        if(re == -1)
            return re;
        re = parseConditionDefines (rangeDefs,enumDefs);


        if(re == -1)
            return re;

        re = consumeToken ("=");
        if(re == -1)
            return re;
        re = consumeToken (">");
        if(re == -1)
            return re;

        token childTableName = consumeToken ();
        if(childTableName.tokenClass != t_String)
        {
            cout << "表名请输入字符串！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        re = consumeToken ("on");

        if(re == -1)
            return re;

        token ipAddr = consumeToken ();
        if(childTableName.tokenClass != t_String)
        {
            cout << "地址请输入字符串！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }


        map<string,map<string,string>> child2EnumDef;
        child2EnumDef[childTableName.tokenName] = enumDefs;
        mfr.enumDefines.push_back (child2EnumDef);

        map<string,map<string,string>> child2RangeDef;
        child2RangeDef[childTableName.tokenName] = rangeDefs;
        mfr.rangeDefines.push_back (child2RangeDef);



        if(mfr.childTable2Ip.count(childTableName.tokenName) > 0)
        {
            cout << "出现了重复的子表名！"<<childTableName.tokenName <<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }
        mfr.childTable2Ip[childTableName.tokenName] = ipAddr.tokenName;


        re = consumeToken ("}");
        if(re == -1)
            return re;

        return 0;
    }


    int parseFragDefines(manualFragResult &mfr)
    {
       int re =  parseFragDefine(mfr);

        if(re == -1)
            return re;

        token peek = lex->peekahead (1);

        if(peek.tokenName.compare (",")== 0)
        {
            int re = consumeToken (",");
            re = parseFragDefines (mfr);
            if(re == -1)
                return re;
        }
        else if(peek.tokenName.compare (";") == 0)
            ;
        else
        {
            cout << "语法错误！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
            return -1;
        }

        return 0;

    }


    int parseManulaFrag(vector<string> &sqlResult)
    {
        int re;
        re = consumeToken ("manualFrag");
        if(re == -1)
            return re;

        manualFragResult mfr;

        token tableName = consumeToken ();

        mfr.tableName = tableName.tokenName;
        if(tableName.tokenClass != t_Word)
        {
            cout << "请输入正确的表名！"<<endl;
            this->clueofError (lex->getCurrentIndex ());
        }

        re = consumeToken (":");
        if(re == -1)
            return re;

        re = parseFragDefines(mfr);
        if(re == -1)
            return re;

        re = consumeToken (";");
        if(re == -1)
            return re;





      map<string,string> table2defs;


        map<string,string>::iterator it;
        it = mfr.childTable2Ip.begin ();

        while(it != mfr.childTable2Ip.end())
        {
            cout << it->first<<" ";
            cout << it->second<<endl;
            it++;
        }
        vector<map<string,map<string,string>>> enumDefs = mfr.enumDefines;
        for(int i = 0 ; i < enumDefs.size () ; i++){

            map<string,map<string,string>>::iterator it;
            it = enumDefs[i].begin ();

            while(it != enumDefs[i].end())
            {
                cout << it->first<<endl;

                string childTableName = it->first;
                map<string,string> row2def;
                row2def = (it->second);



                map<string,string>::iterator it2;
                it2 = row2def.begin ();
                while(it2!=row2def.end ())
                {
                    cout << it2->first<< " ";
                    cout << it2->second << endl;


                    if(mfr.childTable2Ip.count (childTableName ) > 0)
                    {
                        table2defs[it->first]+=(it2->second+"|");
                    }

                    it2++;
                }
                it ++;
            }
        }

        vector<map<string,map<string,string>>> rangeDefs = mfr.rangeDefines;
        for(int i = 0 ; i < rangeDefs.size () ; i++){

            map<string,map<string,string>>::iterator it;
            it = rangeDefs[i].begin ();

            while(it != rangeDefs[i].end())
            {
                cout << it->first<<endl;
                map<string,string> row2def;
                row2def = (it->second);

                string childTableName = it->first;

                map<string,string>::iterator it2;
                it2 = row2def.begin ();
                while(it2!=row2def.end ())
                {
                    cout << it2->first<< " ";
                    cout << it2->second << endl;


                    if(mfr.childTable2Ip.count (childTableName ) > 0)
                    {
                        table2defs[it->first]+=(it2->second+"|");
                    }

                    it2++;
                }

                table2defs[it->first] =  table2defs[it->first].substr (0, table2defs[it->first].size ()-1);
                it ++;
            }
        }

        cout << "|-----------------------------------------------------|"<<endl;


        string define;
        string tableip;

        map<string,string>::iterator it2;
        it2 = table2defs.begin ();
        while(it2!=table2defs.end ())
        {
            cout << it2->first<< " ";
            cout << it2->second << endl;

            tableip+=(it2->first.substr (1,it2->first.size ()-2))+","+(mfr.childTable2Ip[it2->first].substr (1,mfr.childTable2Ip[it2->first].size ()-2))+"*";
            define+=it2->second+"_";
            it2++;
        }


//manualFrag student_test:{range (age between 0&10),enum(aaa=21) =>'student_test.1' on '192.168.0.12:132'},{range (age between 0&10),enum(aaa=21) =>'student_test.2' on '192.168.0.12:132'};


        sqlResult.push_back ("sql manualFrag");

        define = define.substr (0, define.size ()-1);
        tableip = tableip.substr (0,tableip.size ()-1);

        sqlResult.push_back (tableName.tokenName);
        sqlResult.push_back (tableip);
        sqlResult.push_back (define);

        cout << tableip<< endl << define << endl;

        return 0;



    }


    ////-----------------------------------------------------------------------------------------------------------------------------////

    int parseShow(vector<string> &parseResult)
    {

        int re = consumeToken ("show");
        if(re == -1)
            return re;
        re = consumeToken (";");
        if(re == -1)
            return re;

        parseResult.push_back ("sql show");
        return 0;
    }


    ////----------------------------------------------------------------------------------------------------------------------------///


    int parseAddIp(vector<string> &parseResult)
    {
        int re = consumeToken ("addip");
        if(re == -1)
            return re;
        re = consumeToken (";");
        if(re == -1)
            return re;

        parseResult.push_back ("sql addip");
        return 0;

    }


    int parseLoadFile(vector<string> &parseResult)
    {
        int re = consumeToken ("loadfile");
        if(re == -1)
            return re;
        token filePath = consumeToken ();
        if(filePath.tokenClass!=t_String)
        {
            cout << "地址应该是字符串！"<<endl;
            return -1;
        }


        re = consumeToken("to");

        if(re == -1)
            return re;

        token tableName = consumeToken ();
        if(tableName.tokenClass!=t_Word)
        {
            cout << "请输入正确的表名！"<<endl;
            return -1;
        }

        re = consumeToken (";");
        if(re == -1)
            return re;
        parseResult.push_back ("sql loadfile");
        parseResult.push_back (tableName.tokenName);
        parseResult.push_back (filePath.tokenName);

        return 0;
    }


    ///--------------------------------------------------------------------------------------------------------------------------////

    int parse(vector<string> &parseResult)
    {
        do
        {

            token tok = lex->peekCurrentToken();
            ///cout << "["<< tok.tokenName<<"]";

            if (tok.tokenName.compare("select") == 0) {
                int re = parseSelect(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("create") == 0) {
                int re = parseCreate(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("insert") == 0) {
                int re = parseInsert(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("update") == 0) {
                int re = parseUpdate(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("delete") == 0) {
                int re = parseDelete(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("drop") == 0) {
                int re = parseDrop(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("fragment") == 0) {
                int re = parseFrag (parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("desc") == 0) {
                int re = parseDesc (parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("show") == 0) {
                int re = parseShow (parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("manualFrag") == 0) {
                int re = parseManulaFrag(parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("addip") == 0) {
                int re = parseAddIp (parseResult);
                if (re == -1)
                    return -1;
            }
            else if (tok.tokenName.compare("loadfile") == 0) {
                int re = parseLoadFile (parseResult);
                if (re == -1)
                    return -1;
            }
            else {
                cout << "不支持的操作.[" << tok.tokenName << "]" << endl;

                return -1;
            }

            //cout << lex->getCurrentIndex() << "|" << lex->getTokensNum();

        } while (lex->getCurrentIndex() < lex->getTokensNum() );
        return 0;

    }

};

