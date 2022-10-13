#include "global.h"
#include "page.h"
// <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupbyResTable = tokenizedQuery[0];
    parsedQuery.groupbyAttribute = tokenizedQuery[4];
    // cout << "Group by " << parsedQuery.groupbyAttribute << endl;
    parsedQuery.groupbyRelationName = tokenizedQuery[6];
    parsedQuery.groupbyReturn = tokenizedQuery[8];

    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");
    string retColName = (parsedQuery.groupbyReturn).substr(4, (parsedQuery.groupbyReturn).size() - 5);
    // cout << "ret col name = " << retColName << endl;
    // cout << "table = " << parsedQuery.groupbyRelationName << endl;

    if (tableCatalogue.isTable(parsedQuery.groupbyResTable))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyAttribute, parsedQuery.groupbyRelationName) || !tableCatalogue.isColumnFromTable(retColName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

void groupBy(Table *table, string resGroupby, string grpAttr, string retType)
{
    int colIdx = table->getColumnIndex(grpAttr);
    string retColName = retType.substr(4, retType.size() - 5);
    string retOp = retType.substr(0, 3);
    string newCol = retOp + retColName;
    // cout << "Return op = " << retOp << endl;
    // cout << "Return op over column " << retColName << endl;
    // cout << "New column = " << newCol << endl;

    vector<string> colName;
    colName.push_back(grpAttr);
    colName.push_back(newCol);
    
    Table *res = new Table(resGroupby, colName);

    int retCol = table->getColumnIndex(retColName);
    map<int, int> grMap;
    map<int, int> cntMap;
    // Possible operations MAX|MIN|SUM|AVG
    for (int i = 0; i < table->blockCount; i++)
    {
        Page tablePage = bufferManager.getPage(table->tableName, i);
        int row_num = tablePage.numRows();
        for (int j = 0; j < row_num; j++)
        {
            vector<int> r = tablePage.getRow(j);
            if (grMap.count(r[colIdx]) == 0)
            {
                grMap.insert(pair<int, int>(r[colIdx], r[retCol]));
            }
            else
            {
                if (retOp == "MAX")
                {
                    if (grMap[r[colIdx]] < r[retCol])
                    {
                        grMap[r[colIdx]] = r[retCol];
                    }
                }
                else if (retOp == "MIN")
                {
                    if (grMap[r[colIdx]] > r[retCol])
                    {
                        grMap[r[colIdx]] = r[retCol];
                    }
                }
                else if (retOp == "SUM")
                {
                    grMap[r[colIdx]] += r[retCol];
                }
                else if (retOp == "AVG")
                {
                    grMap[r[colIdx]] += r[retCol];
                    if (cntMap.count(r[colIdx]) == 0)
                    {
                        cntMap.insert(pair<int, int>(r[colIdx], 1));
                    }
                    else
                    {
                        cntMap[r[colIdx]]++;
                    }
                }
            }
        }
    }

    if (retOp == "AVG")
    {
        for (auto &j : grMap)
        {
            // cout << "NUM = " << cntMap[j.first] << endl;   
            j.second = j.second / cntMap[j.first];
            // cout << "UPDATED = " << j.second << endl;
        }
    }

    vector<int> data;
    for(auto k: grMap)
    {
        // cout << "first = " << k.first << endl;
        // cout << "sec = " << k.second << endl;
        data.push_back(k.first);
        data.push_back(k.second);
        res->writeRow<int>(data);
        data.clear();
    }

    res->blockify();
    tableCatalogue.insertTable(res);

    return;
}

void executeGROUPBY()
{
    logger.log("executeGrOUPBY");
    Table *t = tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    string grpAttr = parsedQuery.groupbyAttribute;
    string ret = parsedQuery.groupbyReturn;
    string resTable = parsedQuery.groupbyResTable;
    groupBy(t, resTable, grpAttr, ret);
    return;
}
