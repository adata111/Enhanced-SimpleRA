#include "global.h"
#include "page.h"
/**
 * @brief
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
// <new_relation_name> <- JOIN USING NESTED <table1>, <table2> ON <column1> <bin_op> <column2> BUFFER <buffer_size>
// <new_relation_name> <- JOIN USING PARTHASH <table1>, <table2> ON <column1> <bin_op> <column2> BUFFER <buffer_size>
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 13 || tokenizedQuery[7] != "ON" || tokenizedQuery[11] != "BUFFER" || (tokenizedQuery[4] != "NESTED" && tokenizedQuery[4] != "PARTHASH"))
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinType = tokenizedQuery[4];
    parsedQuery.joinFirstRelationName = tokenizedQuery[5];
    parsedQuery.joinSecondRelationName = tokenizedQuery[6];
    parsedQuery.joinFirstColumnName = tokenizedQuery[8];
    parsedQuery.joinSecondColumnName = tokenizedQuery[10];
    parsedQuery.joinBuffer = stoi(tokenizedQuery[12]);

    string binaryOperator = tokenizedQuery[9];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

map<int, vector<vector<int>>> mapify(vector<vector<int>> buffPages, int colIdx)
{
    map<int, vector<vector<int>>> attrMap;
    for (int x = 0; x < buffPages.size(); x++)
    {
        if (attrMap.count(buffPages[x][colIdx]) == 0)
        {
            vector<vector<int>> temp;
            temp.push_back(buffPages[x]);
            attrMap.insert(pair<int, vector<vector<int>>>(buffPages[x][colIdx], temp));
        }
        else
        {
            attrMap[buffPages[x][colIdx]].push_back(buffPages[x]);
        }
    }
    return attrMap;
}

void nestedJoin(Table *t1, Table *t2, string col1, string col2, string resJoin, int buffSize, auto binOp)
{
    // cout << col1 << endl;
    // cout << col2 << endl;
    int b1 = t1->blockCount;
    int b2 = t2->blockCount;
    Table *smallTable;
    Table *bigTable;
    int small, big;
    int colIdxBig, colIdxSmall;
    if (b1 > b2)
    {
        smallTable = t2;
        small = 2;
        colIdxSmall = t2->getColumnIndex(col2);
        bigTable = t1;
        big = 1;
        colIdxBig = t1->getColumnIndex(col1);
        // cout << "col1 = " << colIdxBig << endl;
        // cout << "col2 = " << colIdxSmall << endl;
    }
    else
    {
        smallTable = t1;
        small = 1;
        colIdxSmall = t1->getColumnIndex(col1);
        bigTable = t2;
        big = 2;
        colIdxBig = t2->getColumnIndex(col2);
        // cout << "col2 = " << colIdxBig << endl;
        // cout << "col1 = " << colIdxSmall << endl;
    }

    // cout << "set big, small table\n";
    // Extracting column names of both tables
    vector<string> colName = t1->columns;
    colName.insert(colName.end(), t2->columns.begin(), t2->columns.end());
    // Creating new table for result
    Table *res = new Table(resJoin, colName);
    // cout << "created result table\n";
    // Reading small table blockwise
    int pgIdx = 0;
    vector<vector<int>> data;
    for (int i = 0; i < smallTable->blockCount; i += (buffSize - 2))
    {
        // cout << "reading small table nb-2 blocks at a time\n";
        // cout << "round " << i << endl;
        vector<vector<int>> buffPages;
        // Reading buffsize - 2 number of blocks at a time.
        for (int j = 0; (j < (buffSize - 2)) && ((i+j) < smallTable->blockCount); j++)
        {
            // cout << "reading page " << i+j << endl;
            Page this_page = bufferManager.getPage(smallTable->tableName, i + j);
            // cout << "read " << j << "th page successfully\n";
            int row_num = this_page.numRows();
            // cout << "number of rows on the page = " << row_num << endl;
            for (int k = 0; k < row_num; k++)
            {
                vector<int> tempVec = this_page.getRow(k);
                buffPages.push_back(tempVec);
            }
        }

        // Reading the larger table block by block
        for (int y = 0; y < bigTable->blockCount; y++)
        {
            Page tablePage = bufferManager.getPage(bigTable->tableName, y);
            int row_num = tablePage.numRows();
            int joinFlag = 0;
            int overFlow = 0;
            // cout << "Big table page num = " << y << endl;
            for (int k = 0; k < row_num; k++)
            {
                // cout << "reading row " << k << endl;
                vector<int> r = tablePage.getRow(k);

                for (int x = 0; x < buffPages.size(); x++)
                {
                    // t1.colIdx1 < t2.colIdx2
                    if (parsedQuery.joinBinaryOperator == LESS_THAN)
                    {
                        // cout << "less than\n";
                        if (small == 1 && big == 2)
                        {
                            if (buffPages[x][colIdxSmall] < r[colIdxBig])
                            {
                                joinFlag = 1;
                            }
                        }
                        else
                        {
                            if (r[colIdxBig] < buffPages[x][colIdxSmall])
                            {
                                joinFlag = 1;
                            }
                        }
                    }
                    // t1.colIdx1 > t2.colIdx2
                    else if (parsedQuery.joinBinaryOperator == GREATER_THAN)
                    {
                        // cout << "greater than\n";
                        if (small == 1 && big == 2)
                        {
                            if (buffPages[x][colIdxSmall] > r[colIdxBig])
                            {
                                joinFlag = 1;
                            }
                        }
                        else
                        {
                            if (r[colIdxBig] > buffPages[x][colIdxSmall])
                            {
                                joinFlag = 1;
                            }
                        }
                    }
                    // t1.colIdx1 >= t2.colIdx2
                    else if (parsedQuery.joinBinaryOperator == GEQ)
                    {
                        // cout << "geq\n";
                        if (small == 1 && big == 2)
                        {
                            if (buffPages[x][colIdxSmall] >= r[colIdxBig])
                            {
                                joinFlag = 1;
                            }
                        }
                        else
                        {
                            if (r[colIdxBig] >= buffPages[x][colIdxSmall])
                            {
                                joinFlag = 1;
                            }
                        }
                    }
                    // t1.colIdx1 <= t2.colIdx2
                    else if (parsedQuery.joinBinaryOperator == LEQ)
                    {
                        // cout << "less than equal to\n";
                        if (small == 1 && big == 2)
                        {
                            if (buffPages[x][colIdxSmall] <= r[colIdxBig])
                            {
                                joinFlag = 1;
                            }
                        }
                        else
                        {
                            if (r[colIdxBig] <= buffPages[x][colIdxSmall])
                            {
                                joinFlag = 1;
                            }
                        }
                    }

                    // t1.colIdx1 == t2.colIdx2
                    else if (parsedQuery.joinBinaryOperator == EQUAL)
                    {
                        // cout << "comparing " << buffPages[x][colIdxSmall] << " and " << r[colIdxBig] << "\n";
                        if (buffPages[x][colIdxSmall] == r[colIdxBig])
                        {
                            // cout << "equal\n";
                            joinFlag = 1;
                        }
                    }

                    // t1.colIdx1 != t2.colIdx2
                    else if (parsedQuery.joinBinaryOperator == NOT_EQUAL)
                    {
                        if (buffPages[x][colIdxSmall] != r[colIdxBig])
                        {
                            // cout << "not equal\n";
                            joinFlag = 1;
                        }
                    }

                    if (joinFlag == 1)
                    {
                        if (data.size() == res->maxRowsPerBlock)
                        {
                            Page *pgJoin = new Page(res->tableName, pgIdx, data, data.size());
                            pgJoin->writePage();
                            res->rowsPerBlockCount.push_back(data.size());
                            res->blockCount++;
                            res->rowCount += data.size();
                            data.clear();
                            pgIdx++;
                        }
                        {
                            if (small == 1 && big == 2)
                            {
                                vector<int> temp2 = buffPages[x];
                                temp2.insert(temp2.end(), r.begin(), r.end());
                                data.push_back(temp2);
                                temp2.clear();
                            }
                            else
                            {
                                vector<int> temp2 = r;
                                temp2.insert(temp2.end(), buffPages[x].begin(), buffPages[x].end());
                                data.push_back(temp2);
                                temp2.clear();
                            }
                        }

                        joinFlag = 0;
                    }
                }
            }
        }
    }

    if (data.size() != 0)
    {
        Page *pgJoin = new Page(res->tableName, pgIdx, data, data.size());
        pgJoin->writePage();
        res->blockCount++;
        res->rowCount += data.size();
        res->rowsPerBlockCount.push_back(data.size());
        data.clear();
        pgIdx++;
    }

    tableCatalogue.insertTable(res);
    cout << "Number of Block Accesses: " << blockAcc << endl;
    blockAcc = 0;
    return;
}

pair<vector<int>, vector<vector<int>>> partHash(Table *t, int buffSize, int colIdx)
{
    map<int, vector<vector<int>>> hashMap;
    vector<vector<int>> numRows;
    int num = buffSize - 1;
    for (int t = 0; t < num; t++)
    {
        vector<int> vec;
        numRows.push_back(vec);
    }
    vector<int> buckets(num, 0);
    for (int i = 0; i < t->blockCount; i++)
    {
        Page tablePage = bufferManager.getPage(t->tableName, i);
        int row_num = tablePage.numRows();
        for (int k = 0; k < row_num; k++)
        {
            vector<int> r = tablePage.getRow(k);
            if (hashMap.count(r[colIdx] % num) == 0)
            {
                vector<vector<int>> temp;
                temp.push_back(r);
                hashMap.insert(pair<int, vector<vector<int>>>(r[colIdx] % num, temp));
            }

            else
            {
                if (hashMap[r[colIdx] % num].size() < t->maxRowsPerBlock)
                {
                    hashMap[r[colIdx] % num].push_back(r);
                }
                else
                {
                    string tname = t->tableName + "_" + to_string(r[colIdx] % num);
                    Page *pgHash = new Page(tname, buckets[r[colIdx] % num], hashMap[r[colIdx] % num], hashMap[r[colIdx] % num].size());
                    numRows[r[colIdx] % num].push_back(hashMap[r[colIdx] % num].size());
                    pgHash->writePage();
                    buckets[r[colIdx] % num]++;
                    hashMap[r[colIdx] % num].clear();
                }
            }
        }

        for (auto g : hashMap)
        {
            if (hashMap[g.first % num].size() != 0)
            {
                string tname = t->tableName + to_string(g.first % num);
                Page *pgHash = new Page(tname, buckets[g.first % num], hashMap[g.first % num], hashMap[g.first % num].size());
                numRows[g.first % num].push_back(hashMap[g.first % num].size());
                pgHash->writePage();
                buckets[g.first % num]++;
                hashMap[g.first % num].clear();
            }
        }
    }

    return pair<vector<int>, vector<vector<int>>>(buckets, numRows);
}

void partHashJoin(Table *t1, Table *t2, string col1, string col2, string resJoin, int buffSize)
{
    pair<vector<int>, vector<vector<int>>> t1Hash, t2Hash;
    vector<int> buck1, buck2;
    int sml, big;
    Table *smallTable;
    Table *bigTable;
    vector<vector<int>> smlRow, bigRow;
    int colIdxBig, colIdxSmall;
    int colIdx1 = t1->getColumnIndex(col1);
    int colIdx2 = t2->getColumnIndex(col2);
    t1Hash = partHash(t1, buffSize, colIdx1);
    t2Hash = partHash(t2, buffSize, colIdx2);

    buck1 = t1Hash.first;
    buck2 = t2Hash.first;
    vector<string> colName = t1->columns;
    colName.insert(colName.end(), t2->columns.begin(), t2->columns.end());
    // Creating new table for result
    Table *res = new Table(resJoin, colName);

    vector<vector<int>> data;
    int pgIdx = 0;
    for (int i = 0; i < (buffSize - 1); i++)
    {
        vector<int> bigBucket, smallBucket;
        if (buck1[i] > buck2[i])
        {
            bigBucket = buck1;
            bigTable = t1;
            colIdxBig = colIdx1;
            bigRow = t1Hash.second;
            big = 1;
            smallBucket = buck2;
            smallTable = t2;
            colIdxSmall = colIdx2;
            smlRow = t2Hash.second;
            sml = 2;
        }
        else
        {
            bigBucket = buck2;
            bigTable = t2;
            colIdxBig = colIdx2;
            bigRow = t2Hash.second;
            big = 2;
            smallBucket = buck1;
            smallTable = t1;
            colIdxSmall = colIdx1;
            smlRow = t1Hash.second;
            sml = 1;
        }

        vector<vector<int>> hashBuffPages;
        for (int j = 0; j < smallBucket[i]; j++)
        {
            string tnameSmall = smallTable->tableName + to_string(i);
            Page this_page = bufferManager.getHashPage(tnameSmall, j, smlRow[i][j], smallTable->columnCount);
            int row_num = this_page.numRows();
            for (int k = 0; k < row_num; k++)
            {
                hashBuffPages.push_back(this_page.getRow(k));
            }
        }

        // creating map of all the rows with key as attribute value
        map<int, vector<vector<int>>> attrMap = mapify(hashBuffPages, colIdxSmall);

        for (int y = 0; y < bigBucket[i]; y++)
        {
            string tnameBig = bigTable->tableName + to_string(i);
            Page tablePage = bufferManager.getHashPage(tnameBig, y, bigRow[i][y], bigTable->columnCount);
            int row_num = tablePage.numRows();
            // cout << "max rows per block = " << res->maxRowsPerBlock << endl;
            // Read the page row by row and join with corresponding rows in the map
            for (int k = 0; k < row_num; k++)
            {
                vector<int> r = tablePage.getRow(k);
                for (int z = 0; z < attrMap[r[colIdxBig]].size(); z++)
                {
                    // cout << "data vector size = " << data.size() << endl;
                    if (data.size() == res->maxRowsPerBlock)
                    {
                        Page *pgJoin = new Page(res->tableName, pgIdx, data, data.size());
                        pgJoin->writePage();
                        res->rowsPerBlockCount.push_back(data.size());
                        res->blockCount++;
                        res->rowCount += data.size();
                        data.clear();
                        pgIdx++;
                    }
                    {
                        if (sml == 1)
                        {
                            vector<int> temp2 = attrMap[r[colIdxBig]][z];
                            temp2.insert(temp2.end(), r.begin(), r.end());
                            data.push_back(temp2);
                        }
                        else
                        {
                            vector<int> temp2 = r;
                            temp2.insert(temp2.end(), attrMap[r[colIdxBig]][z].begin(), attrMap[r[colIdxBig]][z].end());
                            data.push_back(temp2);
                        }
                    }

                }
            }
        }

    }

    if (data.size() != 0)
    {
        Page *pgJoin = new Page(res->tableName, pgIdx, data, data.size());
        pgJoin->writePage();
        res->rowsPerBlockCount.push_back(data.size());
        res->blockCount++;
        res->rowCount += data.size();
        data.clear();
        pgIdx++;
    }

    tableCatalogue.insertTable(res);
    cout << "Number of Block Accesses: " << blockAcc << endl;
    blockAcc = 0;
    return;
}

void executeJOIN()
{
    logger.log("executeJOIN");
    blockAcc = 0;
    Table *t1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *t2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    string col1 = parsedQuery.joinFirstColumnName;
    string col2 = parsedQuery.joinSecondColumnName;
    int buffSize = parsedQuery.joinBuffer;
    auto binOp = parsedQuery.joinBinaryOperator;
    string resTable = parsedQuery.joinResultRelationName;
    if (parsedQuery.joinType == "NESTED")
    {
        nestedJoin(t1, t2, col1, col2, resTable, buffSize, binOp);
    }
    else if (parsedQuery.joinType == "PARTHASH")
    {
        partHashJoin(t1, t2, col1, col2, resTable, buffSize);
    }
    return;
}