#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * <new_table_name> <- SORT <table_name> BY <column_name> IN ASC | DESC BUFFER <buffer_size>
 *
 * sorting_order = ASC | DESC
 */
int columnIndex;
bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if ((tokenizedQuery.size() != 10 && tokenizedQuery.size() != 8) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    string sortingStrategy = tokenizedQuery[7];
    if (tokenizedQuery.size() == 10)
    {
        parsedQuery.sortBufferSize = stoi(tokenizedQuery[9]);
    }
    else
    {
        parsedQuery.sortBufferSize = 10;
    }
    if (sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if (sortingStrategy == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (tableCatalogue.isTable(parsedQuery.sortResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

bool sortAsc(const vector<int> &row1, const vector<int> &row2)
{
    return row1[columnIndex] < row2[columnIndex];
}

bool sortAsc1(const pair<int, int> &row1, const pair<int, int> &row2)
{
    return row1.second < row2.second;
}

bool sortDesc(const vector<int> &row1, const vector<int> &row2)
{
    return row1[columnIndex] > row2[columnIndex];
}

bool sortDesc1(const pair<int, int> &row1, const pair<int, int> &row2)
{
    return row1.second > row2.second;
}

vector<int> sortInternal(Table t1, int colIdx, int buffSize, SortingStrategy sortType)
{
    vector<int> rowCnt;
    for (int i = 0; i < t1.blockCount; i += buffSize)
    {
        // Reading buffsize pages at a time
        vector<vector<int>> sortBuffPages;
        for (int j = 0; (j < buffSize) && ((i + j) < t1.blockCount); j++)
        {
            Page this_page = bufferManager.getPage(t1.tableName, i + j);
            int row_num = this_page.numRows();
            for (int k = 0; k < row_num; k++)
            {
                vector<int> tempVec = this_page.getRow(k);
                sortBuffPages.push_back(tempVec);
            }

            // Sorting the vector of vectors
            if (sortType == ASC)
            {
                sort(sortBuffPages.begin(), sortBuffPages.end(), sortAsc);
            }
            else
            {
                sort(sortBuffPages.begin(), sortBuffPages.end(), sortDesc);
            }

            string tname = t1.tableName + "_0";
            rowCnt.push_back(sortBuffPages.size());
            Page *pgSort = new Page(tname, i + j, sortBuffPages, sortBuffPages.size());
            pgSort->writePage();

            sortBuffPages.clear();
        }
    }

    return rowCnt;
}

void executeSORT()
{
    logger.log("executeSORT");
    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table *resTable = new Table(parsedQuery.sortResultRelationName, table.columns);
    columnIndex = table.getColumnIndex(parsedQuery.sortColumnName);
    SortingStrategy sortStr = parsedQuery.sortingStrategy;
    int buffSize = parsedQuery.sortBufferSize;
    vector<int> rowCnt = sortInternal(table, columnIndex, buffSize, sortStr);

    int pgsetNum = 0;
    int sortBuffSize = buffSize - 1; // nb - 1
    int sortRound = 1;
    int readLimit = 1;
    int blkCnt = table.blockCount;

    // Till the number of pagesets is not 1
    while (pgsetNum != 1)
    {
    	/*
        reading nb-1 pagesets (for round 1, each pageset will have just 1 page)
        if nb-1 = 3,
        then we are reading {(1,2,3)(4,5,6)(7,8,9)},{(10,11,12)(13,14,15)(16,17,18)} at a time.
        read first rows of first pages of each pageset in the overall set that is being considered together
        for {(1,2,3)(4,5,6)(7,8,9)} set, the loop reads first rows of pg 1,4 and 7 which are alloted page indices 0,1 and 2 and then breaks
        */

        // iterating over sets of sortBuffSize number of pagesets

        pgsetNum = 0;
        int pgwriteNum = 0;
        for (int pgsetIdx = 0; pgsetIdx < blkCnt; pgsetIdx += pow(sortBuffSize, sortRound))
        {
            pgsetNum += 1;
            vector<Page> buffPages;
            vector<vector<int>> writeRows;
            int readRowIdx[sortBuffSize + 10];   // latest row index that has been read in a page
            int readPgsetIdx[sortBuffSize + 10]; // the index of the page being read with respect to the pageset
            int pgRowNum[sortBuffSize + 10];     // number of rows in a page

            int pgReadNum = 0;
            int k = 0;
            int pgLoaded[sortBuffSize + 10];

            vector<pair<int, int>> sortVec;

            // read first rows of first pages of each pageset in the overall set that is being considered together
            for (int pgLoadIdx = pgsetIdx; pgReadNum < sortBuffSize && pgLoadIdx < blkCnt; pgLoadIdx+=readLimit)
            {
                string tname = table.tableName + "_" + to_string(sortRound - 1);
                buffPages.push_back(bufferManager.getSortPage(tname, pgLoadIdx, rowCnt[pgLoadIdx], table.columnCount));
                pgRowNum[k] = rowCnt[pgLoadIdx];

                readPgsetIdx[k] = 0;
                readRowIdx[k] = 0;

                pgReadNum += 1;
                pgLoaded[k] = pgLoadIdx;
                k++;
                vector<int> firstRow = buffPages[buffPages.size() - 1].getRow(0);
                sortVec.push_back(make_pair(buffPages.size() - 1, firstRow[columnIndex]));
            }

            while (sortVec.size() > 0)
            {
                if (sortStr == ASC)
                {
                    sort(sortVec.begin(), sortVec.end(), sortAsc1);
                }
                else
                {
                    sort(sortVec.begin(), sortVec.end(), sortDesc1);
                }

                int pgIdx = sortVec[0].first;
                sortVec.erase(sortVec.begin());

                vector<int> writerow = buffPages[pgIdx].getRow(readRowIdx[pgIdx]);

                writeRows.push_back(writerow);

                if (writeRows.size() >= table.maxRowsPerBlock)
                {
                    string tname = table.tableName + "_" + to_string(sortRound);
                    Page *pgSort = new Page(tname, pgwriteNum, writeRows, writeRows.size());
                    pgSort->writePage();
                    pgwriteNum += 1;
                    writeRows.clear();
                }

                readRowIdx[pgIdx]++;
                // if the current page does not have further rows to be read
                if (pgRowNum[pgIdx] <= readRowIdx[pgIdx])
                {
                    readPgsetIdx[pgIdx]++;
                    // if next page from the same pageset can be read
                    if (readPgsetIdx[pgIdx] < readLimit && (pgLoaded[pgIdx] + readPgsetIdx[pgIdx]) < blkCnt)
                    {
                        // read the next page
                        int loadIdx = pgLoaded[pgIdx] + readPgsetIdx[pgIdx];
                        string tname = table.tableName + "_" + to_string(sortRound - 1);
                        buffPages[pgIdx] = bufferManager.getSortPage(tname, loadIdx, rowCnt[loadIdx], table.columnCount);
                        readRowIdx[pgIdx] = 0;
                        pgRowNum[pgIdx] = rowCnt[loadIdx];

                        // read the first row from the next page
                        vector<int> firstRow = buffPages[pgIdx].getRow(0);
                        sortVec.push_back(make_pair(pgIdx, firstRow[columnIndex]));
                    }
                }
                // Reading next row of the current page
                else
                {
                    vector<int> nxtRow = buffPages[pgIdx].getRow(readRowIdx[pgIdx]);
                    sortVec.push_back(make_pair(pgIdx, nxtRow[columnIndex]));
                }
            }

            // if some rows are left unwritten
            if (writeRows.size() > 0)
            {
                string tname = table.tableName + "_" + to_string(sortRound);
                Page *pgSort = new Page(tname, pgwriteNum, writeRows, writeRows.size());
                pgSort->writePage();
                pgwriteNum += 1;
                writeRows.clear();
            }
        }
        readLimit = pow(sortBuffSize, sortRound);
        sortRound++;
    }

    for (int i = 0; i < blkCnt; i++)
    {
        string tname = table.tableName + "_" + to_string(sortRound - 1);
        Page pg = bufferManager.getSortPage(tname, i, rowCnt[i], table.columnCount);
        vector<vector<int>> res = pg.allRows();
        Page *finalPg = new Page(resTable->tableName, i, res, res.size());
        finalPg->writePage();
        resTable->rowsPerBlockCount.push_back(res.size());
        resTable->rowCount += res.size();
        resTable->blockCount++;
    }

    if (resTable->rowCount > 0)
    {
        tableCatalogue.insertTable(resTable);
    }

    return;
}