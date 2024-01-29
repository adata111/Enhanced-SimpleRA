#include "global.h"
/**
 * @brief 
 * SYNTAX: UPDATE <table_name> COLUMN <column_name> <OPERATOR> <value>
 */
bool syntacticParseUPDATION()
{
    logger.log("syntacticParseUPDATION");
    if (tokenizedQuery.size() != 6 || tokenizedQuery[2] != "COLUMN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = UPDATION;
    parsedQuery.updationTableName = tokenizedQuery[1];
    parsedQuery.updationColumnName = tokenizedQuery[3];
    parsedQuery.updationOperator = tokenizedQuery[4];
    parsedQuery.updationValue = tokenizedQuery[5];

    string binaryOperator = tokenizedQuery[4];
    if (binaryOperator == "MULTIPLY" ||binaryOperator == "ADD"|| binaryOperator == "SUBTRACT" );
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseUPDATION()
{
    logger.log("semanticParseUPDATION");

    if (!tableCatalogue.isTable(parsedQuery.updationTableName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.updationColumnName, parsedQuery.updationTableName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

int getUpdatedValue(int prev_val,int value)
{
    if(parsedQuery.updationOperator=="MULTIPLY")
        prev_val*=value;
    else if(parsedQuery.updationOperator=="ADD")
        prev_val+=value;
    else if(parsedQuery.updationOperator=="SUBTRACT")
        prev_val-=value;

    return prev_val;
}

void executeUPDATION()
{
    logger.log("executeUPDATION");
    string tname = parsedQuery.updationTableName;
    string colname = parsedQuery.updationColumnName;

    bufferManager.clearPool();
    
    while(lockingManager.lockfile_getStatus(tname) != 0)
    {
        cout << "Updation for "<< tname <<" Blocked" << endl;
    }
    
    // Locking the table for updation
    cout << endl;
    cout << "**********************************************\n";
    cout << "Updating...\n";
    lockingManager.lockfile_changeStatus(tname,2);
    Table* t1 = tableCatalogue.getTable(tname);
    int col_ind = t1->getColumnIndex(colname);
    Page pg = bufferManager.getPage(t1->tableName,0);
    int rowCnt = pg.numRows();

    vector<vector<int>> pg_rows = pg.allRows();
    
    for(int row_ind=0; row_ind<rowCnt; row_ind++)
    {
        int updated_val = getUpdatedValue(pg_rows[row_ind][col_ind], stoi(parsedQuery.updationValue));
        pg.updateRowIndex(row_ind, col_ind, updated_val);
        // sleep(1); // to show parallelism
    }

    pg.writePage();
    bufferManager.clearPool();
    t1->makePermanent();
    // Table is now available for further transactions
    lockingManager.lockfile_changeStatus(tname,0);
    
    return;
}
