#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    bufferManager.clearPool();
    while(lockingManager.lockfile_getStatus(parsedQuery.printRelationName) == 2)
    {
        cout<<"Table updating. Print blocked."<<endl;
    }

    lockingManager.lockfile_changeStatus(parsedQuery.printRelationName,1);
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);
    table->print();
    lockingManager.lockfile_changeStatus(parsedQuery.printRelationName,0);
    return;
}
