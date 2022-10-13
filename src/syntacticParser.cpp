#include "global.h"

bool syntacticParse()
{
    logger.log("syntacticParse");
    string possibleQueryType = tokenizedQuery[0];
    // std::cout << tokenizedQuery[0] << endl;
    // std::cout << tokenizedQuery[1] << endl;
    // std::cout << tokenizedQuery[2] << endl;

    // std::cout << possibleQueryType << endl;

    if (tokenizedQuery.size() < 2)
    {
        cout << "flag 1" << endl;
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (possibleQueryType == "CLEAR")
        return syntacticParseCLEAR();
    else if (possibleQueryType == "INDEX")
        return syntacticParseINDEX();
    else if (possibleQueryType == "LIST")
        return syntacticParseLIST();
    else if (possibleQueryType == "LOAD" && tokenizedQuery[1] == "MATRIX")
    {
        // std::cout << "THIS WORKED!" << endl;
        return syntacticParseLOADMATRIX();
    }
    else if (possibleQueryType == "LOAD")
        return syntacticParseLOAD();
    else if (possibleQueryType == "PRINT" && tokenizedQuery[1] == "MATRIX")
        return syntacticParsePRINTMATRIX();
    else if (possibleQueryType == "PRINT")
        return syntacticParsePRINT();
    else if (possibleQueryType == "RENAME")
        return syntacticParseRENAME();
    else if (possibleQueryType == "EXPORT" && tokenizedQuery[1] == "MATRIX")
    {
        // std::cout << "EXPORT MATRIX" << endl;
        return syntacticParseEXPORTMATRIX();
    }
    else if(possibleQueryType == "EXPORT")
        return syntacticParseEXPORT();
    else if(possibleQueryType == "SOURCE")
        return syntacticParseSOURCE();
    else if (possibleQueryType == "CROSS_TRANSPOSE")
        return syntacticParseCROSSTRANSPOSE();
    else
    {
        string resultantRelationName = possibleQueryType;
        if (tokenizedQuery[1] != "<-" || tokenizedQuery.size() < 3)
        {
            cout << "flag 3" << endl;
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        possibleQueryType = tokenizedQuery[2];
        if (possibleQueryType == "PROJECT")
            return syntacticParsePROJECTION();
        else if (possibleQueryType == "SELECT")
            return syntacticParseSELECTION();
        else if (possibleQueryType == "JOIN")
            return syntacticParseJOIN();
        else if(possibleQueryType == "GROUP" && tokenizedQuery[3] == "BY")
            return syntacticParseGROUPBY();
        else if (possibleQueryType == "CROSS")
            return syntacticParseCROSS();
        else if (possibleQueryType == "DISTINCT")
            return syntacticParseDISTINCT();
        else if (possibleQueryType == "SORT")
            return syntacticParseSORT();
        else
        {
            cout << "flag 4" << endl;
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    return false;
}

ParsedQuery::ParsedQuery()
{
}

void ParsedQuery::clear()
{
    logger.log("ParseQuery::clear");
    this->queryType = UNDETERMINED;

    this->clearRelationName = "";

    this->crossResultRelationName = "";
    this->crossFirstRelationName = "";
    this->crossSecondRelationName = "";

    this->distinctResultRelationName = "";
    this->distinctRelationName = "";

    this->exportRelationName = "";

    this->indexingStrategy = NOTHING;
    this->indexColumnName = "";
    this->indexRelationName = "";

    this->joinBinaryOperator = NO_BINOP_CLAUSE;
    this->joinResultRelationName = "";
    this->joinFirstRelationName = "";
    this->joinSecondRelationName = "";
    this->joinFirstColumnName = "";
    this->joinSecondColumnName = "";
    this->joinType = "";
    this->joinBuffer = 0;

    this->groupbyAttribute = "";
    this->groupbyRelationName = "";
    this->groupbyReturn = "";
    this->groupbyResTable = "";

    this->loadRelationName = "";

    this->printRelationName = "";

    this->projectionResultRelationName = "";
    this->projectionColumnList.clear();
    this->projectionRelationName = "";

    this->renameFromColumnName = "";
    this->renameToColumnName = "";
    this->renameRelationName = "";

    this->selectType = NO_SELECT_CLAUSE;
    this->selectionBinaryOperator = NO_BINOP_CLAUSE;
    this->selectionResultRelationName = "";
    this->selectionRelationName = "";
    this->selectionFirstColumnName = "";
    this->selectionSecondColumnName = "";
    this->selectionIntLiteral = 0;

    this->sortingStrategy = NO_SORT_CLAUSE;
    this->sortResultRelationName = "";
    this->sortColumnName = "";
    this->sortRelationName = "";

    this->sourceFileName = "";
}

/**
 * @brief Checks to see if source file exists. Called when LOAD command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isFileExists(string relationName)
{
    string fileName = "../data/" + relationName + ".csv";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

/**
 * @brief Checks to see if source file exists. Called when SOURCE command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isQueryFile(string fileName){
    fileName = "../data/" + fileName + ".ra";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

const std::string whitespace = " \n\r\t";

std::string syn_ltrim(const std::string &s)
{
    size_t start = s.find_first_not_of(whitespace);
    if(start == std::string::npos)
        return "";
    return s.substr(start);
}
 
std::string syn_rtrim(const std::string &s)
{
    size_t end = s.find_last_not_of(whitespace);
    if(end == std::string::npos)
        return "";
    return s.substr(0, end+1);
}
 
std::string syn_trim(const std::string &s) {
    return syn_rtrim(syn_ltrim(s));
}


/**
 * @brief Check if a file is in matrix form.
 * Checks if the first entry is an integer
 */
bool isMatrix(string matrixName)
{
    string firstEntry;
    string fileName = "../data/" + matrixName + ".csv";
    ifstream fin(fileName, ios::in);
    getline(fin, firstEntry, ',');
    firstEntry = syn_trim(firstEntry);

    try{
        stoi(firstEntry);
        return true;
    }
    catch (exception e){
        return false;
    }
}