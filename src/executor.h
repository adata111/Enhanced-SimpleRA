#include"semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeEXPORTMATRIX();
void executeINDEX();
void executeJOIN();
void executeGROUPBY();
void executeLIST();
void executeLOAD();
void executeLOADMATRIX();
void executePRINT();
void executePRINTMATRIX();
void executeCROSSTRANSPOSE();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeUPDATION();
void executeSOURCE();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);