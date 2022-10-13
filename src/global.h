#ifndef __GLOBAL_H
#define __GLOBAL_H
#include"executor.h"

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern int MATRIX_DIM;
extern int blockAcc;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern BufferManager bufferManager;
extern MatrixBufferManager matrixBufferManager;
extern TableCatalogue tableCatalogue;
extern MatrixCatalogue matrixCatalogue;

#endif