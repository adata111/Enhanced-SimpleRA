#ifndef __MATRIX_H
#define __MATRIX_H

#include "matrixBufferManager.h"

/**
 * @brief The Matrix class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT). 
 *
 */
class Matrix {
   private:
    int getNumCols(string line);
    vector<int> getSubMatRow(string line, int colNo);
    void writeLineToTerminal(vector<int> line);
    bool writeBlock();
    vector<vector<int>> interTrans(vector<vector<int>> mat);

   public:
    string sourceFileName = "";
    string matrixName = "";
    long long int dimension; // gauranteed to be square
    long long int blockCount; // along one dimension, actually blockCount^2 blocks

    bool blockify();
    Matrix();
    Matrix(string matrixName);
    
    bool load();
    void print();
    void makePermanent();
    bool isPermanent();
    void unload();
    void transpose();
};

#endif