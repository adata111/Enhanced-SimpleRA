#ifndef __MATRIXPAGE_H
#define __MATRIXPAGE_H
#include"logger.h"
/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files. 
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs. 
 *</p>
 */

class MatrixPage{

    string matrixName;
    int rowIndex;
    int colIndex;
    vector<vector<int>> matrix;

    public:

    string pageName = "";
    MatrixPage();
    MatrixPage(string matrixName, int rowIndex, int colIndex);
    MatrixPage(string matrixName, int rowIndex, int colIndex, vector<vector<int>> data);
    vector<vector<int>> getMatrix();
    void writeMatrixPage();
};

#endif