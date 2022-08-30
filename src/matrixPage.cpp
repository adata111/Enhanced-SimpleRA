#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
MatrixPage::MatrixPage()
{
    this->matrixName = "";
    this->rowIndex = 0;
    this->colIndex = 0;
    this->pageName = "";
    this->matrix.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When matrices are loaded they are broken up into sub-matrices of 
 * MATRIX_DIM dimension and each block is stored in a different file named
 * "<matrixName>_Page_<rowindex>_<colIndex>". For example, If the Page being
 * loaded is of matrix "R" and the pageIndex is 2 then the file name is "R_Page2".
 * The page loads the rows (or tuples) into a vector of rows (where each row 
 * is a vector of integers).
 *
 * @param matrixName 
 * @param pageIndex 
 */

MatrixPage::MatrixPage(string matrixName, int rowIndex, int colIndex) {
    logger.log("MatrixPage::MatrixPage");
    
    this->matrixName = matrixName;
    this->rowIndex = rowIndex;
    this->colIndex = colIndex;
    this->pageName = "../data/temp/" + this->matrixName + "_Page_" + to_string(this->rowIndex) + "_" + to_string(this->colIndex);

    this->matrix.resize(MATRIX_DIM);
    fill(this->matrix.begin(), this->matrix.end(), vector<int> (MATRIX_DIM, -1));

    ifstream fin(this->pageName, ios::in);
    for (int i = 0; i < MATRIX_DIM; i++) {
        for (int j = 0; j < MATRIX_DIM; j++) {
            int temp;
            fin >> temp;
            this->matrix[i][j] = temp; // -1's are also filled
        }
    }
}

MatrixPage::MatrixPage(string matrixName, int rowIndex, int colIndex, vector<vector<int>> data) {
    logger.log("MatrixPage::MatrixPage");
    
    this->matrixName = matrixName;
    this->rowIndex = rowIndex;
    this->colIndex = colIndex;
    this->pageName = "../data/temp/" + this->matrixName + "_Page_" + to_string(this->rowIndex) + "_" + to_string(this->colIndex);
    
    this->matrix = data;
}

vector<vector<int>> MatrixPage::getMatrix() {
    logger.log("MatrixPage::getMatrix");
    return this->matrix;
}

void MatrixPage::writeMatrixPage() {
    // cout<<this->pageName<<endl;

    ofstream fout(this->pageName, ios::trunc);
    for (int i = 0; i < MATRIX_DIM; i++) {
        for (int j = 0; j < MATRIX_DIM; j++) {
            fout << this->matrix[i][j];
            if (j != MATRIX_DIM - 1) {
                fout << " ";
            }
        }
        fout << endl;
    }
    fout.close();
}