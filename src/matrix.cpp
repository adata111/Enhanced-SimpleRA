#include "global.h"
#include "matrix.h"

const std::string whitespace = " \n\r\t";

std::string ltrim(const std::string &s)
{
    size_t start = s.find_first_not_of(whitespace);
    if(start == std::string::npos)
        return "";
    return s.substr(start);
}
 
std::string rtrim(const std::string &s)
{
    size_t end = s.find_last_not_of(whitespace);
    if(end == std::string::npos)
        return "";
    return s.substr(0, end+1);
}
 
std::string trim(const std::string &s) {
    return rtrim(ltrim(s));
}

Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
}

Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

bool Matrix::load()
{
    logger.log("Matrix::load");
    // std::cout << "Reached load matrix" << endl;
    return this->blockify();
}

vector<int> Matrix::getSubMatRow(string line, int colNo)
{

    stringstream s(line);
    string word;
    vector<int> out;
    int check;
    for (int j=0; j < min(this->dimension, (long long int)(colNo + 1) * MATRIX_DIM); j++)
    {
        
        if (!getline(s, word, ','))
        {
            // cout<<"enough elements not read\n";
            return vector<int>(0);
        }
        if(j >= colNo * MATRIX_DIM){
            
            word = trim(word);
            // std::cout << "word -> ";
            // std::cout << word << endl;
            out.push_back(stoi(word));
            // std::cout << "stoi worked" << endl;
        }
    }

    while (out.size() < MATRIX_DIM){
        out.push_back(-1);
    }

    return out;
}

void Matrix::writeLineToTerminal(vector<int> line)
{
    logger.log("Matrix::writeLineToTerminal");

    for (long long int j = 0; j < line.size() - 1; j++)
    {
        std::cout << line[j] << ", ";
    }
    std::cout << line.back();
    std::cout << endl;
}

int Matrix::getNumCols(string line)
{
    int count = 0;
    for (auto &ch : line){
        if (ch == ','){count++;}
    }

    // if it is "," it would have already been counted
    if (line.back() != ','){count++;}
    return count;
}

bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    // std::cout << "Reached blockify matrix" << endl;

    ifstream fin(this->sourceFileName, ios::in);

    if (fin.peek() == ifstream::traits_type::eof()){
        cout << "ERROR: Input file is empty." << endl;
        return false;
    }

    string firstline;
    getline(fin, firstline);
    int dim = getNumCols(firstline);

    this->dimension = dim;
    this->blockCount = ceil((double)this->dimension / (double)MATRIX_DIM);

    fin.close();

    return this->writeBlock();
}

bool Matrix::writeBlock()
{
    int bj = 0;
    int block_num = this->blockCount;
    int rows = 0;

    while (bj < block_num){
        ifstream fin(this->sourceFileName, ios::in);
        vector<vector<int>> curr_pg(MATRIX_DIM, vector<int>(MATRIX_DIM, -1));
        int block_itr = 0, bi = 0;
        int rows = 0;
        string l;
        while (getline(fin, l))
        {
            // cout<<l<<endl;
            if(bj==0){
                rows++;
                int col_cnt = getNumCols(l);
                if(col_cnt != this->dimension){
                    cout<<"SEMANTIC ERROR: Not a square matrix."<<endl;
                    return false;
                }
            }
            if (block_itr < MATRIX_DIM){
                curr_pg[block_itr] = this->getSubMatRow(l, bj);
                block_itr++;
            }
            else{
                matrixBufferManager.writeMatrixPage(this->matrixName, bi, bj, curr_pg);
                bi++;
                curr_pg.assign(MATRIX_DIM, vector<int>(MATRIX_DIM, -1));
                curr_pg[0] = this->getSubMatRow(l, bj);
                block_itr = 1;
            }
        }
        // cout<<rows<<" <<<<<\n";
        if(rows!=this->dimension && bj==0){
            cout<<"SEMANTIC ERROR: Not a square matrix."<<endl;
            return false;
        }
        matrixBufferManager.writeMatrixPage(this->matrixName, bi, bj, curr_pg);
        bj++;
        fin.close();
    }
    return true;
}


void Matrix::makePermanent()
{
    logger.log("Matrix::makePermanent");
    if (!isPermanent())
        matrixBufferManager.deleteFile(this->sourceFileName);
    ofstream fout(this->sourceFileName, ios::trunc);
    vector<vector<int>> curr_mat;
    long long int i, j;
    for (i = 0; i < this->dimension; i++)
    {
        int bi = i / MATRIX_DIM;
        int bl_line = i % MATRIX_DIM;
        int bj = 0;
        j = 0;

        vector<int> line;

        for (bj = 0; bj < this->blockCount; bj++)
        {
            MatrixPage curr_pg = matrixBufferManager.getMatrixPage(this->matrixName, bi, bj);
            curr_mat = curr_pg.getMatrix();
            if(bj==this->blockCount-1)
            {
                // for(auto it = curr_mat[bl_line].begin(); it!=curr_mat[bl_line].begin() + this->dimension - j; it++)
                //     line.push_back(*it);
                line.insert(line.end(), curr_mat[bl_line].begin(), curr_mat[bl_line].begin() + this->dimension - j);
            }
            else
            {

                // for(auto it = curr_mat[bl_line].begin(); it!=curr_mat[bl_line].end(); it++)
                //     line.push_back(*it); 
                // line.emplace_back(vector<int>(curr_mat[bl_line].begin(), curr_mat[bl_line].end()));
                line.insert(line.end(), curr_mat[bl_line].begin(), curr_mat[bl_line].end());
                j += MATRIX_DIM;
            }
        }
        
        // writing the matrix in a file
        for (int k = 0; k < line.size() - 1; k++)
        {
            fout << line[k] << ", ";
        }
        fout << line.back() << endl;
    }
    fout.close();
}

/**
 * @brief Function to check if matrix is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Matrix::isPermanent()
{
    logger.log("Matrix::isPermanent");
    if (this->sourceFileName == "../data/" + this->matrixName + ".csv")
        return true;
    return false;
}

/**
 * @brief Function prints the first 20X20 submatrix of the loaded matrix. If the matrix is smaller
 * than 20X20, the whole matrix is printed, else the first 20X20 submatrix is printed.
 */
void Matrix::print()
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->dimension);

    vector<vector<int>> this_page = matrixBufferManager.getMatrixPage(this->matrixName, 0, 0).getMatrix();
    // cout << "Matrix::print"<< endl;
    // cout << this->matrixName << endl; 
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row.insert(row.end(), this_page[rowCounter].begin(), this_page[rowCounter].begin() + count);
        this->writeLineToTerminal(row);
        row.clear();
        // std::cout << endl;
    }
    // this->writeLineToTerminal(row);
    printRowCount(this->dimension);
}

vector<vector<int>> Matrix::interTrans(vector<vector<int>> mat)
{
    for (int i = 0; i < MATRIX_DIM; i++)
        for (int j = i + 1; j < MATRIX_DIM; j++)
        {
            swap(mat[i][j], mat[j][i]);
        }
    return mat;
}

void Matrix::transpose()
{
    for (int rowIndex = 0; rowIndex < this->blockCount; rowIndex++)
    {
        for (int colIndex = rowIndex; colIndex < this->blockCount; colIndex++)
        {
            if (rowIndex != colIndex)
            {
                MatrixPage pg_ij = matrixBufferManager.getMatrixPage(this->matrixName, rowIndex, colIndex);
                vector<vector<int>> mat_ij = pg_ij.getMatrix();

                MatrixPage pg_ji = matrixBufferManager.getMatrixPage(this->matrixName, colIndex, rowIndex);
                vector<vector<int>> mat_ji = pg_ji.getMatrix();

                vector<vector<int>> trans_ij = interTrans(mat_ij);
                vector<vector<int>> trans_ji = interTrans(mat_ji);

                matrixBufferManager.writeMatrixPage(this->matrixName, rowIndex, colIndex, trans_ji);
                matrixBufferManager.writeMatrixPage(this->matrixName, colIndex, rowIndex, trans_ij);
            }

            else
            {
                MatrixPage mat_pg = matrixBufferManager.getMatrixPage(this->matrixName, rowIndex, colIndex);
                vector<vector<int>> mat = mat_pg.getMatrix();

                vector<vector<int>> mat_trans = interTrans(mat);

                matrixBufferManager.writeMatrixPage(this->matrixName, rowIndex, colIndex, mat_trans);
            }
        }
    }
}

void Matrix::unload() {
    for (long long int i = 0; i < this->blockCount; i++) {
        for (long long int j = 0; j < this->blockCount; j++) {
            matrixBufferManager.deleteFile(this->matrixName, i, j);
        }
    }
    if (!isPermanent())
        matrixBufferManager.deleteFile(this->sourceFileName);
}