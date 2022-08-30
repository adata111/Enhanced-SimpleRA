#include "global.h"

MatrixBufferManager::MatrixBufferManager()
{
    logger.log("MatrixBufferManager::MatrixBufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param MatrixName 
 * @param pageIndex 
 * @return MatrixPage 
 */

MatrixPage MatrixBufferManager::getMatrixPage(string matrixName, int rowIndex, int colIndex) {
    logger.log("MatrixBufferManager::getMatrixPage");

    string pageName = "../data/temp/" + matrixName + "_Page_" + to_string(rowIndex) + "_" + to_string(colIndex);
    // cout<<pageName<<endl;
    if (this->inPool(pageName)) {
        return this->getFromPool(pageName);
    } else {
        return this->insertIntoPool(matrixName, rowIndex, colIndex);
    }
}
/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName 
 * @return true 
 * @return false 
 */
bool MatrixBufferManager::inPool(string pageName)
{
    logger.log("MatrixBufferManager::inPool");
    for (auto page : this->pages)
    {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName 
 * @return MatrixPage 
 */
MatrixPage MatrixBufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}

MatrixPage MatrixBufferManager::insertIntoPool(string matrixName, int rowIndex, int colIndex) {
    logger.log("MatrixBufferManager::insertIntoPool");
    
    MatrixPage page(matrixName, rowIndex, colIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        this->pages.pop_front();
    this->pages.push_back(page);
    return page;
}

void MatrixBufferManager::writeMatrixPage(string matrixName, int rowIndex, int colIndex, vector<vector<int>> data) {
    logger.log("MatrixBufferManager::writeMatrixPage");
    MatrixPage page(matrixName, rowIndex, colIndex, data);

    // update data in page stored in buffer manager by matching pageName
    for(auto &p:this->pages)
    {
        if(p.pageName == page.pageName)
        {
            p = page;
            break;
        }
    }

    page.writeMatrixPage();
}
/**
 * @brief Deletes file names fileName
 *
 * @param fileName 
 */
void MatrixBufferManager::deleteFile(string fileName)
{
    
    if (remove(fileName.c_str()))
        logger.log("BufferManagerMatrix::deleteFile: Err");
    else logger.log("BufferManagerMatrix::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the MatrixName and pageIndex.
 *
 * @param matrixName 
 * @param pageIndex 
 */
void MatrixBufferManager::deleteFile(string matrixName, int rowIndex, int colIndex) {
    logger.log("MatrixBufferManager::deleteFile");

    string fileName = "../data/temp/" + matrixName + "_Page_" + to_string(rowIndex) + "_" + to_string(colIndex);
    this->deleteFile(fileName);
} 