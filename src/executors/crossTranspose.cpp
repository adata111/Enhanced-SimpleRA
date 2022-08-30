#include "global.h"
/**
 * @brief
 * SYNTAX: CROSS_TRANSPOSE matrix1_name matrix2_name
 */
bool syntacticParseCROSSTRANSPOSE()
{
    logger.log("syntacticParseCROSSTRANSPOSE");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CROSSTRANSPOSE;
    parsedQuery.CrossTransposeMatrixOne = tokenizedQuery[1];
    parsedQuery.CrossTransposeMatrixTwo = tokenizedQuery[2];
    return true;
}

bool semanticParseCROSSTRANSPOSE()
{
    logger.log("semanticParseCROSSTRANSPOSE");
    if (!matrixCatalogue.isMatrix(parsedQuery.CrossTransposeMatrixOne))
    {
        cout << "SEMANTIC ERROR: First matrix doesn't exist" << endl;
        return false;
    }

    if (!matrixCatalogue.isMatrix(parsedQuery.CrossTransposeMatrixTwo))
    {
        cout << "SEMANTIC ERROR: Second matrix doesn't exist" << endl;
        return false;
    }

    return true;
}

void executeCROSSTRANSPOSE()
{
    logger.log("executeCROSSTRANSPOSE");
    if(parsedQuery.CrossTransposeMatrixOne == parsedQuery.CrossTransposeMatrixTwo){
        Matrix *matrixOne = matrixCatalogue.getMatrix(parsedQuery.CrossTransposeMatrixOne);
        matrixOne->transpose();
        return;
    }
    Matrix *matrixOne = matrixCatalogue.getMatrix(parsedQuery.CrossTransposeMatrixOne);
    Matrix *matrixTwo = matrixCatalogue.getMatrix(parsedQuery.CrossTransposeMatrixTwo);
    if(matrixOne->dimension != matrixTwo->dimension){
        cout << "ERROR: First and second matrices have different dimensions" << endl;
        return;
    }
    // Transposing the inputted matrices
    matrixOne->transpose();
    matrixTwo->transpose();

    // Inplace replacement
    for (int rowIndex = 0; rowIndex < matrixTwo->blockCount; rowIndex++)
    {
        for (int colIndex = 0; colIndex < matrixTwo->blockCount; colIndex++)
        {
            MatrixPage matOneIJPage = matrixBufferManager.getMatrixPage(matrixOne->matrixName, rowIndex, colIndex);
            vector<vector<int>> matOneIJ = matOneIJPage.getMatrix();

            MatrixPage matTwoIJPage = matrixBufferManager.getMatrixPage(matrixTwo->matrixName, rowIndex, colIndex);
            // vector<vector<int>> matTwoIJ = matTwoIJPage.getMatrix();

            matrixBufferManager.writeMatrixPage(matrixOne->matrixName, rowIndex, colIndex, matTwoIJPage.getMatrix());
            matrixBufferManager.writeMatrixPage(matrixTwo->matrixName, rowIndex, colIndex, matOneIJ);
        }
    }
}