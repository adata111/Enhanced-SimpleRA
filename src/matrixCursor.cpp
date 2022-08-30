// #include "global.h"

// MatrixCursor::MatrixCursor(string matrixName, int pageIndex)
// {
//     logger.log("Cursor::Cursor");
//     this->page = bufferManager.getPage(matrixName, pageIndex);
//     this->pagePointer = 0;
//     this->matrixName = matrixName;
//     this->pageIndex = pageIndex;
// }
 
// /**
//  * @brief This function reads the next row from the page. The index of the
//  * current row read from the page is indicated by the pagePointer(points to row
//  * in page the cursor is pointing to).
//  *
//  * @return vector<int> 
//  */
// vector<int> Cursor::getNext()
// {
//     logger.log("Cursor::geNext");
//     vector<int> result = this->page.getRow(this->pagePointer);
//     this->pagePointer++;
//     if(result.empty()){
//         tableCatalogue.getMatrix(this->matrixName)->getNextPage(this);
//         if(!this->pagePointer){
//             result = this->page.getRow(this->pagePointer);
//             this->pagePointer++;
//         }
//     }
//     return result;
// }
// /**
//  * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
//  * reading from the new page.
//  *
//  * @param pageIndex 
//  */
// void Cursor::nextPage(int pageIndex)
// {
//     logger.log("Cursor::nextPage");
//     this->page = bufferManager.getPage(this->matrixName, pageIndex);
//     this->pageIndex = pageIndex;
//     this->pagePointer = 0;
// }