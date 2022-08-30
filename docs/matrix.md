# Report

## Part 2

#### Page layout:

We cannot store huge matrices in a single page because of storage limitation (a single page can hold 8 kilo bytes of data). 
Thus we split the input matrix into smaller submatrices which can fit in a single page. Since the capacity of a page is 8 KB and we are storing integer values, the number of elements that can fit in a page are -
(block_size * 1024) / sizeof(int) = 2048 as block_size is 8 and size of int data type is 4. Hence, the number of elements that can fit in a page is 2048 and the size of square matrix that can be stored in a page is 
int(sqrt(2048)) = 45 = MATRIX_DIM
Thus each page stores a submatrix of dimension 45 x 45. In case all the positions in a block are not filled i.e. MATRIX_DIM > remaining sub-matrix dimension, then the vacant places are filled with -1.
This page layout is chosen to make transpose effective since we have to implement cross-transpose later. This implementation reduces the number of page accesses as opposed to storing the data row-wise.

#### Procedure for cross transpose:

Cross transpose of A and B matrices refers to transposing the matrices individually and then storing respective transpose in the other matrix (A transpose is stored in B and viceversa) where we can only use limited memory for swapping the pages. Because of memory storage limitations, we can not access all the elements of the matrix together and hence using standard procedure of matrix transpose (swapping element at (i,j) and element at (j,i)) is not possible. To solve these problems we consider a huge matrix as a grid of small submatrices and look at these submatrices individually.

Considering (i,j) to be the indices of a value in the input matrix, its corresponding page index would be (i/45,j/45) since the size of matrix fitting in a page is 45 x 45 (the denominator value in (i/45,j/45) will change depending on the capacity of each block). For computing the cross transpose, we start by internally transposing the two blocks (i/45,j/45) and (j/45,i/45). After transposing, these two blocks are swapped. If i/45 = j/45, the block is just internally transposed. Performing this procedure for all the submatrices of the matrix, we obtain the transpose of the matrix stored in itself i.e. now A contains A transpose and B contains B transpose.

Now that we have obtained the transpose of both the matrix A and B, we use a blockwise approach for swapping the elements of these 2 matrices. Consider the block (submatrix) labelled (bi,bj) in A and B. With the help of a temporary vector of vectors (of data type int) we swap the elements of matrix A and matrix B block by block. After swapping all the submatrix blocks, we have successfully swapped the elements of the two matrices and thus we have B^T stored in what was originally A matrix and A transpose stored in what was originally B matrix.


## Part 3
#### Page layout:

For using the storage space optimally, we will be storing only the non-zero elements of the given sparse matrix and their respective indices. We would be storing the row index i, column index j and the corresponding non-zero value at (i,j) in the sparse matrix in the form of a table. Considering the capacity of a single page to be 8 kilobytes and we are storing integer values, a table (of the above-suggested structure) with appx 675 rows can be stored on a single page. Thus the overall dimension of the table would be 675 X 3.

#### Loading the sparse matrix:

While loading the input sparse matrix, the matrix will be read in a block-by-block manner (similar to the way dense matrices are read in the code). A submatrix of dimension, say, 25 X 25 would be used to traverse the sparse matrix and the non-zero values (rowIndex, colIndex, value) will be stored in the above-stated page structure. The maximum number of non-zero values in a block is 625 so a single page can store all the entries from a block.

The following points need to be considered while loading the matrix:
- All the entries from a submatrix must be stored on the same page. If a submatrix has more non-zero values than the number of vacant rows on the current page, then all the non-zero numbers for that submatrix will be stored on a new page.
- In such a case, the vacant rows will be filled up with -1.

#### Submatrix and page indexing:

Depending on the distribution of nonzero numbers in the given sparse matrix, the number of entries corresponding to each 25 x 25 submatrix in a page will vary. This will cause the number of submatrix blocks to be unequally distributed across the pages. Thus we need to maintain a record of  entries corresponding to each 25 x 25 submatrix and the index of the page on which they are stored. 

Considering i and j to be indices of a non-zero entry in the sparse matrix, its submatrix block number will be (i/25, j/25) since the size of the submatrix block is 25 x 25 and we consider integer division. We keep a counter for indexing the pages. The counter is initially set to zero and incremented every time a new page is accessed. A file is maintained to hold the record of a submatrix block and its corresponding page index. Along with this we also store an index that refers to the starting position of entries for a submatrix block so that we can directly access the entries of the submatrix block (using the page index and the start index).

Thus, for each submatrix block we are storing:
- Page index
- Start index of the submatrix within the page.

#### Computing the transpose:

According to the structure of storage of the sparse matrix, the non-zero values are stored as (i,j,value). For transposing we can simply interchange the row index and column index and store the tuple as (j,i,value) but this makes further access (export, print etc) difficult. Thus we try to follow a process similar to the one followed for cross transposing dense matrices.

Considering i and j to be indices of a non-zero entry in the sparse matrix, its submatrix block number will be (i/25, j/25) and the indices of the number with respect to the submatrix block will be (Si,Sj) = (i%25,j%25). Firstly we internally transpose the elements of the submatrix block (i/25,j/25) and (j/25,i/25) and then we swap the two submatrix blocks. In case of i/25 = j/25, we just internally transpose the elements of the block.

For internally transposing the elements of a submatrix block, we follow the standard procedure for computing transpose of a matrix. Thus the entry (Si,Sj,value) becomes (Sj,Si,value) and viceversa. After internally transposing, we sort the tuples based on first the row index and then column index. Then we store these sorted elements starting from the start index in the page corresponding to the considered submatrix block.

The page index gives us the page corresponding to the submatrix. The start index gives the starting point for entries belonging to the submatrix. Considering we are reading submatrix block indexed (i/25,j/25), we continue to read the entries in the page until either (row index)/25 is not equal to i/25 or (column index)/25 is not equal to j/25 or both.
