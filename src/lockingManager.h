#ifndef __LOCKINGMANAGER_H
#define __LOCKINGMANAGER_H
#include"page.h"

class LockingManager{
    string lockingfilename = "../data/temp/lockfile";
    
    public:
    
    int rowNum = 0;
    vector<pair<string,int>> lockingTable;
    vector<string> lock_tables;
    vector<int> lock_status;
    
    LockingManager();
    void lockfile_read();
    void lockfile_changeStatus(string tname, int lockNum);
    int lockfile_getStatus(string tname);
    void lockfile_write();
    void lockFile_create();
    void lockFile_insertTable(string tname);
};

#endif