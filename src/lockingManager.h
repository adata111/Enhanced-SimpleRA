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
    // void read_lock_file();
    void lockfile_read();
    void lockfile_changeStatus(string tname, int lockNum);
    int lockfile_getStatus(string tname);
    void lockfile_write();
    void lockFile_create();
    void lockFile_insertTable(string tname);
    void change_lock_status(string tableName,int lock_type);
    void insert_table_to_lock(string tableName);
    int status_of_table(string tableName);
    // void update_start_file();
    // void create_start_file();
    // int ping_start_file();
    // bool isFileExists(string name);
};

#endif