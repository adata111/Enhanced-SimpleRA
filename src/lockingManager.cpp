#include "global.h"

string tableName;
LockingManager::LockingManager()
{
    logger.log("LockingManager::LockingManager");
}

void LockingManager::lockFile_create()
{
    struct stat buffer;
    string filename = this->lockingfilename;
    if(stat(filename.c_str(), &buffer) != 0)
    {
        ofstream fout(this->lockingfilename);
        fout.close();
    }

    return;
}

void LockingManager::lockfile_read()
{
    bool readable = false;
    while (this->rowNum > 0 && !readable)
    {
        string tname, status;
        ifstream fin(this->lockingfilename, ios::in);
        this->lockingTable.clear();

        for (int j = 0; j < this->rowNum; j++)
        {
            fin >> tname >> status;
            if (tname.length() == 0 || status.length() == 0)
            {
                readable = false;
            }
            else
            {
                this->lockingTable.push_back(make_pair(tname, stoi(status)));
                readable = true;
            }
        }

        fin.close();
    }

    return;
}

void LockingManager::lockfile_write()
{
    ofstream fout(this->lockingfilename, ios::trunc);
    for (int i = 0; i < this->rowNum; i++)
    {
        fout << this->lock_tables[i] << " " << this->lock_status[i] << endl;
    }
    fout.close();
}

bool findTable(const pair<string, int> &lockEntry)
{
    return lockEntry.first == tableName;
}

void LockingManager::lockfile_changeStatus(string tname, int lockNum)
{
    this->lockfile_read();
    tableName = tname;
    auto tableItr = find_if((this->lockingTable).begin(), (this->lockingTable).end(), findTable);
    // Table was found
    if (tableItr != (this->lockingTable).end())
    {
        int index = tableItr - (this->lockingTable).begin();
        this->lockingTable[index].second = lockNum;
    }

    // TO BE CHANGED
    ofstream fout(this->lockingfilename, ios::trunc);
    for (int i = 0; i < this->rowNum; i++)
    {
        fout << this->lockingTable[i].first << " " << this->lockingTable[i].second << endl;
    }
    fout.close();
}

void LockingManager::lockFile_insertTable(string tname)
{
    this->lockfile_read();
    tableName = tname;
    auto tableItr = find_if((this->lockingTable).begin(), (this->lockingTable).end(), findTable);
    if (tableItr == (this->lockingTable).end())
    {
        this->lockingTable.push_back(make_pair(tname, 0));
        this->rowNum += 1;
        // TO BE CHANGED
        ofstream fout(this->lockingfilename, ios::trunc);
        for (int i = 0; i < this->rowNum; i++)
        {
            fout << this->lockingTable[i].first << " " << this->lockingTable[i].second << endl;
        }
        fout.close();
    }

    return;
}

int LockingManager::lockfile_getStatus(string tname)
{
    this->lockfile_read();
    tableName = tname;
    auto tableItr = find_if((this->lockingTable).begin(), (this->lockingTable).end(), findTable);
    
    // If table was found
    if (tableItr != (this->lockingTable).end())
    {
        int index = tableItr - (this->lockingTable).begin();
        return this->lockingTable[index].second;
    }
    else
    {
        cout << "ERR: Trying to find status of unknown table" << endl;
        exit(1);
    }

    return -1;
}