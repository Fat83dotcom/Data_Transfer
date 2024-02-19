#define FMT_HEADER_ONLY
#include <fmt/format.h>
#include <fmt/core.h>
#include <iostream>
#include <pqxx/pqxx>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>

#include "configFile.h"

namespace fs = std::filesystem;

using fmt::format;
using std::string;
using std::exception;
using std::cout;
using std::endl;
using std::ofstream;
using std::ifstream;
using std::getline;
using std::ios;
using std::chrono::system_clock;
using std::chrono::time_point;
using fmt::format;
using std::vector;
using std::for_each;


class Error : public exception {
private:
    string msg;
public:
    Error(const string &msg) : msg(msg){}

    const char *what() const noexcept override {
        return msg.c_str();
    }
};


class LogFile {
private:
    ofstream logFile;
public:
    LogFile(const string &nameF) : logFile(nameF, ios::app) {
        if (logFile.is_open()){
            logFile << "Log Operando -> " <<  this->currentTime() << endl;
            logFile << endl;
        }
        else{
            cout << "Log não está operando..." << endl;
        }
    }
    ~LogFile(){
        logFile.close();
        cout << "Arquivo Log fechado." << endl;
    }

    string currentTime(){
        char buffer[50]; 
        auto currentTime = system_clock::now();
        // time_point<system_clock> timePoint = currentTime;
        time_t currentTimeT = system_clock::to_time_t(currentTime);
        std::tm* localTime = std::localtime(&currentTimeT);
        strftime(buffer, sizeof(buffer), "%A, %d/%m/%Y %H:%M:%S", localTime);
        return buffer;
    }

    void registerLog(const string &msg){
        logFile << msg << " -> " << this->currentTime() << endl;
        logFile << endl;
    }
};


class DataBase {
private:
    pqxx::connection C;
    LogFile *log = new LogFile("LogFileAlgoritmoExtracaoCPP.txt");

    string _queryInserTimeTable(const string &tableName, const vector<string> values){
        string sql = format(
            "INSERT INTO tabelas_horarias.\"{}\" (data_hora, umidade, pressao, temp_int, temp_ext)"
            "VALUES ('{}', {}, {}, {}, {})", tableName.c_str(), values[0].c_str(), values[1].c_str(),
            values[2].c_str(), values[3].c_str(), values[4].c_str()
        );
        return sql;
    }
    

    void _execDB(const string &sql){
        try {
            pqxx::work W(C);
            W.exec(sql);
            W.commit();
        } 
        catch (const exception &e) {
            this->log->registerLog(e.what());
        }
    }
public:
    DataBase(const string &config) : C(config){}
    virtual ~DataBase(){
        delete log;
        cout << "DB delete." << endl;
    }

    void insertDataDB(const string currentDate, vector<string>data){
        try{
            if (currentDate != "00-00-0"){
                string sql = this->_queryInserTimeTable(currentDate, data);
                this->_execDB(sql);
            }
        }
        catch(const exception& e) {
            this->log->registerLog(e.what());
        }
    }
};

class SQLSuplier {
public:
    virtual string getSQL() = 0;
protected:
    SQLSuplier() {}
    virtual ~SQLSuplier() {}
private:
    vector<string> sqlContainer;
};

class DBExecuter {
private:
    string config;
    DataBase *db;
    SQLSuplier *sql;
public:
    virtual void insert(string &sql) = 0;
    virtual void select(string &sql) = 0;
    virtual void update(string & sql) = 0;
    virtual void delete_(string &sql) = 0;
protected:
    DBExecuter(string &dbConfig) {
        config = dbConfig;
    }
    virtual ~DBExecuter() {}
};

class SQLSupplierDadosEstacao {

};



class ExtractDateFromFile {
private:
    ifstream extracFile;
    vector<string> extractedDates;

    void __extractDateFromFile() {
        string line;
        while (!this->extracFile.eof()) {
            getline(this->extracFile, line); 
            this->extractedDates.push_back(line);
            cout << line << endl;
        }
    }
public:
    ExtractDateFromFile(const string &fileName) : extracFile(fileName, ios::in) {
        if (this->extracFile.is_open()){
            try {
                cout << "Arquivo Aberto!!!" << endl;
                cout << endl;
                this->__extractDateFromFile();
            }
            catch(const exception& e) {
                cout << e.what() << endl;
            }
        }
        else{
            cout << "Arquivo não está operando..." << endl;
        }
    }
    ~ExtractDateFromFile(){
        this->extracFile.close();
        cout << "Arquivo fechado." << endl;
    }
    vector<string> getDates() {
        return this->extractedDates;
    }
};

int main(int, char**){
    // ExtractDateFromFile *extFile = new ExtractDateFromFile("dateSequence.txt");
    // for (const auto &date : extFile->getDates()){
    //     cout << date << endl;
    // };
    // cout << extFile->getDates().size() << endl;
    // delete extFile;

    string filepath = "dateSequence.txt";
    ifstream file;
    file.open(filepath, ios::in);
    if (file.is_open()) {
        cout << "abriu." << endl;
        string line;
        while (getline(file, line)) {
            
            cout << line << endl;
        }
        file.close();
    }
    else {
        cout << "não abriu." << endl;
    }
    
}