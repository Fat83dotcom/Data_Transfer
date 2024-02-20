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
using std::vector;
using std::for_each;

typedef struct {
    string date_hour;
    float temperature;
    float humidity;
    float pressure;
    int idSensor;
}DataForTransfer;


// Interfaces

class SQLSuplier {
public:
    virtual string getSQL(const vector<string> args) = 0;
    SQLSuplier() {}
    virtual ~SQLSuplier() {}
};

class Source {
protected:
    string config;
    SQLSuplier *sql;
public:
    Source(const string &dbConfig) {
        config = dbConfig;
    }
    virtual ~Source(){}
    virtual string getConfigDB() = 0;
    virtual vector<string> getQuery() = 0;
};

class Error : public exception {
private:
    string msg;
public:
    Error(const string &msg) : msg(msg){}

    const char *what() const noexcept override {
        return msg.c_str();
    }
};

// Concrete Classes

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

class SQLSupplierDadosEstacao : public SQLSuplier{
    //origin
public:
    string getSQL(const vector<string> args) {
        string sql = format(
        "SELECT data_hora, temp_ext, umidade, pressao FROM \"tabelas_horarias\"."
        "\"{}\" FROM dados_estacao;", args[0]
    );
    // Define a quantidade de argumentos que o vector deve ter, para controlar a entrada
    // e saída de argumentos do format em qualquer situação.
    if (args.size() == 1){
        return sql;
    }
    return "";
    }
};

class SQLSuplierEstacaoIOT : public SQLSuplier {
    // Destiny
public:
    string getSQL(const vector<string> args){
        string sql = format(
            "INSERT INTO \"Core_datasensor\" (date_hour, temperature, humidity, pressure, id_sensor_id)"
            " VALUES (\'{}\', {}, {}, {}, {});", args[0], args[1], args[2], args[3], args[4]
        );
        if (args.size() == 5){
            return sql;
        }
        return "";
    }
};

class DBExecuter {
private:
    DataBase *dbOrigin;
    DataBase *dbDestiny;
    Source *origin;
    Source *destiny;
public:
    void insert() {

    };
    void select() {

    };
protected:
    DBExecuter() {}
    virtual ~DBExecuter() {}
};

class SourceDadosEstacao : public Source {
private:
    string fileName = "build/dateSequence.txt";
    ExtractDateFromFile *tableNames;
public:
    SourceDadosEstacao(const string &dbConfig) : Source(dbConfig) {
        sql = new SQLSupplierDadosEstacao();
        ExtractDateFromFile *tableNames = new ExtractDateFromFile(
            this->fileName
        );
    }
    virtual ~SourceDadosEstacao() {}

    string getConfigDB() {
        return this->config;
    }
    vector<string> getQuery() {
        vector<string> queries;
        vector<string> *tableNamesFromFile;
        tableNamesFromFile = &this->tableNames->getDates();

        for_each();
    }

};


int main(int, char**){
    // ExtractDateFromFile *extFile = new ExtractDateFromFile("dateSequence.txt");
    // for (const auto &date : extFile->getDates()){
    //     cout << date << endl;
    // };
    // cout << extFile->getDates().size() << endl;
    // delete extFile;

    SQLSupplierDadosEstacao *sqlDadoEsta = new SQLSupplierDadosEstacao();
    SQLSuplierEstacaoIOT *iot = new SQLSuplierEstacaoIOT();
    vector<string> arg = {"12-04-2024"};
    vector<string> arg1 = {"12-04-2024", "deokookde", "kokoko", "dasdasd", "mkmlkm"};
   
    string ret = sqlDadoEsta->getSQL(arg);
    string ret1 = iot->getSQL(arg1);
    cout << ret << endl;
    cout << ret1 << endl;

    return 0;    
}