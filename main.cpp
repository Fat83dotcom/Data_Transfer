#define FMT_HEADER_ONLY
#include <fmt/format.h>
#include <fmt/core.h>
#include <iostream>
#include <pqxx/pqxx>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>

#include "configFile.h"

namespace fs = std::filesystem;

using std::ios;
// using std::cout;
using std::endl;
using fmt::format;
using std::string;
using std::vector;
using pqxx::result;
using std::getline;
using std::for_each;
using std::ofstream;
using std::ifstream;
using std::exception;
using std::chrono::system_clock;
using std::chrono::time_point;
using std::chrono::duration;
   
static void daemon(const char* workPath) {
    pid_t pid;
    
    pid = fork();
    
    if (pid < 0) exit(EXIT_FAILURE);
    
    if (pid > 0) exit(EXIT_SUCCESS);

    if (setsid() < 0) exit(EXIT_FAILURE);

    signal(SIGCHLD, SIG_IGN);
    signal(SIGHUP, SIG_IGN);

    pid = fork();

    if (pid < 0) exit(EXIT_FAILURE);
 
    if (pid > 0) exit(EXIT_SUCCESS);

    umask(0);

    chdir(workPath);
    
    int x;
    for (x = sysconf(_SC_OPEN_MAX); x>=0; x--) {
        close (x);
    }

    openlog ("Data_Transfer", LOG_PID, LOG_DAEMON);
}


typedef struct {
    string date_hour;
    string temperature;
    string humidity;
    string pressure;
    string idSensor;
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
    SQLSuplier *sql;
public:
    Source() {}
    virtual ~Source(){}
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
protected:
    ofstream logFile;
public:
    LogFile(const string &nameF) : logFile(nameF, ios::app) {
        if (logFile.is_open()){
            logFile << "Log Operando -> " <<  this->currentTime() << endl;
            logFile << endl;
        }
    }
    ~LogFile(){
        logFile.close();
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
protected:
    pqxx::connection C;
    LogFile *log = new LogFile("LogClassDataBase.txt");
public:
    void execDB(const string &sql){
        try {
            pqxx::work W(C);
            W.exec(sql);
            W.commit();
        } 
        catch (const exception &e) {
            this->log->registerLog(e.what());
        }
    }
    vector<DataForTransfer> returnExecDB(const string &sql, const string idSensor){
        try {
            pqxx::work W(C);
            result r {W.exec(sql)};
            vector<DataForTransfer> data;
            for (auto row : r) {
                DataForTransfer tuple;
                tuple.date_hour = row[0].c_str();
                tuple.temperature = row[1].c_str();
                tuple.humidity = row[2].c_str();
                tuple.pressure = row[3].c_str();
                tuple.idSensor = idSensor;
                data.push_back(tuple);
            }
            W.commit();
            return data;
        } 
        catch (const exception &e) {
            this->log->registerLog(e.what());
            vector<DataForTransfer> dataNone;
            return dataNone;
        }
    }

    DataBase(const string &config) : C(config){}
    virtual ~DataBase(){
        delete log;
    }
};

class ExtractDateFromFile {
private:
    ifstream extracFile;
    LogFile *log = new LogFile("LogClassExtractDateFromFile.txt");
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
                this->__extractDateFromFile();
            }
            catch(const exception& e) {
                this->log->registerLog(e.what());
            }
        }
    }
    ~ExtractDateFromFile(){
        this->extracFile.close();
        delete log;
    }
    vector<string> getDates() {
        return this->extractedDates;
    }
};

class SQLSupplierDadosEstacao : public SQLSuplier {
    //origin
private:
    LogFile *log = new LogFile("LogClassSQLSupplierDadosEstacao.txt");
public:
    string getSQL(const vector<string> args) {   
        // Define a quantidade de argumentos que o vector deve ter, para controlar a entrada
        // e saída de argumentos do format em qualquer situação.
        try {
            if (args.size() == 1){
                string sql = format(
                    "SELECT data_hora, temp_ext, umidade, pressao FROM \"tabelas_horarias\"."
                    "\"{}\";", args[0]
                );
                return sql;
            }
            return "";
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
            return "";
        }
    }
    virtual ~SQLSupplierDadosEstacao(){
        delete log;
    }
};

class SQLSuplierEstacaoIOT : public SQLSuplier {
    // Destiny
private:
    LogFile *log = new LogFile("LogClassSQLSupplierDadosEstacao.txt");
public:
    string getSQL(const vector<string> args){
        try {
            if (args.size() == 5){
                string sql = format(
                    "INSERT INTO \"Core_datasensor\" (date_hour, temperature, humidity, pressure, id_sensor_id)"
                    " VALUES (\'{}\', {}, {}, {}, {});", args[0], args[1], args[2], args[3], args[4]
                );
                return sql;
            }
            return "";
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
            return "";
        }
    }
    virtual ~SQLSuplierEstacaoIOT(){
        delete log;
    }
};

class SourceDadosEstacao : public Source {
private:
    LogFile *log = new LogFile("LogClassSourceDadosEstacao.txt");
protected:
    string fileName = "dateSequence.txt";
    ExtractDateFromFile *tableNames = new ExtractDateFromFile(
        this->fileName
    );
public:
    SourceDadosEstacao() : Source() {
        this->sql = new SQLSupplierDadosEstacao();
    }
    virtual ~SourceDadosEstacao() {
        delete sql;
        delete log;
        delete this->tableNames;
    }

    vector<string> getQuery() {
        try {
            vector<string> queries;
            for (const auto &tbName : this->tableNames->getDates()) {
                if (!tbName.empty()){
                    vector<string> args;
                    args.push_back(tbName);
                    queries.push_back(
                        sql->getSQL(args)
                    );
                }
            }       
            return queries;
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
            vector<string> queries;
            return queries;
        }   
    }
};

class SourceEstacaIOT : public Source {
private:
    LogFile *log = new LogFile("LogClassSourceEstacaIOT.txt");
protected:
    vector<DataForTransfer*> dataForSQL;
public:
    SourceEstacaIOT() : Source() {
        this->sql = new SQLSuplierEstacaoIOT();
    }
    virtual ~SourceEstacaIOT(){
        delete sql;
        delete log;
    }
    void _clearDataQuery(){
        try {
            if (!this->dataForSQL.empty()) {
                this->dataForSQL.clear();
            }
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
        }
    }
    void setDataQuery(DataForTransfer* dTQ){
        try {
            this->dataForSQL.push_back(dTQ);
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
        }
    }
    vector<string> getQuery() {
        try {
            vector<string> queries;
            for (auto &dFSQL : this->dataForSQL) {
                vector<string> args;
                args.push_back(dFSQL->date_hour);
                args.push_back(dFSQL->temperature);
                args.push_back(dFSQL->humidity);
                args.push_back(dFSQL->pressure);
                args.push_back(dFSQL->idSensor);
                queries.push_back(
                    sql->getSQL(args)
                ); 
            }
            this->_clearDataQuery();
            return queries;
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
            vector<string> queries;
            return queries;
        }
    }
};

class Counter {
private:
    unsigned long numTables = 0;
    unsigned long sectionRows = 0;
    unsigned long totalRows = 0;

public:
    Counter(){}
    ~Counter(){}
    void incrementRows() {
        this->sectionRows++;
    }
    void resetRows() {
        this->sectionRows = 0;
    }
    void incrementTables() {
        this->numTables++;
    }
    void incrementTotalRows() {
        this->totalRows += this->sectionRows;
    }
    unsigned long getTotalRows() {
        return this->totalRows;
    }
    unsigned long getNumTables() {
        return this->numTables;
    }
    unsigned long getsectionRows() {
        return this->sectionRows;
    }
};

class Timer {
private:
    time_point<std::chrono::steady_clock> start;
    time_point<std::chrono::steady_clock> end;
    duration<double> diff;
public:
    Timer(){}
    ~Timer(){}

    void startTimer() {
        start = std::chrono::steady_clock::now();
    }

    void endTimer() {
        end = std::chrono::steady_clock::now();
        diff = end - start;
    }

    double getElapsedTimeInSeconds() {
        return diff.count();
    }
};

class LogExecuter {
private:
    ofstream logFile;
public:
    LogExecuter(const string &nameFile) : logFile(nameFile, ios::app) {
        if(this->logFile.is_open()){
            logFile << "Log Executer on fire !!" << endl;
        }
    }
    ~LogExecuter() {
        this->logFile.close();
    }

    ofstream& registerLog() {
        return logFile;
    }
};

class DBExecuter {
private:
    LogFile *log = new LogFile("LogClassDBExecuter.txt");
    LogExecuter *innerFile = new LogExecuter("Log_Executer.txt");
protected:
    DataBase *dbOrigin;
    DataBase *dbDestiny;
    SourceDadosEstacao *origin;
    SourceEstacaIOT *destiny;
    Counter *count;
    Timer *time;
public:
    DBExecuter() {
        try {
            dbOrigin = new DataBase(configDBOrigin);
            dbDestiny = new DataBase(configDBDestiny);
            origin = new SourceDadosEstacao();
            destiny = new SourceEstacaIOT;
            count = new Counter();
            time = new Timer();
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
        }
    }
    ~DBExecuter() {
        delete dbOrigin;
        delete dbDestiny;
        delete origin;
        delete destiny;
        delete log;
        delete count;
        delete time;
        delete innerFile;
    }
    void executer(){
        try {
            // Complexidade do algoritmo O(n²)
            innerFile->registerLog() << "Inicio da tarefa: \n" << log->currentTime() << endl;
            innerFile->registerLog() << "Criando as queries de consulta..." << "-> ";
            time->startTimer();
            vector<string> queryOrigin = origin->getQuery();
            time->endTimer();
            innerFile->registerLog() << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

            for (auto &queryOrigin : queryOrigin) {
                innerFile->registerLog() << "Query que está sendo buscada e transferida: " << queryOrigin << "-> ";
                time->startTimer();
                vector<DataForTransfer> dataFromDB = dbOrigin->returnExecDB(queryOrigin, "1");
                time->endTimer();
                innerFile->registerLog() << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

                for (auto &transferDataTo : dataFromDB) {
                    destiny->setDataQuery(&transferDataTo);
                }

                innerFile->registerLog() << "Criando as queries de inserção..." << "-> ";
                time->startTimer();
                vector<string> queryDestiny = destiny->getQuery();
                time->endTimer();
                innerFile->registerLog() << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

                innerFile->registerLog() << "Transferindo..." << "-> ";
                time->startTimer();
                for (auto &&queryDestiny : queryDestiny){
                    count-> incrementRows();
                    dbDestiny->execDB(queryDestiny);
                }
                time->endTimer();
                innerFile->registerLog() << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;
                if (count->getsectionRows() > 0) count->incrementTables();
             
                count->incrementTotalRows();
                innerFile->registerLog() << "Transação terminada..." << endl;
                innerFile->registerLog() << "Linhas inseridas dessa query: " << count->getsectionRows() << endl;
                innerFile->registerLog() << "Linha inseridas até agora: " << count->getTotalRows() << endl;
                innerFile->registerLog() << "Tabelas inseridas até agora: " << count->getNumTables() << endl;
                innerFile->registerLog() << "***************************************" << endl;
                count->resetRows();
            }
            innerFile->registerLog() << "Total de tabelas Inseridas: " << count->getNumTables() << endl;
            innerFile->registerLog() << "Total de linhas inseridas: " << count-> getTotalRows() << endl;
            innerFile->registerLog() << "\nTérmino da tarefa: " << log->currentTime() << endl;
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
        } 
    }
};

int main(int, char**){

    const char* dirTarget = "/home/ubuntu/data_transf";
    // const char* dirTarget = "/home/fernando/Área de Trabalho/Data_Transfer/dataTransf";

    daemon(dirTarget);
    
    while (1) {
        syslog (LOG_NOTICE, "Data_Transfer Daemon started.");
        try {
            DBExecuter *exec = new DBExecuter();
            exec->executer();
            delete exec;
        }
        catch(const std::exception& e) {
            ;
        }
        break;
        sleep(30);
    }
   
    syslog (LOG_NOTICE, "Data_Transfer Daemon terminated.");
    closelog();
    
    return EXIT_SUCCESS;
}