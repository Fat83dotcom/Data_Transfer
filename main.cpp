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

using std::ios;
using std::cout;
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
    vector<DataForTransfer> returnExecDB(const string &sql){
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
                tuple.idSensor = "1";
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
        cout << "DB delete." << endl;
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
                cout << "Arquivo Aberto!!!" << endl;
                cout << endl;
                this->__extractDateFromFile();
            }
            catch(const exception& e) {
                this->log->registerLog(e.what());
            }
        }
        else{
            cout << "Arquivo não está operando..." << endl;
        }
    }
    ~ExtractDateFromFile(){
        this->extracFile.close();
        delete log;
        cout << "Arquivo fechado." << endl;
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
    void clearDataQuery(){
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

class DBExecuter {
private:
    LogFile *log = new LogFile("LogClassDBExecuter.txt");
protected:
    DataBase *dbOrigin;
    DataBase *dbDestiny;
    SourceDadosEstacao *origin;
    SourceEstacaIOT *destiny;
    Counter *count;
    Timer *time;
public:
    DBExecuter(){
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
    }
    void executer(){
        try {
            // Complexidade do algoritmo O(n²)
            cout << "Criando as queries de consulta..." << "-> ";
            time->startTimer();
            vector<string> queryOrigin = origin->getQuery();
            time->endTimer();
            cout << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

            for (auto &queryOrigin : queryOrigin) {
                cout << "Query que está sendo buscada e transferida: " << queryOrigin << "-> ";
                time->startTimer();
                vector<DataForTransfer> dataFromDB = dbOrigin->returnExecDB(queryOrigin);
                time->endTimer();
                cout << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

                destiny->clearDataQuery();
                for (auto &transferDataTo : dataFromDB) {
                    destiny->setDataQuery(&transferDataTo);
                }

                cout << "Criando as queries de inserção..." << "-> ";
                time->startTimer();
                vector<string> queryDestiny = destiny->getQuery();
                time->endTimer();
                cout << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;

                cout << "Transferindo..." << "-> ";
                time->startTimer();
                for (auto &&queryDestiny : queryDestiny){
                    count-> incrementRows();
                    dbDestiny->execDB(queryDestiny);
                }
                time->endTimer();
                cout << "Tempo de execução: " << time->getElapsedTimeInSeconds() << "s." << endl;
                if (count->getsectionRows() > 0) count->incrementTables();
             
                count->incrementTotalRows();
                cout << "Transação terminada..." << endl;
                cout << "Linhas inseridas dessa query: " << count->getsectionRows() << endl;
                cout << "Linha inseridas até agora: " << count->getTotalRows() << endl;
                cout << "Tabelas inseridas até agora: " << count->getNumTables() << endl;
                cout << "***************************************" << endl;
                count->resetRows();
            }
            cout << "Total de tabelas Inseridas: " << count->getNumTables() << endl;
            cout << "Total de linhas inseridas: " << count-> getTotalRows() << endl;
        }
        catch(const std::exception& e) {
            this->log->registerLog(e.what());
        } 
    }
};

int main(int, char**){
    // ExtractDateFromFile *extFile = new ExtractDateFromFile("dateSequence.txt");
    // for (const auto &date : extFile->getDates()){
    //     cout << date << endl;
    // };
    // cout << extFile->getDates().size() << endl;
    // delete extFile;

    // SQLSupplierDadosEstacao *sqlDadoEsta = new SQLSupplierDadosEstacao();
    // SQLSuplierEstacaoIOT *iot = new SQLSuplierEstacaoIOT();
    // vector<string> arg = {"12-04-2024"};
    // vector<string> arg1 = {"12-04-2024", "deokookde", "kokoko", "dasdasd", "mkmlkm"};
   
    // string ret = sqlDadoEsta->getSQL(arg);
    // string ret1 = iot->getSQL(arg1);
    // cout << ret << endl;
    // cout << ret1 << endl;
    // delete sqlDadoEsta;

    // DataForTransfer a, b, c;
    // a = {"2024-02-05", "32.5", "85.7", "935.78", "0"};
    // b = {"2024-02-05", "33.8", "75.3", "934.18", "0"};
    // c = {"2024-02-05", "36.78", "47.2", "945.74", "0"};

    // vector<DataForTransfer> data;
    // data.push_back(a);
    // data.push_back(b);
    // data.push_back(c);

    // SourceEstacaIOT *source2 = new SourceEstacaIOT();

    // for (auto &dt : data) {
    //     source2->setDataQuery(&dt);
    // }
    // for (auto &query : source2->getQuery()) {
    //     cout << query << endl;
    // }

    // delete source2;

    // SourceDadosEstacao *source1 = new SourceDadosEstacao();

    // vector<string> sqlVector = source1->getQuery();
    // for (auto &&data : sqlVector){
    //     cout << data << endl;
    // };
    // delete source1;

    // DataBase *db = new DataBase(configDBOrigin);

    // for (auto &&data : sqlVector){
    //     db->returnExecDB(data);
    // };
    // delete db;
    try {
        DBExecuter *exec = new DBExecuter();
        exec->executer();
        delete exec;
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
    }

    return 0;    
}