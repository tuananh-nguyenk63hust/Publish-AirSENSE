//Code by Tuan Anh Nguyen
//Code is build with C++11
#include<bits/stdc++.h>
#include<mqtt/async_client.h>
#include<jsoncpp/json/json.h>
#include<fstream>
#include<ctime> 
//#include<mysql/mysql.h>
#include<mysql_connection.h>
#include<cppconn/prepared_statement.h>
#include<cppconn/driver.h>
#include<cppconn/statement.h>
#include<cppconn/resultset.h>
#include<cppconn/exception.h>
#define version 100000000
//using namespace std;
const std::string ADDRESS="tcp://localhost:1883"; //address server
const int QOS=0; // this's qos client subscribe
const std::string ID="AirsenseDB"; // This's name of client subscribe mosquitto 
const std::string TOPIC="SPARC/#"; // This's name of topic client subscribe 
const int num_of_reconnect=5;  // this's number when try connect
//const std::string moth["January","February","March","April","May","June","July","August","September","October","November","December"]={'01','02'.'03','04','05','06','07','08','09','10','11','12'};
//std::string file="/home/banhbanh/Desktop/"; // save raw data file edit time when receive data 
//char *NameFile=new char[30];
const std::string HOST="tcp://localhost";
const std::string USER="sparclab";
const std::string PASS="SPARCLab1";
sql::Driver *driver;
sql::Connection *Conn;
sql::Statement *sta;
sql::PreparedStatement *preSta;
sql::ResultSet *res;
bool CheckConnection()
{
    try
    {
        bool Connected=(Conn!=NULL)&&((Conn->isValid())||(Conn->reconnect()));
        if (!Connected)
        {
            driver=get_driver_instance();
            Conn=driver->connect(HOST,USER,PASS);
            Connected=Conn->isValid();
        }
        else return true;
        return false;
    }
    catch(sql::SQLException &e)
    {
        delete Conn;
        Conn=NULL;
        return false;
    }
}
class action_listener: public virtual mqtt::iaction_listener  // this listener when connect
{
    std::string namen;
    void on_failure(const mqtt::token& tok) override {}
    void on_success(const mqtt::token& tok) override {}
public: action_listener(const std::string& name): namen(name){}  
};
void DBReceive(Json::Value JsonData)
{
    long long NodeId;
    long long realtime;
    float PM2p5;
    float PM10;
    float PM1;
    float CO;
    float TEM;
    float HUM;
    try
    {
        std::string DBID=JsonData["NAME"].asString();
    //long long NodeId;
    std::stringstream stream;
    stream<<std::hex<<DBID;
    stream>>NodeId;
    NodeId+=version;
    realtime=JsonData["DATA"]["REALTIMES"].asInt64();
    PM2p5=JsonData["DATA"]["PM2.5"].asFloat();
    PM10=JsonData["DATA"]["PM10"].asFloat();
    PM1=JsonData["DATA"]["PM1"].asFloat();
    CO=JsonData["DATA"]["CO"].asFloat();
    TEM=JsonData["DATA"]["TEM"].asFloat();
    HUM=JsonData["DATA"]["HUM"].asFloat();
    if (CheckConnection())
    {
        std::cout<<Conn<<std::endl;
        Conn->setSchema("Airsense");
       // std::string timeconvert=coverttime(realtime);
            std::cout<<"INSERT INTO DB"<<std::endl;
            preSta=Conn->prepareStatement("INSERT INTO Data(NodeId,Time,PM2p5,PM10,PM1,TEMPERATURE,HUMIDITY) VALUES(?,?,?,?,?,?,?)");
            preSta->setInt64(1,NodeId);
            preSta->setInt(2,realtime);
            preSta->setDouble(3,PM2p5);
            preSta->setDouble(4,PM10);
            preSta->setDouble(5,PM1);
            preSta->setDouble(7,HUM);
            preSta->setDouble(6,TEM);
            //preSta->setDouble(8 ,CO);
            preSta->executeUpdate();
            preSta=Conn->prepareStatement("INSERT INTO ExtendedData(NodeId,Time,CO,CO2,SO2,NO2,O3) VALUES(?,?,?,?,?,?,?)");
            preSta->setString(1,DBID);
            preSta->setInt(2,realtime);
            preSta->setDouble(3,CO);
            preSta->setDouble(4,0);
            preSta->setDouble(5,0);
            preSta->setDouble(6,0);
            preSta->setDouble(7,0);
    }

    
    }
    catch(const Json::LogicError& e)
    {
        std::cerr<<"Can't conver Jsonfile"<<std::endl;
    }
    catch(const Json::RuntimeError& e)
    {
        std::cerr<<"Time Error"<<std::endl;
    }
    catch(const sql::SQLException& e)
    {
        std::cerr<<"Error Logic SQL"<<std::endl;
    }

}
 class callback: public virtual mqtt::callback, public virtual mqtt::iaction_listener //callback when connect and subscribe
{
   mqtt::async_client& cli_; //client subscribe
   action_listener listener;
   mqtt::connect_options& Conn; 
   int num=0;
    void reconnect()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));   
        try
        {
          cli_.connect(Conn,nullptr,*this);   
        }
        catch(const std::exception& e)
        {
            //cout<<"ERROR"<<endl;
            std::cerr <<"ERROR"<< e.what() << '\n';
            exit(1);
        }
        
    }
   
    // when connect fail
    void on_failure(const mqtt::token& tok) override
    {
        std::cout<<"Connect Fail"<<std::endl;
        num++;
        if (num>num_of_reconnect) exit(1);
        reconnect();
    }
    void on_success(const mqtt::token& tok) override{}
    void connected(const std::string& cause) override
    {
        std::cout<<"Connect Success"<<std::endl;
        cli_.subscribe(TOPIC,QOS,nullptr,listener);
    }
    void connection_lost(const std::string& cause) override
    {
        std::cout<<cause<<std::endl;
        num=0;
        reconnect();      
    }
    void message_arrived(mqtt::const_message_ptr msg) override
    {
        std::cout<<"Token: "<<msg->get_topic()<<std::endl;
        std::cout<<"QOS: "<<msg->get_qos()<<std::endl;
        std::cout<<msg<<std::endl;
        //cout<<msg<<endl;
        std::string Payload_byte=msg->to_string();
        Json::Value MessageData;
        Json::Reader reader;
        reader.parse(Payload_byte,MessageData);
        std::cout<<MessageData.toStyledString()<<std::endl;
        DBReceive(MessageData);
        
    }
public: callback(mqtt::async_client& cli, mqtt::connect_options& cOn): num(0),cli_(cli), Conn(cOn), listener("sublistener") {}
};

int main()
{
   
        mqtt::async_client cli(ADDRESS,ID); // client subscribe
        mqtt::connect_options conn;
        callback cb(cli,conn);
        cli.set_callback(cb);
        cli.connect(conn,nullptr,cb);
    //Conn->setSchema("Airsense");
    
    while (tolower(std::cin.get())!='q');
    
}
