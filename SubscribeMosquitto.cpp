//Code by Tuan Anh Nguyen
//Code is build with C++11

#include<bits/stdc++.h>
#include<mqtt/async_client.h>
#include<jsoncpp/json/json.h>
#include<fstream>
#include<ctime>
#include<mysql/mysql.h>
#include<mysql_connection.h>
#include<cppconn/prepared_statement.h>
#include<cppconn/driver.h>
#include<cppconn/statement.h>
#include<cppconn/resultset.h>
#include<cppconn/exception.h>
//using namespace std;
const std::string ADDRESS="tcp://localhost:1883"; //address server
const int QOS=0; // this's qos client subscribe
const std::string ID="Airsense"; // This's name of client subscribe mosquitto 
const std::string TOPIC="SPARC"; // This's name of topic client subscribe 
const int num_of_reconnect=5;  // this's number when try connect
//const std::string moth["January","February","March","April","May","June","July","August","September","October","November","December"]={'01','02'.'03','04','05','06','07','08','09','10','11','12'};
std::string file="/home/banhbanh/Desktop/"; // save raw data file edit time when receive data 
//char *NameFile=new char[30];
const std::string HOST="tcp://localhost";
const std::string USER="sparclab1";
const std::string PASS="sparclab1";
sql::Driver *driver;
sql::Connection *Conn;
sql::Statement *sta;
sql::PreparedStatement *preSta;
sql::ResultSet *res;
std::string ConvertMonthToInt(std::string s)
{
    std::string res;
    if (s=="January") res="01";
    if (s=="February") res="02";
    if (s=="March") res="03";
    if (s=="April") res="04";
    if (s=="May") res="05";
    if (s=="June") res="06";
    if (s=="July") res="07";
    if (s=="August") res="08";
    if (s=="September") res="09";
    if (s=="October") res="10";
    if (s=="November") res="11";
    if (s=="December") res="12";
    return res;
}
class action_listener: public virtual mqtt::iaction_listener  // this listener when connect
{
    std::string namen;
    void on_failure(const mqtt::token& tok) override {}
    void on_success(const mqtt::token& tok) override {}
public: action_listener(const std::string& name): namen(name){}  
};

std::string coverttime(long long& Time)
{
    putenv("TZ=Asia/Ho_Chi_Minh");
    time_t ttime= Time;
    std::string NameFile="";
    tm * CoverTimee;
    CoverTimee=localtime(&ttime);
    char *NameYear=new char[5];
   // for (int i=0;i<=32;i++) NameFile1[i]="";
    strftime(NameYear,5,"%Y",CoverTimee);
    //std::cout<<NameYear<<std::endl;
    char *NameMoth=new char[10];
    strftime(NameMoth,10,"%B",CoverTimee);
    char *NameDay=new char[5];
    strftime(NameDay,5,"%d",CoverTimee);
    char *NameHours=new char[5];
    strftime(NameHours,5,"%H",CoverTimee);
    char *NameMinute=new char[3];
    strftime(NameMinute,3,"%M",CoverTimee);
    //NameFile+=NameMoth;
    std::string STR_NameMoth=ConvertMonthToInt(NameMoth);    
    //std::cout<<STR_NameMoth<<std::endl;
    NameFile+=NameYear;
    NameFile+="-"+STR_NameMoth+"-"+NameDay+" "+NameHours+":"+NameMinute+":00";
    //NameFile=NameHours;
    std::cout<<NameFile<<std::endl;
    return NameFile;
}
bool checkretain(std::string ttime,std::string iid)
{
    std::string timelate="";
    std::string IDLate="";
    //std::cout<<"ok"<<std::endl;
    preSta=Conn->prepareStatement("SELECT * FROM data");
    res=preSta->executeQuery();
    res->afterLast();
    //std::cout<<"ok"<<std::endl;
    while(res->previous())
    {
        timelate=res->getString("TIME");
        IDLate=res->getString("ID");
        break;
    }
    std::cout<<timelate<<std::endl;
    std::cout<<IDLate<<std::endl;
    if ((timelate==ttime)&&(iid==IDLate))
     {  
         std::cout<<"false"<<std::endl;
         return false;
     }
    return true;
}
void DBReceive(Json::Value JsonData)
{
    std::string DBID=JsonData["NAME"].asString();
    long long realtime=JsonData["DATA"]["REALTIMES"].asInt64();
    float PM2p5=JsonData["DATA"]["PM2.5"].asFloat();
    float PM10=JsonData["DATA"]["PM10"].asFloat();
    float PM1=JsonData["DATA"]["PM1"].asFloat();
    float CO=JsonData["DATA"]["CO"].asFloat();
    float TEM=JsonData["DATA"]["TEM"].asFloat();
    float HUM=JsonData["DATA"]["HUM"].asFloat();
    driver=get_driver_instance();
    Conn=driver->connect(HOST,USER,PASS);
    Conn->setSchema("Airsense");
    std::string timeconvert=coverttime(realtime);
    if (checkretain(timeconvert,DBID))
    {
        //std::cout<<checkretain(timeconvert)<<std::endl;
        //if (checkretain(timeconvert)) std::cout<<"Don't accept to DB"<<std::endl;
        std::cout<<"Accept To DB"<<std::endl;
        preSta=Conn->prepareStatement("INSERT INTO data(ID,TIME,PM2p5,PM10,PM1,HUM,TEM,CO) VALUES(?,?,?,?,?,?,?,?)");
        preSta->setString(1,DBID);
        preSta->setDateTime(2,timeconvert);
        preSta->setDouble(3,PM2p5);
        preSta->setDouble(4,PM10);
        preSta->setDouble(5,PM1);
        preSta->setDouble(6,HUM);
        preSta->setDouble(7,TEM);
        preSta->setDouble(8 ,CO);
        preSta->executeUpdate();
    }
    else std::cout<<"Don't Accept To DB"<<std::endl;

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
        //Time=time(nullptr);
        //std::cout<<Time<<std::endl;
        /*std::string LastFile=file;
        std::string namefile=coverttime(Time);
       // std::cout<<namefile<<std::endl;
        file+=namefile;
        std::cout<<file<<std::endl;
        bool BcheckFile=checkFile(file);
        std::ofstream fo(file,std::ios::app);
        if (BcheckFile==false) 
        {
            std::cout<<"Create File!"<<std::endl;
            fo<<"ID\tREALTIMES\tPM2.5\tPM10\tPM1\tCO\tHUM\tTEM\n";
        } 

        fo<<ID_Cli<<"\t"<<Time<<"\t"<<Pm2p5<<"\t"<<Pm10<<"\t"<<Pm1<<"\t"<<Co<<"\t"<<Hum<<"\t"<<Tem<<"\n";
        file=LastFile;
        //char * NameFile=new char[23];*/
        
    }
public: callback(mqtt::async_client& cli, mqtt::connect_options& cOn): num(0),cli_(cli), Conn(cOn), listener("sublistener") {}
};

int main()
{
    
    //Conn->setSchema("Airsense");
    mqtt::async_client cli(ADDRESS,ID); // client subscribe
    mqtt::connect_options conn;
    //cli.connect(conn);
    callback cb(cli,conn);
    cli.set_callback(cb);
    cli.connect(conn,nullptr,cb);
    while (tolower(std::cin.get())!='q');
    
}
