//Code by Tuan Anh Nguyen
//Code is build with C++11

#include<bits/stdc++.h>
#include<mqtt/async_client.h>
#include<jsoncpp/json/json.h>
#include<fstream>
#include<ctime>
//using namespace std;
const std::string ADDRESS="tcp://localhost:1883"; //address server
const int QOS=0; // this's qos client subscribe
const std::string ID="Airsense"; // This's name of client subscribe mosquitto 
const std::string TOPIC="SPARC"; // This's name of topic client subscribe 
const int num_of_reconnect=5;  // this's number when try connect
std::string file="/home/sparclab/Desktop/"; // save raw data file edit time when receive data 
char *NameFile=new char[30];
class action_listener: public virtual mqtt::iaction_listener  // this listener when connect
{
    std::string namen;
    void on_failure(const mqtt::token& tok) override {}
    void on_success(const mqtt::token& tok) override {}
public: action_listener(const std::string& name): namen(name){}  
};
bool checkFile(std::string path)
{
    std::ifstream fi(path);
    return fi.good();
}
void coverttime(long long& Time)
{
    time_t ttime= Time;
    tm * CoverTimee;
    CoverTimee=localtime(&ttime);
    strftime(NameFile,30,"%H-%d-%B-%Y.csv",CoverTimee);
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
        //reader.parse(msg)
        //cout<<MessageData<<endl;
        std::string ID_Cli=MessageData["NAME"].asString();
        //cout<<"ID \t";
        std::cout<<ID_Cli<<std::endl;
        float Pm2p5=MessageData["DATA"]["PM2.5"].asFloat();
        float Pm10=MessageData["DATA"]["PM10"].asFloat();
        float Pm1=MessageData["DATA"]["PM1"].asFloat();
        float Hum=MessageData["DATA"]["HUM"].asFloat();
        float Tem=MessageData["DATA"]["TEM"].asFloat();
        float Co=MessageData["DATA"]["CO"].asFloat();
        long long Time=MessageData["DATA"]["REALTIMES"].asInt64();
        //std::cout<<Time<<std::endl;
        //cout<<checkFile(file)<<endl; //checkfile empty
        coverttime(Time);
        std::cout<<NameFile<<std::endl;
        std::string LastFile=file;
        for (int i=0;i<=29;i++) 
        {
            file+=NameFile[i];
           *(NameFile +i) = ' ';
        }
       // NameFile="";
        //NameFile[3]=''
        //delete[] NameFile;
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
        //char * NameFile=new char[23];
        
    }
public: callback(mqtt::async_client& cli, mqtt::connect_options& cOn): num(0),cli_(cli), Conn(cOn), listener("sublistener") {}
};

int main()
{
    mqtt::async_client cli(ADDRESS,ID); // client subscribe
    mqtt::connect_options conn;
    //cli.connect(conn);
    callback cb(cli,conn);
    cli.set_callback(cb);
    cli.connect(conn,nullptr,cb);
    while (tolower(std::cin.get())!='q');
    
}
