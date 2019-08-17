// Code by Tuan Anh Nguyen
// Code is build with C++11

#include<mqtt/client.h>
#include<bits/stdc++.h>
#include<jsoncpp/json/json.h>
using namespace std;
//const string ADDRESS ="23.89.159.119:1883";
const string ADDRESS="tcp://localhost:1883";
const string ID="Testtest";
string TOPIC ="SPARC";
//string PAYLOAD ="hello word";
int QOS=0;
int main()
{   
    mqtt::async_client cli(ADDRESS,ID);
    mqtt::connect_options COnn;
    mqtt::token_ptr tok=cli.connect(COnn);
    tok->wait();
    cout<<"Connected"<<endl;
    double a[6];
    //for (int i=1;i<=6;i++) cin>>a[i];
    string strid[10];
    strid[1]="ESP_199991";
    strid[2]="ESP_299391";
    strid[3]="ESP_234123";
    strid[4]="ESP_132324";
    strid[5]="ESP_423424";
    strid[6]="ESP_231234";
    strid[7]="ESP_132244";
    strid[8]="ESP_231254";
    strid[9]="ESP_123421";
    strid[0]="ESP_999213";
  //  PAYLOAD["DATA"]["CREATE TIME"]="23h25";
   // PAYLOAD["DATA"]["END TIME"]="24h";
    //long long realtimes=312443222;
    for (int i=1;i<=10000;i++)
    {
        srand (static_cast <unsigned> (time(0)));
        int testint=i%10;
        for (int j=1;j<=6;j++) a[j]=static_cast<double> (rand())/static_cast <double> (RAND_MAX/100);
        Json::Value PAYLOAD;
        PAYLOAD["NAME"]=strid[testint];
        cout<<strid[testint]<<endl;
        PAYLOAD["DATA"]["PM2.5"]=a[1];
        PAYLOAD["DATA"]["PM10"]=a[2];
        PAYLOAD["DATA"]["PM1"]=a[3];
        PAYLOAD["DATA"]["HUM"]=a[4];
        PAYLOAD["DATA"]["TEM"]=a[5];
        PAYLOAD["DATA"]["CO"]=a[6];
        long long realtimes=rand()%10000000+2000000;
        PAYLOAD["DATA"]["REALTIMES"]=realtimes;
    //mqtt::message mess=mqtt::message();
        string PAYLOAD1="OK";
        Json::FastWriter Fast;
        string Payload_byte=Fast.write(PAYLOAD);
 // mqtt::message mess(TOPIC,Payload_byte.c_str(),1,true);
        cli.publish(TOPIC,Payload_byte.c_str(),1,true);
        cout<<"Publish Completed"<<endl;
        cout<<Payload_byte.c_str()<<endl;
        Json::Reader reader;
        Json::Value val;
        reader.parse(Payload_byte,val);
        if (i==10000) cout<<"ok"<<endl; 
        
    }
   // cout<<val<<endl;
    cout<<"Done"<<endl;
}
