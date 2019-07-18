// Code by Tuan Anh Nguyen
//Syntax use C++11

#include<mqtt/client.h>
#include<bits/stdc++.h>
#include<jsoncpp/json/json.h>
using namespace std;
const string ADDRESS ="23.89.159.119:1883"; // address new server
const string ID="Test";
string TOPIC ="SPARC";
//string PAYLOAD ="hello word";
int QOS=1;
int main()
{   
    mqtt::async_client cli(ADDRESS,ID);
    mqtt::connect_options COnn;
    mqtt::token_ptr tok=cli.connect(COnn);
    tok->wait();
    cout<<"Connected"<<endl;
    float a[6];
    for (int i=1;i<=6;i++) cin>>a[i];
    Json::Value PAYLOAD;
    PAYLOAD["NAME"]="ESP_6996";
    PAYLOAD["DATA"]["PM2.5"]=a[1];
    PAYLOAD["DATA"]["PM10"]=a[2];
    PAYLOAD["DATA"]["PM1"]=a[3];
    PAYLOAD["DATA"]["HUM"]=a[4];
    PAYLOAD["DATA"]["TEM"]=a[5];
    PAYLOAD["DATA"]["CO"]=a[6];
  //  PAYLOAD["DATA"]["CREATE TIME"]="23h25";
   // PAYLOAD["DATA"]["END TIME"]="24h";
   PAYLOAD["DATA"]["REALTIMES"]=696996;
    //mqtt::message mess=mqtt::message();
    string PAYLOAD1="OK";
   /* mqtt::message_ptr mess=mqtt::make_message(TOPIC,PAYLOAD.toStyledString());
    mess->set_qos(0);
    //mess.set_topic(TOPIC);
    //mess.set_payload(PAYLOAD);
    //mess.set_payload("{Sparclab: HUST, student: 100}");
    cout<<mess->get_qos()<<endl;
  //  string PM2p5=PAYLOAD["DATA"]["PM2.5"];
    Json::FastWriter fast;
    //string DATA=fast.write(PAYLOAD);
    string Cli_id=PAYLOAD["NAME"].asString();
    cout<<Cli_id<<endl;*/
    //cout<<DATA<<endl;
    //mqtt::message mess= new mqtt::message(TOPIC,PAYLOAD.toStyledString());
  //  byte[] bytedata=Encoding.UTF8.GetBytes(PAYLOAD);
  Json::FastWriter Fast;
  string Payload_byte=Fast.write(PAYLOAD);
 // mqtt::message mess(TOPIC,Payload_byte.c_str(),1,true);
    cli.publish(TOPIC,Payload_byte.c_str(),1,true);
    cout<<Payload_byte.c_str()<<endl;
    Json::Reader reader;
    Json::Value val;
    reader.parse(Payload_byte,val);
   // cout<<val<<endl;
    cout<<"Done"<<endl;
}
