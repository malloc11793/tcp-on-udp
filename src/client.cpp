#include <bits/stdc++.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>


using namespace std;

#define RTT 30
#define MSS 1024
#define THRESHOLD 65536
#define RCVBUF 524288
string serverIP = "0.0.0.0";
int serverPort = 8888;
sockaddr_in serverAddr, clientAddr;
int sockfd;
int noprint=0;
int rwnd = RCVBUF;
int gen = 1e7;
class Packet {
    public:
        unsigned short src_port,dst_port;
         int seq_num;
         int ack_num;
        bool ACK, PSH, SYN, FIN;
        unsigned short data_len;
        
        char data[MSS];

        Packet(){
            src_port = 0;
            dst_port = 0;
            seq_num = 0;
            ack_num = 0;
            ACK = false;
            PSH = false;
            SYN = false;
            FIN = false;
            data_len = 0;
            memset(data, 0, MSS);
        }

};
Packet pkt_send,pkt_recv;
struct timeval ROVT;

void connect2Server(){
    cout << "(Connecting)" << endl;

    srand(time(NULL));

    pkt_send.SYN = true;
    pkt_send.src_port = clientAddr.sin_port;
    pkt_send.dst_port = serverPort;
    pkt_send.seq_num = rand()%10000 + 1;
    socklen_t addr_len = sizeof(serverAddr);
    while(true){
        cout << "send packet : SYN : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
        if(rand()%gen>10)
            sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
        else
            cout << "packet lost" << endl;
        Packet pkt_recv;

        int rcv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
        
        if(rcv_len<0){
            cout << "Timeout: ";
            continue;
        }
        break;
    }
    cout << "receive packet : SYN : ACK : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;

    pkt_send.seq_num++;
    pkt_send.ack_num = pkt_recv.seq_num + 1;
    pkt_send.ACK = true;
    pkt_send.SYN = false;
    
    cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
    if(rand()%gen>10)
        sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
    else
        cout << "packet lost" << endl;

    cout << "(connected)" << endl;
}

int main(int argc, char *argv[]){
    ROVT.tv_sec = 1;
    ROVT.tv_usec = 0;
    sockfd =  socket(PF_INET, SOCK_DGRAM, 0);
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,&ROVT,sizeof(ROVT));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rwnd, sizeof(rwnd));
    if (sockfd == -1){
        cout << "Fail to create a socket";
        return -1;
    }

    serverIP = argv[1];
    serverPort = atoi(argv[2]);

    bzero(&serverAddr, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = inet_addr(serverIP.c_str());
    serverAddr.sin_port = atoi(argv[2]);

    bzero(&clientAddr, sizeof(clientAddr));
    clientAddr.sin_family = AF_INET;

    clientAddr.sin_port = rand()%10000 + 1;

    cout << setw(25)<< "Server's IP address: " << setw(10) << serverIP << endl;
    cout << setw(25) << "Server's port: " << setw(10) <<  serverPort << endl;
    cout << endl;

    connect2Server(); 

    cout << "(requested tasks)" << endl;
    int taskcnt=0;
    string cmd;
    socklen_t addr_len = sizeof(serverAddr);
    
    for(int i=1; i+2<argc; i++){
        noprint=0;
        string cmd(argv[i+2]);
        cout << "(task " << i << " : " << cmd  << ")" << endl;

        pkt_send.ACK = false;
        pkt_send.PSH = true;
        pkt_send.SYN = false;
        pkt_send.FIN = false;
        bzero(pkt_send.data, sizeof(pkt_send.data));
        strcpy(pkt_send.data, argv[i+2]);
        pkt_send.data_len = strlen(pkt_send.data);

        if(rand()%gen>10)
            sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
        else
            cout << "packet lost" << endl;
        cout << "send packet : PSH : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
         int recv_base = pkt_send.ack_num;
        if(cmd.find(".mp4")!=string::npos || cmd.find(".txt")!=string::npos || cmd.find(".jpg")!=string::npos){
            if (cmd.find(".mp4")!=string::npos || cmd.find(".jpg")!=string::npos)
                noprint=1;
            
            int recv_len;
             int pkt_num = 0;
            map < int,Packet> recv_buf;
            while(true){
                recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
                if(recv_len){
                    cout << "receive packet : PSH : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;
                    pkt_num = pkt_recv.ack_num - pkt_send.seq_num;
                    break;
                }
                cout << "PSH ACK miss" << endl;
                if(rand()%gen>10)
                    sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
                else
                    cout << "packet lost" << endl;
                
            }
            pkt_send.ACK = true;
            pkt_send.PSH = false;
            pkt_send.SYN = false;
            pkt_send.FIN = false;
            pkt_send.ack_num = pkt_recv.seq_num;
            pkt_send.seq_num = pkt_recv.ack_num;

            if(rand()%gen>10)
                sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
            else
                cout << "packet lost" << endl;

            cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
            int cnt=0;
            while (true){
                recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
                if(recv_len<0){
                    cout << "Timeout: ";
                    if(rand()%gen>10)
                        sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
                    else
                        cout << "packet lost" << endl;
                    cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
                    continue;
                }
                cout << "receive packet : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;
                recv_buf.insert(make_pair(pkt_recv.seq_num,pkt_recv));
                if (pkt_recv.seq_num > pkt_send.ack_num){
                    cout << "(gap detected)" << endl;
                    pkt_send.ACK = true;
                    pkt_send.PSH = false;
                    pkt_send.SYN = false;
                    pkt_send.FIN = false;
                    for( int i=0;i<pkt_num;i++){
                         int seq = recv_base + i*MSS;
                        if (recv_buf.find(seq) == recv_buf.end()){
                            pkt_send.ack_num = seq;
                            break;
                        }
                    }
                }
                else
                    pkt_send.ack_num = pkt_recv.seq_num + MSS;
                if(rand()%gen>10)
                    sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
                else
                    cout << "packet lost" << endl;
                cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
                if(recv_buf.size() == pkt_num)
                    break;
                cout << "current buff" << recv_buf.size() <<" " << pkt_num  << "needed" << endl;
            }
            cout <<pkt_num << " packets must be received" << endl;
            ofstream file(cmd, ios::out | ios::binary);
            for(auto it=recv_buf.begin();it!=recv_buf.end();it++){
                file.write(it->second.data,it->second.data_len);
            }
            file.close();
            recv_buf.clear();
            cout << cnt << " packets received" << endl;
            
        }
        else{
        while(!pkt_recv.FIN){
            int rcv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
            if(pkt_send.PSH == true)
                cout << "receive packet : PSH : ACK : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;
            else
                cout << "receive packet : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;

            pkt_send.ACK = true;
            pkt_send.PSH = false;
            pkt_send.SYN = false;
            pkt_send.FIN = false;
            pkt_send.seq_num = pkt_recv.ack_num;
            pkt_send.ack_num = pkt_recv.seq_num + strlen(pkt_recv.data) + 1;


            if(rand()%gen>10)
                sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
            else
                cout << "packet lost" << endl;
            cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
        }
        }
        cout << "(task  " << i << " completed)" << endl;
            if (!noprint){
                cout  << "Result:" << endl;
                ifstream result(cmd, ios::in);
                if(result.is_open()){
                string line;
                    while (getline(result, line)){
                        cout << line << endl;
                    }
                }
                else{
                    cout << pkt_recv.data << endl;
                }
            }
        bzero(&pkt_recv, sizeof(pkt_recv));
    }

    cout << "(out of tasks)" << endl;
    cout << "(disconnecting)" << endl;


    pkt_send.ACK = false;
    pkt_send.PSH = false;
    pkt_send.SYN = false;
    pkt_send.FIN = true;
    while(true){
        if(rand()%gen>10)
            sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
        else
            cout << "packet lost" << endl;
        cout << "send packet : FIN : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
        int rcv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
        if(rcv_len<0){
            cout << "Timeout: ";
            continue;
        }
        break;
    }
    cout << "receive packet : FIN : ACK : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;
    while (true){
        int rcv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&serverAddr, &addr_len);
        if(rcv_len<0){
            cout << "Timeout: ";
            if(rand()%gen>10)
                sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
            else
                cout << "packet lost" << endl;
            cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;
            continue;
        }
        cout << "receive packet : FIN : SEQ = " << pkt_recv.seq_num << " : ACK  = " << pkt_recv.ack_num << endl;
        break;
    }
    pkt_send.ACK = true;
    pkt_send.FIN = false;
    pkt_send.PSH = false;
    pkt_send.SYN = false;
    pkt_send.seq_num++;
    pkt_send.ack_num = pkt_recv.seq_num + 1;
    if(rand()%gen>10)
        sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0,(struct sockaddr *)&serverAddr, addr_len);
    else
        cout << "packet lost" << endl;
    cout << "send packet : ACK : SEQ = " << pkt_send.seq_num << " : ACK  = " << pkt_send.ack_num << endl;

    cout << "(disconnected)" << endl;
}