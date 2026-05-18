#include<bits/stdc++.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<netdb.h>
#include<arpa/inet.h>
#include<pthread.h>
#include <time.h>
#define RTT 30
#define MSS 1024
#define THRESHOLD 65536
#define RCVBUF 524288

using namespace std;
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
int sockfd;
int gen=1e7;
struct sockaddr_in serverAddr, clientAddr;
struct timeval ROVT;
struct timespec _t;
vector<Packet> file_pkts;
map <int, int> pkts_stat;//0:acked 1: unsend 2:send, unacked
long timer = 0;
pthread_mutex_t Mutex;

int clientcnt = 0;
pid_t pid;
int recv_len;
int send_base;
int notacked=0;
int dup_cnt = 0;
int pre_ack=-1;

int addr_len;
void calc(string expression);


void *recv_thread(void *arg);

Packet pkt_recv, pkt_send;
int mode = 0;
int cwnd = MSS, ssthresh = THRESHOLD, rwnd = RCVBUF;
int main(int argc, char *argv[]){
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    signal(SIGCHLD,SIG_IGN);
    if (sockfd < 0){
        cerr << "failed to create socket\n";
        exit(1);
    }

    memset(&serverAddr, 0, sizeof(serverAddr));
    memset(&clientAddr, 0, sizeof(clientAddr));

    serverAddr.sin_family = PF_INET;
    serverAddr.sin_port = atoi(argv[1]);
    cout << "server port : " << serverAddr.sin_port << endl;
    serverAddr.sin_addr.s_addr = htons(INADDR_ANY);

    if(bind(sockfd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0){
        cerr << "failed to bind\n";
        exit(1);
    }
    cout << "wait for connection\n";
    addr_len = sizeof(clientAddr);
    while(true){
        recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
        if(fork()== 0){
            pid = getpid();
            break;
        }

    }
    //three way handshake

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    serverAddr.sin_port = atoi(argv[1]);

    ROVT.tv_sec = 1;
    ROVT.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &ROVT, sizeof(ROVT));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rwnd, sizeof(rwnd));
    bind(sockfd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));

    cout << "receive packet " << inet_ntoa(clientAddr.sin_addr) << " : " << clientAddr.sin_port << " : SYN : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
    cout << "(add client  " << inet_ntoa(clientAddr.sin_addr) << " : " << clientAddr.sin_port << " : (" << pid << "))\n";
    cout << "client port : " << pkt_recv.src_port << endl;


    cout << "(" << pid << ")" << "connecting" << endl;
    cout << "(" << pid << ")" << "cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
    while (true){
        pkt_send.ACK = true;
        pkt_send.PSH = false;
        pkt_send.SYN = true;
        pkt_send.FIN = false;
        srand(time(NULL));
        pkt_send.seq_num = rand() % 10000 + 1;
        pkt_send.ack_num = pkt_recv.seq_num + 1;
        if(rand()%gen>10){ 
        sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
        }
        else{
            cout << "packet loss\n";
        }
        cout << "send packet (" << pid << ")" << " : SYN : ACK : SEQ = " << pkt_send.seq_num << " : ACK =" << pkt_send.ack_num << endl;

        recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
        if(recv_len < 0){
            cout << "timeout\n";
            continue;
        }
        break;
    }

    cout << "receive packet (" << pid << ")" << " : ACK : SEQ : " << pkt_recv.seq_num << " : ACK =" << pkt_recv.ack_num << endl;
    cout << "(" << pid << ")" << "connected\n";
    if (mode == 0)
        cout << "slow start mode\n";
    else if(mode == 1)
        cout << "congestion avoidance mode\n";
    else
        cout << "fast recovery mode\n";

    //read command
    while(true){
        recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
        if (recv_len < 0){
            cout << "1timeout: retransmit" << endl; 
            cwnd = MSS;
            ssthresh = THRESHOLD/2;
            mode = 0;
            cout << "slow start mode : cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
            continue;
        }
        if (pkt_recv.FIN){//close connection
            cout << "receive packet : FIN : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
            pkt_send.seq_num = pkt_recv.ack_num;
            pkt_send.ack_num = pkt_recv.seq_num + 1;
            pkt_send.ACK = true;
            pkt_send.PSH = false;
            pkt_send.SYN = false;
            pkt_send.FIN = true;
            if(true){
            sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
            }
            else{
                cout << "packet loss\n";
            }
            cout << "send packet(" <<  pid << "): FIN : ACK : SEQ = " << pkt_send.seq_num << " : ACK = " << pkt_send.ack_num << endl;
            break;
        }
        if (!pkt_recv.PSH){//receive ack
            cout << "receive packet : ACK : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
            pkt_send.seq_num = pkt_recv.ack_num;
            pkt_send.ack_num = pkt_recv.seq_num + 1;

            if (mode == 0){//slow start
                cwnd += MSS;
                if (cwnd >= ssthresh){
                    mode = 1;
                    cout << "congestion avoidance mode\n";
                }
            }
            else if (mode == 1){//congestion avoidance
                cwnd += MSS * MSS / cwnd;
            }
            else{//fast recovery
                ssthresh = cwnd / 2;
                cwnd = ssthresh + 3 * MSS;
                mode = 1;
                cout << "congestion avoidance mode\n";
            }
            cout << "(" << pid << ")" << "cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
            continue;
        }
        else{//receive cmd  
            cout << "receive packet(" <<  pid <<  "):PSH : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
            cout << "(" << pid << ")" << pkt_recv.data << endl;
            cout << "(" << pid << ")" << "try file transmission\n";
            cout << "pkt_data: " << pkt_recv.data << endl;
            ifstream file(pkt_recv.data, ios::in | ios::binary);
            if (file.is_open()){
                
                pkt_send.seq_num = pkt_recv.ack_num;
                int f=1;
                while(!file.eof()){
                    file.read(pkt_send.data, MSS);
                    if(f){
                        pkt_send.ack_num = pkt_recv.seq_num + (static_cast< int>(pkt_recv.data[1])<<8) + (static_cast< int>(pkt_recv.data[0]));
                        f=0;
                    }
                    if(!file.gcount()){
                        break;
                    }
                    pkt_send.data_len = file.gcount();
                    file_pkts.push_back(pkt_send);
                    pkts_stat[pkt_send.seq_num] = 1;
                    pkt_send.seq_num += pkt_send.data_len;
                    memset(pkt_send.data, 0, MSS);
                    if(file.eof())
                        break;
                }
                notacked = file_pkts.size();
                pkt_send.ACK = false;
                pkt_send.PSH = true;
                pkt_send.SYN = false;
                pkt_send.FIN = false;
                bzero(pkt_send.data, sizeof(pkt_send.data));
                pkt_send.data_len = 0;
                pkt_send.ack_num = pkt_recv.seq_num + file_pkts.size();
                pkt_send.seq_num = pkt_recv.ack_num;    
                int lflag=1;
                while(true){
                    if(rand()%gen>10 ){
                    sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
                    }
                    else{
                        cout << "packet loss\n";
                    }
                    cout << "send packet (" <<  pid <<  "): PSH : SEQ = " << pkt_send.seq_num << " : ACK = " << pkt_send.ack_num << endl;
                    recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
                    if(recv_len){
                        cout << "receive packet (" <<  pid <<  "): ACK : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
                        break;
                    }
                    cout << "retry" << endl;
                }
                
                pthread_t recv_thread_t;
                pthread_create(&recv_thread_t, NULL, recv_thread, NULL);
                pthread_mutex_init(&Mutex, 0);
                while(true){
                    int send_cnt = 0;
                    int stat;
                    usleep(1000);
                    pthread_mutex_lock(&Mutex);
                    if (notacked == 0){
                        cout << "transmission complete\n";
                        pthread_mutex_unlock(&Mutex);
                        break;
                    }
                   //send pkt in cwnd
                    for(int ind=0;ind<file_pkts.size();ind++){
                        if (send_cnt >= cwnd/MSS)
                            break;
                        stat = pkts_stat[file_pkts[ind].seq_num];
                        if (stat == 0){
                            continue;
                        }
                        pkt_send = file_pkts[ind];
                        
                        if(lflag && pkt_send.seq_num ==pkts_stat.begin()->first+8192 || rand()%gen<10){ 
                            cout << "packet loss\n";
                            lflag=0;
                        }
                        else{
                            sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
                        }
                        if(!timer){
                            send_base = pkt_send.seq_num;
                            clock_gettime(CLOCK_REALTIME, &_t);
                            timer = _t.tv_sec*1000000+_t.tv_nsec/1000+100000;
                        }
                        cout << "send packet (" <<  pid <<  "): SEQ = " << pkt_send.seq_num << " : ACK = " << pkt_send.ack_num << endl;
                        if(stat == 1)
                            notacked++;
                        pkts_stat[file_pkts[ind].seq_num] = 2;

                        send_cnt++;
                        if(mode==2){
                            cout << "(" << pid << ") fast recovery mode" << endl;
                            cout << "cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
                        }
                    }
                    //check timer
                    cout << "send_base: " << send_base << endl;
                    clock_gettime(CLOCK_REALTIME, &_t);
                    long now = _t.tv_sec*1000000+_t.tv_nsec/1000;
                    if (now > timer){
                        stat = pkts_stat[send_base];
                        if(stat == 2){
                            timer = 0;
                            cout << "(timeout)" << endl;
                            dup_cnt = 0;
                            ssthresh = cwnd / 2;
                            cwnd = MSS;
                            mode = 0;
                            cout << "slow start mode (" <<  pid <<  "): cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
                        }
                    }   
                    pthread_mutex_unlock(&Mutex);
                }
                pthread_join(recv_thread_t, NULL);
                pthread_mutex_destroy(&Mutex);

                file_pkts.clear();
                pkts_stat.clear();
                timer = 0;
                rwnd = RCVBUF;
            }
            else{
                cout << "(" << pid << ")" << "file not exist\n";
                cout << "(" << pid << ")" << "try dns lookup\n";

                string hostname = pkt_recv.data;
                struct addrinfo hints, *res; 
                memset(&hints, 0, sizeof(hints));
                hints.ai_family = AF_INET;
                hints.ai_socktype = SOCK_STREAM;
                if(!getaddrinfo(hostname.c_str(), NULL, &hints, &res)){
                    cout << "("<< pid  << ") success"  << endl;
                    
                    for(struct addrinfo *p = res; p != NULL; p = p->ai_next){
                        struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
                        void *addr = &(ipv4->sin_addr);
                        char ipstr[INET_ADDRSTRLEN];
                        inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
                        strcpy(pkt_send.data, ipstr);
                        cout << "("<< pid  << ") " << ipstr  << endl;
                    }



                    pkt_send.ACK = true;
                    pkt_send.PSH = true;
                    pkt_send.SYN = false;
                    pkt_send.FIN = true;
                    pkt_send.seq_num = pkt_recv.ack_num;
                    pkt_send.ack_num = pkt_recv.seq_num + strlen(pkt_recv.data);
                    if(rand()%gen>10){
                    sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
                    }
                    else{
                        cout << "packet loss\n";
                    }
                    cout << "(" << pid << ")" << "send packet : PSH : ACK : SEQ = " << pkt_send.seq_num << " ACK = " << pkt_send.ack_num << endl;

                }
                else{
                    cout << "(" << pid << ")" << "fail" << endl;
                    cout << "(" << pid << ")" << "try calculattion\n";
                    calc(pkt_recv.data);
                }
            }

        }

    }
    //disconnect
    usleep(50000);
    while(true){
        pkt_send.ACK = false;
        pkt_send.PSH = false;
        pkt_send.SYN = false;
        pkt_send.FIN = true;
        
        if(rand()%gen>10){
        sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
        }
        else{
            cout << "packet loss\n";
        }
        cout << "send packet (" <<  pid <<  "): FIN : SEQ = " << pkt_send.seq_num << " : ACK = " << pkt_send.ack_num << endl;
        recv_len = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
        if(recv_len < 0){
            cout << "(timeout)" << endl;
            continue;
        }
        cout << "receive packet (" <<  pid <<  "): FIN : ACK : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
        break;
    }
    cout << "(" << pid << ")" << "disconnect\n";
    close(sockfd);
    return 0;
}

void *recv_thread( void *arg){
    while (true){
        int rlen;
        rlen = recvfrom(sockfd, (void *)&pkt_recv, sizeof(pkt_recv), 0, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
        pthread_mutex_lock(&Mutex);
        if(rlen>0){
            int head, tail,target;
            head = pkts_stat.begin()->first;
            tail = pkts_stat.rbegin()->first;
            target = pkt_recv.ack_num-MSS;
            if(!(target<head || target>tail)){
                int stat = pkts_stat[pkt_recv.ack_num-MSS];
                if (stat == 0){
                    cout << "receive packet (" <<  pid <<  "): ACK : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num ;
                    if(pre_ack == pkt_recv.ack_num){
                        dup_cnt++;
                        cout << " : duplicate ACK " << dup_cnt << endl;
                        if(dup_cnt == 3){
                            timer = 0;
                            cout << "fast retransmit mode\n";
                            ssthresh = cwnd / 2;
                            cwnd = ssthresh + 3 * MSS;
                            mode = 2;
                        }
                    }
                    else{
                        cout << endl;
                        dup_cnt = 0;
                    }
                    
                }
                else if(stat == 2){
                    cout << "receive packet (" <<  pid <<  "): ACK : SEQ = " << pkt_recv.seq_num << " : ACK = " << pkt_recv.ack_num << endl;
                    rwnd-=pkt_recv.data_len;
                    if (send_base == pkt_recv.ack_num - MSS){
                        timer = 0;
                    }
                    if(mode==0){
                        cwnd += MSS;
                        if(cwnd >= ssthresh){
                            mode = 1;
                            cout << "congestion avoidance mode\n";
                        }
                    }
                    else if(mode == 1){
                        cwnd += MSS * MSS / cwnd;
                    }
                    else{
                        cwnd = ssthresh;
                        mode = 1;
                        cout << "congestion avoidance mode" << endl;
                        cout << "cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
                    }
                    cout << "(" <<  pid <<  ")cwnd = " << cwnd << " rwnd = " << rwnd << " threshold = " << ssthresh << endl;
                    pkts_stat[pkt_recv.ack_num-MSS] = 0;
                    notacked-=2;
                }
            }
            if(!notacked){
                cout << "finish sending file" << endl;
                timer = 0;
                pthread_mutex_unlock(&Mutex);
                break;
            }
            pre_ack = pkt_recv.ack_num;
        }
        pthread_mutex_unlock(&Mutex);
    }
    cout << "thread quit" << endl;
    pthread_exit(NULL);
}

void calc(string expression){
    //1+2+3
     std::stack<double> numbers;
    std::stack<char> operators;

    std::istringstream iss(expression);
    char c;
    while (iss >> c) {
        if (std::isspace(c)) // Skip spaces
            continue;

        if (std::isdigit(c)) {
            iss.putback(c);
            double num;
            iss >> num;
            numbers.push(num);
        } else if (c == '(') {
            operators.push(c);
        } else if (c == ')') {
            while (!operators.empty() && operators.top() != '(') {
                char op = operators.top();
                operators.pop();
                double b = numbers.top();
                numbers.pop();
                double a = numbers.top();
                numbers.pop();
                if (op == '+')
                    numbers.push(a + b);
                else if (op == '-')
                    numbers.push(a - b);
                else if (op == '*')
                    numbers.push(a * b);
                else if (op == '/')
                    numbers.push(a / b);
                else if (op == '^')
                    numbers.push(std::pow(a, b));
            }
            operators.pop(); // Discard '('
        } else if (c == '*' || c == '/' || c == '^') { // Operator
            while (!operators.empty() && operators.top() != '(' &&
                   ((c != '^' && (operators.top() == '*' || operators.top() == '/')) ||
                   (c == '^' && operators.top() == '^'))) {
                char op = operators.top();
                operators.pop();
                double b = numbers.top();
                numbers.pop();
                double a = numbers.top();
                numbers.pop();
                if (op == '+')
                    numbers.push(a + b);
                else if (op == '-')
                    numbers.push(a - b);
                else if (op == '*')
                    numbers.push(a * b);
                else if (op == '/')
                    numbers.push(a / b);
                else if (op == '^')
                    numbers.push(std::pow(a, b));
            }
            operators.push(c);
        } else { // Operator
            while (!operators.empty() && operators.top() != '(' &&
                   ((c != '^' && (operators.top() == '*' || operators.top() == '/')) ||
                   (c == '^' && operators.top() == '^'))) {
                char op = operators.top();
                operators.pop();
                double b = numbers.top();
                numbers.pop();
                double a = numbers.top();
                numbers.pop();
                if (op == '+')
                    numbers.push(a + b);
                else if (op == '-')
                    numbers.push(a - b);
                else if (op == '*')
                    numbers.push(a * b);
                else if (op == '/')
                    numbers.push(a / b);
                else if (op == '^')
                    numbers.push(std::pow(a, b));
            }
            operators.push(c);
        }
    }

    while (!operators.empty()) {
        char op = operators.top();
        operators.pop();
        double b = numbers.top();
        numbers.pop();
        double a = numbers.top();
        numbers.pop();
        if (op == '+')
            numbers.push(a + b);
        else if (op == '-')
            numbers.push(a - b);
        else if (op == '*')
            numbers.push(a * b);
        else if (op == '/')
            numbers.push(a / b);
        else if (op == '^')
            numbers.push(std::pow(a, b));
    }

    pkt_send.ACK = true;
    pkt_send.PSH = true;
    pkt_send.SYN = false;
    pkt_send.FIN = true;
    pkt_send.seq_num = pkt_recv.ack_num;
    pkt_send.ack_num = pkt_recv.seq_num + strlen(pkt_recv.data);
    std::ostringstream oss;
    oss << numbers.top();
    strcpy(pkt_send.data, oss.str().c_str());

    if(rand()%gen>10){
    sendto(sockfd, (void *)&pkt_send, sizeof(pkt_send), 0, (struct sockaddr *)&clientAddr, addr_len);
    }
    else{
        cout << "packet loss\n";
    }
    cout << "(" << pid << ")" << "send packet (" <<  pid <<  "): PSH : ACK : SEQ = " << pkt_send.seq_num << " ACK = " << pkt_send.ack_num << endl;

}

