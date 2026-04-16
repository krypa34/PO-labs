#include <atomic>
#include <iostream>
#include <winsock2.h>
#include <vector>
#include <thread>
#include <string>

#pragma comment(lib, "ws2_32.lib")

using namespace std;

struct DataHeader
{
    uint32_t matrix_size;
    uint32_t thread_count;
    uint32_t data_length;
};

int sendAll(SOCKET sock, const char *data, int length)
{
    int total = 0;
    while (total < length)
    {
        int sent = send(sock, data + total, length - total, 0);
        if (sent <= 0)
            return -1;
        total += sent;
    }
    return total;
}

int recvAll(SOCKET sock, char *buffer, int length)
{
    int total = 0;
    while (total < length)
    {
        int r = recv(sock, buffer + total, length - total, 0);
        if (r <= 0)
            return -1;
        total += r;
    }
    return total;
}

bool sendMsg(SOCKET sock, const string &msg)
{
    uint32_t netLen = htonl((uint32_t)msg.size());
    if (send(sock, reinterpret_cast<char *>(&netLen), sizeof(netLen), 0) != sizeof(netLen))
        return false;
    if (send(sock, msg.c_str(), (int)msg.size(), 0) != (int)msg.size())
        return false;
    return true;
}

string recvMsg(SOCKET sock)
{
    uint32_t netLen = 0;
    if (recvAll(sock, reinterpret_cast<char *>(&netLen), sizeof(netLen)) != sizeof(netLen))
        return "";
    uint32_t len = ntohl(netLen);
    if (len == 0 || len > 1024 * 1024)
        return "";
    string msg(len, '\0');
    if (recvAll(sock, &msg[0], (int)len) != (int)len)
        return "";
    return msg;
}

void clientThread()
{
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);

    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8080);
    serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock, (sockaddr *)&serverAddr, sizeof(serverAddr)) != 0)
    {
        cerr << "Connection failed: " << WSAGetLastError() << endl;
        WSACleanup();
        return;
    }
    cout << "Connected to server" << endl;

    if (!sendMsg(sock, "HELLO"))
    {
        cerr << "Failed to send HELLO" << endl;
        return;
    }
    string resp = recvMsg(sock);
    if (resp.empty())
    {
        cerr << "No response to HELLO" << endl;
        return;
    }
    cout << "[SERVER] " << resp << endl;

    int matrixSize = 10000;
    vector<vector<int>> matrix(matrixSize, vector<int>(matrixSize));
    srand(42);
    for (auto &row : matrix)
        for (auto &val : row)
            val = rand() % 1001;

    vector<int> threadConfig = {1, 2, 4, 8, 16, 32, 64};

    if (!sendMsg(sock, "SEND_DATA"))
    {
        cerr << "Failed to send SEND_DATA" << endl;
        return;
    }

    DataHeader header;
    header.matrix_size = htonl((uint32_t)matrixSize);
    header.thread_count = htonl((uint32_t)threadConfig.size());
    header.data_length = htonl((uint32_t)(matrixSize * matrixSize * sizeof(int)));

    sendAll(sock, reinterpret_cast<char *>(&header), sizeof(header));

    vector<int> configNet(threadConfig.size());
    for (size_t i = 0; i < threadConfig.size(); ++i)
        configNet[i] = htonl(threadConfig[i]);
    sendAll(sock, reinterpret_cast<char *>(configNet.data()),
            (int)(configNet.size() * sizeof(int)));

    vector<int> flatNet(matrixSize * matrixSize);
    for (int i = 0; i < matrixSize; ++i)
        for (int j = 0; j < matrixSize; ++j)
            flatNet[i * matrixSize + j] = htonl(matrix[i][j]);
    sendAll(sock, reinterpret_cast<char *>(flatNet.data()),
            (int)(flatNet.size() * sizeof(int)));

    resp = recvMsg(sock);
    if (resp.empty())
    {
        cerr << "No response to SEND_DATA" << endl;
        return;
    }
    cout << "[SERVER] " << resp << endl;

    if (!sendMsg(sock, "START_COMPUTATION"))
    {
        cerr << "Failed to send START_COMPUTATION" << endl;
        return;
    }

    resp = recvMsg(sock);
    if (resp.empty())
    {
        cerr << "No response to START_COMPUTATION" << endl;
        return;
    }
    cout << "[SERVER] " << resp << endl;

    atomic<bool> done(false);

    thread listener([&sock, &done]()
                    {
        while (!done) {
            string msg = recvMsg(sock);
            if (msg.empty()) {
                cerr << "[LISTENER] Connection lost" << endl;
                done = true;
                break;
            }
            cout << "[SERVER] " << msg << endl;
            if (msg.find("COMPUTATION_COMPLETE") != string::npos) {
                done = true;
            }
        } });

    while (!done)
    {
        this_thread::sleep_for(chrono::seconds(2));
        if (!done)
        {
            sendMsg(sock, "GET_STATUS");
        }
    }

    listener.join();

    if (!sendMsg(sock, "GET_RESULT"))
    {
        cerr << "Failed to send GET_RESULT" << endl;
    }
    else
    {
        string result = recvMsg(sock);
        if (result.empty())
        {
            cerr << "No result received" << endl;
        }
        else
        {
            cout << "\n========== RESULT ==========\n"
                 << result << "\n============================" << endl;
        }
    }

    this_thread::sleep_for(chrono::milliseconds(200));

    closesocket(sock);
    WSACleanup();
    cout << "Done." << endl;
}

int main()
{
    thread(clientThread).join();
    cout << "\nPress Enter to exit...";
    cin.get();
    return 0;
}