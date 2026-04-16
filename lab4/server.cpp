#include <iostream>
#include <vector>
#include <thread>
#include <winsock2.h>
#include <chrono>
#include <map>
#include <mutex>
#include <memory>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

using namespace std;
using namespace chrono;

struct DataHeader
{
    uint32_t matrix_size;
    uint32_t thread_count;
    uint32_t data_length;
};

struct ClientData
{
    vector<vector<int>> matrix;
    vector<int> thread_config;
    vector<double> results;
    int current_thread = 0;
    bool is_processing = false;
    mutex mtx;
};

map<SOCKET, shared_ptr<ClientData>> clients;
mutex clientsMapMutex;

int recvAll(SOCKET sock, char *buffer, int length)
{
    int total = 0;
    while (total < length)
    {
        int received = recv(sock, buffer + total, length - total, 0);
        if (received <= 0)
            return -1;
        total += received;
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
    if (len == 0 || len > 65536)
        return "";
    string msg(len, '\0');
    if (recvAll(sock, &msg[0], (int)len) != (int)len)
        return "";
    return msg;
}

void processMatrixSection(int startCol, int endCol,
                          const vector<vector<int>> &original,
                          vector<vector<int>> &result)
{
    int n = (int)original.size();
    for (int col = startCol; col < endCol; ++col)
    {
        int colMax = original[0][col];
        for (int row = 1; row < n; ++row)
            if (original[row][col] > colMax)
                colMax = original[row][col];
        result[col][col] = colMax;
    }
}

void handleClient(SOCKET clientSocket)
{
    auto data = make_shared<ClientData>();
    {
        lock_guard<mutex> lock(clientsMapMutex);
        clients[clientSocket] = data;
    }

    cout << "[SERVER] Handling client #" << clientSocket << endl;

    try
    {
        while (true)
        {
            string command = recvMsg(clientSocket);
            if (command.empty())
            {
                cout << "[SERVER] Client #" << clientSocket << " disconnected (empty msg)" << endl;
                break;
            }

            cout << "[CLIENT #" << clientSocket << "] " << command << endl;

            if (command == "HELLO")
            {
                sendMsg(clientSocket, "CONNECTED");
            }

            else if (command == "SEND_DATA")
            {
                DataHeader header;
                if (recvAll(clientSocket, reinterpret_cast<char *>(&header), sizeof(header)) != sizeof(header))
                    throw runtime_error("Invalid header");

                header.matrix_size = ntohl(header.matrix_size);
                header.thread_count = ntohl(header.thread_count);
                header.data_length = ntohl(header.data_length);

                if (header.data_length != header.matrix_size * header.matrix_size * sizeof(int))
                    throw runtime_error("Data length mismatch");

                vector<int> threadConfig(header.thread_count);
                if (recvAll(clientSocket,
                            reinterpret_cast<char *>(threadConfig.data()),
                            header.thread_count * sizeof(int)) != (int)(header.thread_count * sizeof(int)))
                    throw runtime_error("Failed to receive thread config");
                for (int &t : threadConfig)
                    t = ntohl(t);

                vector<int> flatMatrix(header.matrix_size * header.matrix_size);
                if (recvAll(clientSocket,
                            reinterpret_cast<char *>(flatMatrix.data()),
                            header.data_length) != (int)header.data_length)
                    throw runtime_error("Failed to receive matrix data");

                {
                    lock_guard<mutex> lock(data->mtx);
                    data->matrix.assign(header.matrix_size, vector<int>(header.matrix_size));
                    for (uint32_t i = 0; i < header.matrix_size; ++i)
                        for (uint32_t j = 0; j < header.matrix_size; ++j)
                            data->matrix[i][j] = ntohl(flatMatrix[i * header.matrix_size + j]);
                    data->thread_config = threadConfig;
                    data->results.clear();
                }

                sendMsg(clientSocket, "DATA_RECEIVED");
            }

            else if (command == "START_COMPUTATION")
            {
                {
                    lock_guard<mutex> lock(data->mtx);
                    if (data->matrix.empty() || data->thread_config.empty())
                        throw runtime_error("Data not initialized");
                    if (data->is_processing)
                        throw runtime_error("Already processing");
                    data->is_processing = true;
                    data->current_thread = 0;
                }

                shared_ptr<ClientData> dataCopy = data;

                thread([dataCopy, clientSocket]()
                       {
                    vector<int> config;
                    vector<vector<int>> matrix;
                    {
                        lock_guard<mutex> lock(dataCopy->mtx);
                        config = dataCopy->thread_config;
                        matrix = dataCopy->matrix;
                    }

                    for (size_t i = 0; i < config.size(); ++i) {
                        {
                            lock_guard<mutex> lock(dataCopy->mtx);
                            dataCopy->current_thread = (int)i;
                        }

                        int threadsCount = config[i];
                        int n = (int)matrix.size();

                        auto start = high_resolution_clock::now();

                        vector<vector<int>> result = matrix;
                        vector<thread> threads;

                        int colsPerThread = n / threadsCount;
                        int extraCols     = n % threadsCount;

                        for (int t = 0; t < threadsCount; ++t) {
                            int startCol = t * colsPerThread + min(t, extraCols);
                            int endCol   = startCol + colsPerThread + (t < extraCols ? 1 : 0);
                            threads.emplace_back(processMatrixSection,
                                                 startCol, endCol,
                                                 cref(matrix), ref(result));
                        }
                        for (auto& th : threads) th.join();

                        auto end = high_resolution_clock::now();
                        double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;

                        {
                            lock_guard<mutex> lock(dataCopy->mtx);
                            dataCopy->results.push_back(elapsed);
                        }

                        string progress = "PROGRESS: " + to_string(threadsCount) +
                                          " threads, time: " + to_string(elapsed) + "s";
                        sendMsg(clientSocket, progress);
                    }

                    {
                        lock_guard<mutex> lock(dataCopy->mtx);
                        dataCopy->is_processing = false;
                    }

                    sendMsg(clientSocket, "COMPUTATION_COMPLETE");
                    cout << "[CLIENT #" << clientSocket << "] COMPUTATION_COMPLETE" << endl; })
                    .detach();

                sendMsg(clientSocket, "COMPUTATION_STARTED");
            }

            else if (command == "GET_STATUS")
            {
                lock_guard<mutex> lock(data->mtx);
                if (!data->is_processing)
                {
                    sendMsg(clientSocket, "STATUS: COMPLETED");
                }
                else
                {
                    string status = "STATUS: " +
                                    to_string(data->current_thread + 1) + "/" +
                                    to_string(data->thread_config.size());
                    sendMsg(clientSocket, status);
                }
            }

            else if (command == "GET_RESULT")
            {
                lock_guard<mutex> lock(data->mtx);
                string result = "RESULT:\nMatrix size: " +
                                to_string(data->matrix.size()) + "x" +
                                to_string(data->matrix.size());
                for (size_t i = 0; i < data->thread_config.size(); ++i)
                {
                    result += "\n" + to_string(data->thread_config[i]) +
                              " threads: " + to_string(data->results[i]) + " seconds";
                }
                sendMsg(clientSocket, result);
            }

            else
            {
                sendMsg(clientSocket, "ERROR: Unknown command: " + command);
            }
        }
    }
    catch (const exception &e)
    {
        cerr << "[ERROR] Client #" << clientSocket << ": " << e.what() << endl;
        sendMsg(clientSocket, string("ERROR: ") + e.what());
    }

    closesocket(clientSocket);
    {
        lock_guard<mutex> lock(clientsMapMutex);
        clients.erase(clientSocket);
    }
    cout << "[SERVER] Client #" << clientSocket << " removed." << endl;
}

int main()
{
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        cerr << "WSAStartup failed" << endl;
        return 1;
    }

    SOCKET serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == INVALID_SOCKET)
    {
        cerr << "socket() failed" << endl;
        WSACleanup();
        return 1;
    }

    int opt = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<char *>(&opt), sizeof(opt));

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8080);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverSocket, (sockaddr *)&serverAddr, sizeof(serverAddr)) != 0)
    {
        cerr << "bind() failed: " << WSAGetLastError() << endl;
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    listen(serverSocket, 10);
    cout << "Server started on port 8080" << endl;

    while (true)
    {
        SOCKET clientSocket = accept(serverSocket, nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET)
        {
            cerr << "[SERVER] accept() failed: " << WSAGetLastError() << endl;
            continue;
        }
        cout << "[SERVER] New client connected. Socket: " << clientSocket << endl;
        thread(handleClient, clientSocket).detach();
    }

    closesocket(serverSocket);
    WSACleanup();
    return 0;
}