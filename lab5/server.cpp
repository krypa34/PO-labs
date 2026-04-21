#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#define PORT 7890
#define ROOT_DIR "static"

using namespace std;

string readFile(const string& path) {
    ifstream file(path, ios::binary);
    if (!file.is_open()) return "";
    ostringstream ss;
    ss << file.rdbuf();
    return ss.str();
}

void sendResponse(SOCKET client, const string& status, const string& content) {
    ostringstream response;
    response << "HTTP/1.1 " << status << "\r\n";
    response << "Content-Length: " << content.size() << "\r\n";
    response << "Content-Type: text/html\r\n\r\n";
    response << content;
    send(client, response.str().c_str(), response.str().length(), 0);
}

void handleClient(SOCKET clientSocket) {
    char buffer[4096];
    int received = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
        closesocket(clientSocket);
        return;
    }
    buffer[received] = '\0';

    istringstream request(buffer);
    string method, path, version;
    request >> method >> path >> version;

    if (method != "GET") {
        string body = "<html><body><h1>405 Method Not Allowed</h1></body></html>";
        sendResponse(clientSocket, "405 Method Not Allowed", body);
        closesocket(clientSocket);
        return;
    }

    if (path == "/") path = "/index.html";

    string filePath = string(ROOT_DIR) + path;

    string content = readFile(filePath);
    if (content.empty()) {
        string body = "<html><body><h1>404 Not Found</h1></body></html>";
        sendResponse(clientSocket, "404 Not Found", body);
    } else {
        sendResponse(clientSocket, "200 OK", content);
    }

    closesocket(clientSocket);
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cerr << "WSAStartup failed." << endl;
        return 1;
    }

    SOCKET serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (serverSocket == INVALID_SOCKET) {
        cerr << "Socket creation failed." << endl;
        WSACleanup();
        return 1;
    }

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(PORT);

    if (bind(serverSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        cerr << "Bind failed." << endl;
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
        cerr << "Listen failed." << endl;
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    cout << "Server started at http://localhost:" << PORT << endl;

    while (true) {
        SOCKET clientSocket = accept(serverSocket, NULL, NULL);
        if (clientSocket == INVALID_SOCKET) {
            cerr << "Accept failed." << endl;
            continue;
        }
        thread(handleClient, clientSocket).detach();
    }

    closesocket(serverSocket);
    WSACleanup();
    return 0;
}