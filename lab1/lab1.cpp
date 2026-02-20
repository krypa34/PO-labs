#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <windows.h>
#include <intrin.h>
#include <iomanip>
#include <algorithm>

#define SeedNum 7
#define ShouldCheckCorrectness 1

using namespace std;
using chrono::duration_cast;
using chrono::high_resolution_clock;
using chrono::nanoseconds;

void printCacheInfo()
{
    int CPUInfo[4];
    __cpuid(CPUInfo, 0);
    int nIds = CPUInfo[0];

    for (int i = 0; i <= nIds; i++)
    {
        __cpuidex(CPUInfo, i, 0);
        if (CPUInfo[0] == 4)
        {
            int cacheLevel = (CPUInfo[0] >> 5) & 0x7;
            int cacheType = CPUInfo[0] & 0xFF;
            if (cacheType == 1 || cacheType == 2 || cacheType == 3)
            {
                cout << "Cache Level: " << cacheLevel << ", Type: "
                     << (cacheType == 1 ? "Data" : (cacheType == 2 ? "Instruction" : "Unified"))
                     << ", Size: " << ((CPUInfo[1] + 1) * 8) << " KB" << endl;
            }
        }
    }
}

void printSystemInfo(int &cpuNum)
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    cpuNum = (int)sysinfo.dwNumberOfProcessors;

    cout << "System Information:" << endl;
    cout << "Number of logical processors: " << sysinfo.dwNumberOfProcessors << endl;
    cout << "Processor architecture: "
         << (sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_INTEL ? "x86" : sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_AMD64 ? "x64"
                                                                                                                                                     : "Unknown")
         << endl;
    cout << "Page size: " << sysinfo.dwPageSize << " bytes" << endl;
    cout << "Minimum application address: " << sysinfo.lpMinimumApplicationAddress << endl;
    cout << "Maximum application address: " << sysinfo.lpMaximumApplicationAddress << endl;

    MEMORYSTATUSEX memInfo;
    memInfo.dwLength = sizeof(MEMORYSTATUSEX);
    GlobalMemoryStatusEx(&memInfo);

    cout << "Total physical memory: " << memInfo.ullTotalPhys / (1024 * 1024) << " MB" << endl;
    cout << "Available physical memory: " << memInfo.ullAvailPhys / (1024 * 1024) << " MB" << endl;

    printCacheInfo();
}

void processColumnSection(int startCol, int endCol,
                          const vector<vector<int>> &primaryMatrix,
                          vector<vector<int>> &matrix)
{
    const int n = (int)matrix.size();
    for (int col = startCol; col < endCol; ++col)
    {
        int mx = primaryMatrix[0][col];
        for (int row = 1; row < n; ++row)
        {
            mx = max(mx, primaryMatrix[row][col]);
        }
        matrix[col][col] = mx;
    }
}

bool checkMatrixCorrectness(const vector<vector<int>> &matrix,
                            const vector<vector<int>> &primaryMatrix,
                            int randomColCount = 10)
{
    const int n = (int)matrix.size();
    bool isCorrect = true;

    for (int k = 0; k < randomColCount; ++k)
    {
        int col = rand() % n;

        int mx = primaryMatrix[0][col];
        for (int row = 1; row < n; ++row)
        {
            mx = max(mx, primaryMatrix[row][col]);
        }

        if (matrix[col][col] != mx)
        {
            cout << "Error in column " << col
                 << ": Expected " << mx
                 << ", but got " << matrix[col][col] << endl;
            isCorrect = false;
        }
    }
    return isCorrect;
}

void linearProcessMatrix(vector<vector<int>> &matrix)
{
    const int n = (int)matrix.size();
    for (int col = 0; col < n; ++col)
    {
        int mx = matrix[0][col];
        for (int row = 1; row < n; ++row)
        {
            mx = max(mx, matrix[row][col]);
        }
        matrix[col][col] = mx;
    }
}

int main()
{
    int cpuNum;
    printSystemInfo(cpuNum);

    vector<int> matrixSizes = {
        100,
        1000,
        5000,
        20000,
        50000};

    vector<int> numCPUArr = {
        1,
        max(1, cpuNum / 2),
        max(1, cpuNum),
        max(1, cpuNum * 2),
        max(1, cpuNum * 4),
        max(1, cpuNum * 8),
        max(1, cpuNum * 16),
    };

    cout << "\nTest Results:" << endl;
    cout << "Matrix Size\tThreads\tTime (seconds)\tCorrect?" << endl;

    for (int matrixSize : matrixSizes)
    {
        vector<vector<int>> primaryMatrix(matrixSize, vector<int>(matrixSize));
        srand(SeedNum);
        for (int i = 0; i < matrixSize; i++)
        {
            for (int j = 0; j < matrixSize; j++)
            {
                primaryMatrix[i][j] = rand() % 10001;
            }
        }

        // Linear
        {
            vector<vector<int>> copiedMatrix = primaryMatrix;
            auto start = high_resolution_clock::now();
            linearProcessMatrix(copiedMatrix);
            auto end = high_resolution_clock::now();

            string correctness = ShouldCheckCorrectness
                                     ? (checkMatrixCorrectness(copiedMatrix, primaryMatrix) ? "Yes" : "No")
                                     : "Unknown";

            double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;
            cout << "\n"
                 << matrixSize << "\t\tLinear\t"
                 << fixed << setprecision(6) << elapsed << "\t" << correctness << endl;
        }

        // Multithread
        for (int threadsCount : numCPUArr)
        {
            if (threadsCount <= 0)
                continue;
            if (threadsCount > matrixSize)
                threadsCount = matrixSize;

            vector<vector<int>> copiedMatrix = primaryMatrix;
            vector<thread> threads;

            auto start = high_resolution_clock::now();

            int colsPerThread = matrixSize / threadsCount;
            int extraCols = matrixSize % threadsCount;

            for (int t = 0; t < threadsCount; ++t)
            {
                int startCol = t * colsPerThread + min(t, extraCols);
                int endCol = startCol + colsPerThread + (t < extraCols ? 1 : 0);

                threads.emplace_back(processColumnSection,
                                     startCol, endCol,
                                     cref(primaryMatrix),
                                     ref(copiedMatrix));
            }

            for (auto &th : threads)
            {
                if (th.joinable())
                    th.join();
            }

            auto end = high_resolution_clock::now();

            string correctness = ShouldCheckCorrectness
                                     ? (checkMatrixCorrectness(copiedMatrix, primaryMatrix) ? "Yes" : "No")
                                     : "Unknown";

            double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;
            cout << matrixSize << "\t\t" << threadsCount << "\t"
                 << fixed << setprecision(6) << elapsed << "\t" << correctness << endl;
        }
    }

    cout << "\nPress Enter to exit...";
    cin.get();
    return 0;
}