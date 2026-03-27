#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <cstdlib>
#include <ctime>
#include <iomanip>

using namespace std;
using chrono::duration_cast;
using chrono::high_resolution_clock;
using chrono::nanoseconds;

void linearExecution(const vector<int> &data, int &xorResult);
void parallelWithMutex(const vector<int> &data, int &xorResult, int numThreads);
void parallelWithCAS(const vector<int> &data, int &xorResult, int numThreads);

void processSectionWithMutex(int start, int end, const vector<int> &data, int &globalXor, mutex &mtx);
void processSectionWithCAS(int start, int end, const vector<int> &data, atomic<int> &atomicXor);

int main()
{
    vector<int> matrixSizes = {10000, 1000000, 100000000, 2000000000};
    vector<int> threadCounts = {8, 16, 32, 64, 128, 256};

    cout << "\nTest Results:" << endl;
    cout << "Matrix Size\tThreads\tMode\tTime (seconds)\tXOR Result" << endl;

    for (int matrixSize : matrixSizes)
    {
        vector<int> data(matrixSize);
        srand(static_cast<unsigned>(time(nullptr)));
        for (int i = 0; i < matrixSize; ++i)
        {
            data[i] = rand() % 1001;
        }

        int xorResult = 0;
        auto start = high_resolution_clock::now();
        linearExecution(data, xorResult);
        auto end = high_resolution_clock::now();
        double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;
        cout << matrixSize << "\t\t-\tLinear\t" << fixed << setprecision(6)
             << elapsed << "\t" << xorResult << endl;

        cout << endl;

        for (int numThreads : threadCounts)
        {
            int xorResult = 0;
            auto start = high_resolution_clock::now();
            parallelWithMutex(data, xorResult, numThreads);
            auto end = high_resolution_clock::now();
            double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;
            cout << matrixSize << "\t\t" << numThreads << "\tMutex\t"
                 << fixed << setprecision(6) << elapsed << "\t" << xorResult << endl;
        }

        cout << endl;

        for (int numThreads : threadCounts)
        {
            int xorResult = 0;
            auto start = high_resolution_clock::now();
            parallelWithCAS(data, xorResult, numThreads);
            auto end = high_resolution_clock::now();
            double elapsed = duration_cast<nanoseconds>(end - start).count() * 1e-9;
            cout << matrixSize << "\t\t" << numThreads << "\tCAS\t"
                 << fixed << setprecision(6) << elapsed << "\t" << xorResult << endl;
        }

        cout << endl
             << endl;
    }

    cout << "\nPress Enter to exit...";
    cin.get();
    return 0;
}

void linearExecution(const vector<int> &data, int &xorResult)
{
    xorResult = 0;
    for (int value : data)
    {
        if (value % 7 == 0)
        {
            xorResult ^= value;
        }
    }
}

void processSectionWithMutex(int start, int end, const vector<int> &data, int &globalXor, mutex &mtx)
{
    for (int i = start; i < end; ++i)
    {
        if (data[i] % 7 == 0)
        {
            lock_guard<mutex> lock(mtx);
            globalXor ^= data[i];
        }
    }
}

void parallelWithMutex(const vector<int> &data, int &xorResult, int numThreads)
{
    xorResult = 0;
    mutex mtx;
    vector<thread> threads;

    int chunkSize = data.size() / numThreads;
    for (int t = 0; t < numThreads; ++t)
    {
        int start = t * chunkSize;
        int end = (t == numThreads - 1) ? data.size() : start + chunkSize;
        threads.emplace_back(processSectionWithMutex, start, end, cref(data), ref(xorResult), ref(mtx));
    }

    for (auto &th : threads)
    {
        if (th.joinable())
        {
            th.join();
        }
    }
}

void processSectionWithCAS(int start, int end, const vector<int> &data, atomic<int> &atomicXor)
{
    int localXor = 0;

    for (int i = start; i < end; ++i)
    {
        if (data[i] % 7 == 0)
        {
            localXor ^= data[i];
        }
    }

    int expected = atomicXor.load(memory_order_relaxed);
    int desired;

    do
    {
        desired = expected ^ localXor;
    } while (!atomicXor.compare_exchange_weak(expected, desired, memory_order_relaxed));
}

void parallelWithCAS(const vector<int> &data, int &xorResult, int numThreads)
{
    atomic<int> atomicXor(0);
    vector<thread> threads;

    int chunkSize = data.size() / numThreads;
    for (int t = 0; t < numThreads; ++t)
    {
        int start = t * chunkSize;
        int end = (t == numThreads - 1) ? data.size() : start + chunkSize;
        threads.emplace_back(processSectionWithCAS, start, end, cref(data), ref(atomicXor));
    }

    for (auto &th : threads)
    {
        if (th.joinable())
        {
            th.join();
        }
    }

    xorResult = atomicXor.load();
}