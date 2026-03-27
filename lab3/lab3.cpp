#include <iostream>
#include <queue>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <shared_mutex>
#include <atomic>
#include <cstdlib>
#include <ctime>
#include <limits>

using namespace std;

using read_write_lock = shared_mutex;
using read_lock = shared_lock<read_write_lock>;
using write_lock = unique_lock<read_write_lock>;

atomic<bool> stop_program(false);
atomic<int> global_task_id(0);
mutex cout_mutex;

struct Task
{
    int id;
    function<void()> task;
};

template <typename task_type_t>
class task_queue
{
    using task_queue_implementation = queue<task_type_t>;

public:
    task_queue() = default;
    ~task_queue() { clear(); }

    bool empty() const
    {
        read_lock lock(m_rw_lock);
        return m_tasks.empty();
    }

    size_t size() const
    {
        read_lock lock(m_rw_lock);
        return m_tasks.size();
    }

    void clear()
    {
        write_lock lock(m_rw_lock);
        while (!m_tasks.empty())
        {
            m_tasks.pop();
        }
    }

    bool pop(task_type_t &task)
    {
        write_lock lock(m_rw_lock);
        if (m_tasks.empty())
        {
            return false;
        }
        else
        {
            task = m_tasks.front();
            m_tasks.pop();
            return true;
        }
    }

    bool pop()
    {
        write_lock lock(m_rw_lock);
        if (m_tasks.empty())
        {
            return false;
        }
        else
        {
            m_tasks.pop();
            return true;
        }
    }

    bool push(const task_type_t &task, size_t max_size)
    {
        write_lock lock(m_rw_lock);
        if (m_tasks.size() >= max_size)
        {
            return false;
        }
        m_tasks.push(task);
        return true;
    }

private:
    mutable read_write_lock m_rw_lock;
    task_queue_implementation m_tasks;
};

class thread_pool
{
public:
    thread_pool() = default;
    ~thread_pool() { terminate(); }

    void initialize(const size_t worker_count)
    {
        write_lock lock(m_rw_lock);
        if (m_initialized || m_terminated)
        {
            return;
        }

        m_workers.reserve(worker_count);
        for (size_t id = 0; id < worker_count; ++id)
        {
            m_workers.emplace_back(&thread_pool::routine, this, static_cast<int>(id));
        }

        m_initialized = !m_workers.empty();
    }

    void terminate()
    {
        {
            write_lock lock(m_rw_lock);
            if (working_unsafe())
            {
                m_terminated = true;
                m_force_terminate = true;
            }
            else
            {
                m_workers.clear();
                m_terminated = false;
                m_initialized = false;
                return;
            }
        }

        m_task_waiter.notify_all();

        for (thread &worker : m_workers)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }

        m_workers.clear();
        m_terminated = false;
        m_initialized = false;
    }

    template <typename task_t, typename... arguments>
    void add_task(int task_id, task_t &&task, arguments &&...parameters)
    {
        {
            read_lock lock(m_rw_lock);
            if (!working_unsafe())
            {
                return;
            }
        }

        auto binded_task = std::bind(forward<task_t>(task), forward<arguments>(parameters)...);
        Task new_task{task_id, binded_task};

        bool added = m_tasks.push(new_task, max_queue_size);

        if (added)
        {
            ++total_tasks_created;
            {
                lock_guard<mutex> lock(cout_mutex);
                cout << "Task " << task_id << " added to queue. Current queue size: "
                     << m_tasks.size() << endl;
            }
            m_task_waiter.notify_one();
        }
        else
        {
            ++total_tasks_rejected;
            {
                lock_guard<mutex> lock(cout_mutex);
                cout << "Task " << task_id << " rejected. Queue is full (15 tasks)." << endl;
            }
        }
    }

    size_t get_total_tasks_created() const { return total_tasks_created.load(); }
    size_t get_total_tasks_completed() const { return total_tasks_completed.load(); }
    size_t get_total_tasks_rejected() const { return total_tasks_rejected.load(); }

    long long get_total_execution_time_ms() const
    {
        return total_execution_time_ms.load();
    }

    double get_average_execution_time_ms() const
    {
        size_t completed = total_tasks_completed.load();
        if (completed == 0)
        {
            return 0.0;
        }
        return static_cast<double>(total_execution_time_ms.load()) / completed;
    }

    long long get_min_execution_time_ms() const
    {
        if (total_tasks_completed.load() == 0)
        {
            return 0;
        }
        return min_execution_time_ms.load();
    }

    long long get_max_execution_time_ms() const
    {
        if (total_tasks_completed.load() == 0)
        {
            return 0;
        }
        return max_execution_time_ms.load();
    }

private:
    mutable read_write_lock m_rw_lock;
    mutable condition_variable_any m_task_waiter;
    vector<thread> m_workers;
    task_queue<Task> m_tasks;

    bool m_initialized = false;
    bool m_terminated = false;
    bool m_force_terminate = false;

    const size_t max_queue_size = 15;

    atomic<size_t> total_tasks_created{0};
    atomic<size_t> total_tasks_completed{0};
    atomic<size_t> total_tasks_rejected{0};

    atomic<long long> total_execution_time_ms{0};
    atomic<long long> min_execution_time_ms{numeric_limits<long long>::max()};
    atomic<long long> max_execution_time_ms{0};

    void update_min_time(long long value)
    {
        long long current = min_execution_time_ms.load();
        while (value < current && !min_execution_time_ms.compare_exchange_weak(current, value))
        {
        }
    }

    void update_max_time(long long value)
    {
        long long current = max_execution_time_ms.load();
        while (value > current && !max_execution_time_ms.compare_exchange_weak(current, value))
        {
        }
    }

    void routine(int worker_id)
    {
        while (true)
        {
            Task current_task;

            {
                write_lock lock(m_rw_lock);

                auto wait_condition = [this]
                {
                    return m_terminated || !m_tasks.empty();
                };

                m_task_waiter.wait(lock, wait_condition);

                if (m_force_terminate)
                {
                    return;
                }

                if (!m_tasks.empty())
                {
                    m_tasks.pop(current_task);
                }
            }

            if (current_task.task)
            {
                {
                    lock_guard<mutex> lock(cout_mutex);
                    cout << "Worker " << worker_id
                         << " started task " << current_task.id << endl;
                }

                auto start_time = chrono::steady_clock::now();

                current_task.task();

                auto end_time = chrono::steady_clock::now();
                long long execution_time =
                    chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

                total_execution_time_ms += execution_time;
                update_min_time(execution_time);
                update_max_time(execution_time);
                ++total_tasks_completed;

                {
                    lock_guard<mutex> lock(cout_mutex);
                    cout << "Worker " << worker_id
                         << " finished task " << current_task.id
                         << ". Execution time: " << execution_time << " ms" << endl;
                }
            }

            if (m_force_terminate)
            {
                return;
            }
        }
    }

    bool working() const
    {
        read_lock lock(m_rw_lock);
        return working_unsafe();
    }

    bool working_unsafe() const
    {
        return m_initialized && !m_terminated;
    }
};

void executeTask(int taskId, int duration)
{
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Task " << taskId << " started, duration: "
             << duration << " seconds." << endl;
    }

    this_thread::sleep_for(chrono::seconds(duration));

    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Task " << taskId << " completed." << endl;
    }
}

int getRandomDuration(int min, int max)
{
    return min + rand() % (max - min + 1);
}

int getRandomInterval(int min, int max)
{
    return min + rand() % (max - min + 1);
}

void autoTerminateAfterTime(int seconds)
{
    this_thread::sleep_for(chrono::seconds(seconds));
    stop_program = true;
}

int main()
{
    const int workers_amount = 6;
    const int min_task_time = 5;
    const int max_task_time = 10;

    const int num_generators = 3;
    const int simulation_duration = 30;

    srand(static_cast<unsigned>(time(nullptr)));

    auto program_start_time = chrono::steady_clock::now();

    thread_pool pool;
    pool.initialize(workers_amount);

    atomic<bool> stop_generation(false);

    auto generate_tasks = [&pool, &stop_generation, min_task_time, max_task_time]()
    {
        while (!stop_generation)
        {
            int duration = getRandomDuration(min_task_time, max_task_time);
            int task_id = global_task_id.fetch_add(1);

            pool.add_task(task_id, executeTask, task_id, duration);

            int interval = getRandomInterval(1, 3);
            this_thread::sleep_for(chrono::seconds(interval));
        }
    };

    vector<thread> generators;
    for (int i = 0; i < num_generators; ++i)
    {
        generators.emplace_back(generate_tasks);
    }

    thread timer_thread(autoTerminateAfterTime, simulation_duration);

    while (!stop_program)
    {
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    stop_generation = true;
    pool.terminate();

    for (auto &generator : generators)
    {
        if (generator.joinable())
        {
            generator.join();
        }
    }

    if (timer_thread.joinable())
    {
        timer_thread.join();
    }

    auto program_end_time = chrono::steady_clock::now();
    long long total_program_time_ms =
        chrono::duration_cast<chrono::milliseconds>(program_end_time - program_start_time).count();

    cout << endl;
    cout << "==================== STATISTICS ====================" << endl;
    cout << "Total tasks added: " << pool.get_total_tasks_created() << endl;
    cout << "Total tasks completed: " << pool.get_total_tasks_completed() << endl;
    cout << "Total tasks rejected: " << pool.get_total_tasks_rejected() << endl;
    cout << "Total execution time of all completed tasks: "
         << pool.get_total_execution_time_ms() << " ms" << endl;
    cout << "Average execution time per task: "
         << pool.get_average_execution_time_ms() << " ms" << endl;
    cout << "Minimum execution time per task: "
         << pool.get_min_execution_time_ms() << " ms" << endl;
    cout << "Maximum execution time per task: "
         << pool.get_max_execution_time_ms() << " ms" << endl;
    cout << "Total program runtime: " << total_program_time_ms << " ms" << endl;
    cout << "====================================================" << endl;

    cout << "\nPress Enter to exit...";
    cin.get();
    return 0;
}