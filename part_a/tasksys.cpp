#include "tasksys.h"
#include <algorithm>
#include <vector>
#include <thread>
#include <atomic>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    int num_workers = std::min(num_threads_, num_total_tasks);
    std::atomic<int> next_task_idx{0};
    std::vector<std::thread> workers;
    workers.reserve(num_workers);

    auto worker_loop = [&]() {
        while (true) {
            int task_idx = next_task_idx.fetch_add(1);
            if (task_idx >= num_total_tasks) {
                break;
            }
            runnable->runTask(task_idx, num_total_tasks);
        }
    };

    for (int i = 0; i < num_workers; i++) {
        workers.emplace_back(worker_loop);
    }

    for (auto& worker : workers) {
        worker.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    num_workers_ = num_threads - 1;
    killed_.store(false);
    next_task_idx_.store(0);
    finished_workers_.store(0);
    current_runnable_ = nullptr;
    current_num_tasks_ = 0;

    if (num_workers_ > 0) {
        worker_has_work_ = std::make_unique<std::atomic<bool>[]>(num_workers_);
        for (int i = 0; i < num_workers_; i++) {
            worker_has_work_[i].store(false);
        }
        workers_.reserve(num_workers_);
        for (int i = 0; i < num_workers_; i++) {
            workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this, i);
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    killed_.store(true);
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop(int thread_id) {
    while (!killed_.load(std::memory_order_relaxed)) {
        if (!worker_has_work_[thread_id].load(std::memory_order_acquire)) {
            continue;
        }

        while (true) {
            int task_idx = next_task_idx_.fetch_add(1, std::memory_order_relaxed);
            if (task_idx >= current_num_tasks_) {
                break;
            }
            current_runnable_->runTask(task_idx, current_num_tasks_);
        }

        worker_has_work_[thread_id].store(false, std::memory_order_release);
        finished_workers_.fetch_add(1, std::memory_order_release);
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    current_runnable_ = runnable;
    current_num_tasks_ = num_total_tasks;
    next_task_idx_.store(0, std::memory_order_relaxed);
    finished_workers_.store(0, std::memory_order_relaxed);

    for (int i = 0; i < num_workers_; i++) {
        worker_has_work_[i].store(true, std::memory_order_release);
    }

    while (true) {
        int task_idx = next_task_idx_.fetch_add(1, std::memory_order_relaxed);
        if (task_idx >= num_total_tasks) {
            break;
        }
        runnable->runTask(task_idx, num_total_tasks);
    }

    while (finished_workers_.load(std::memory_order_acquire) < num_workers_) {
        // Spin wait
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    num_workers_ = num_threads - 1;
    killed_ = false;
    current_runnable_ = nullptr;
    total_tasks_ = 0;
    next_task_idx_ = 0;
    completed_tasks_ = 0;

    if (num_workers_ > 0) {
        workers_.reserve(num_workers_);
        for (int i = 0; i < num_workers_; i++) {
            workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
        }
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        killed_ = true;
    }
    work_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        int task_idx = -1;
        IRunnable* runnable = nullptr;
        int total = 0;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_cv_.wait(lock, [this]() {
                return killed_ || (next_task_idx_ < total_tasks_);
            });

            if (killed_) {
                return;
            }

            task_idx = next_task_idx_++;
            runnable = current_runnable_;
            total = total_tasks_;
        }

        runnable->runTask(task_idx, total);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            completed_tasks_++;
            if (completed_tasks_ == total_tasks_) {
                done_cv_.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;
        next_task_idx_ = 0;
        completed_tasks_ = 0;
    }
    work_cv_.notify_all();

    while (true) {
        int task_idx = -1;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (next_task_idx_ < total_tasks_) {
                task_idx = next_task_idx_++;
            } else {
                done_cv_.wait(lock, [this]() {
                    return completed_tasks_ == total_tasks_;
                });
                break;
            }
        }

        runnable->runTask(task_idx, num_total_tasks);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            completed_tasks_++;
            if (completed_tasks_ == total_tasks_) {
                done_cv_.notify_one();
                break;
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
