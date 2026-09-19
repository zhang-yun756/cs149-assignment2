#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    killed_ = false;
    next_launch_id_ = 0;

    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        killed_ = true;
    }
    run_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        int sub_idx = -1;
        int total = 0;
        IRunnable* runnable = nullptr;
        std::shared_ptr<TaskInfo> task_to_complete = nullptr;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            run_cv_.wait(lock, [this]() {
                return killed_ || !ready_tasks_.empty();
            });

            if (killed_ && ready_tasks_.empty()) {
                return;
            }

            TaskID current_id = ready_tasks_.front();
            auto it = tasks_map_.find(current_id);
            if (it == tasks_map_.end()) {
                ready_tasks_.pop();
                continue;
            }

            std::shared_ptr<TaskInfo> info = it->second;
            sub_idx = info->next_task_idx++;
            runnable = info->runnable;
            total = info->total_tasks;

            // Immediately evict the task launch once all sub-tasks have been claimed
            if (info->next_task_idx >= info->total_tasks) {
                ready_tasks_.pop();
            }

            task_to_complete = info;
        }

        // Run sub-task outside the lock
        runnable->runTask(sub_idx, total);

        // Update completion status and resolve dependencies
        {
            std::lock_guard<std::mutex> lock(mutex_);
            task_to_complete->completed_tasks++;

            if (task_to_complete->completed_tasks == task_to_complete->total_tasks) {
                bool new_tasks_ready = false;

                // Propagate completion to all dependent tasks
                for (TaskID dep_id : task_to_complete->dependents) {
                    auto dep_it = tasks_map_.find(dep_id);
                    if (dep_it != tasks_map_.end()) {
                        dep_it->second->unresolved_deps--;
                        if (dep_it->second->unresolved_deps == 0) {
                            if (dep_it->second->total_tasks > 0) {
                                ready_tasks_.push(dep_id);
                                new_tasks_ready = true;
                            }
                        }
                    }
                }

                tasks_map_.erase(task_to_complete->task_id);

                if (new_tasks_ready) {
                    run_cv_.notify_all();
                }

                if (tasks_map_.empty()) {
                    sync_cv_.notify_all();
                }
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::lock_guard<std::mutex> lock(mutex_);
    TaskID current_id = next_launch_id_++;

    if (num_total_tasks <= 0) {
        return current_id;
    }

    auto info = std::make_shared<TaskInfo>(current_id, runnable, num_total_tasks, 0);

    // Deduplicate dependency IDs and identify any incomplete dependencies
    std::set<TaskID> unique_deps(deps.begin(), deps.end());
    for (TaskID dep : unique_deps) {
        auto it = tasks_map_.find(dep);
        if (it != tasks_map_.end()) {
            it->second->dependents.push_back(current_id);
            info->unresolved_deps++;
        }
    }

    tasks_map_[current_id] = info;

    // If all dependencies are satisfied, mark task as ready to execute
    if (info->unresolved_deps == 0) {
        ready_tasks_.push(current_id);
        run_cv_.notify_all();
    }

    return current_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mutex_);
    sync_cv_.wait(lock, [this]() {
        return tasks_map_.empty();
    });
}
