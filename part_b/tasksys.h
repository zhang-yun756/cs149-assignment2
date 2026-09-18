#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <thread>
#include <vector>
#include <queue>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
/*
 * Structure representing the execution and dependency state of a bulk task launch.
 */
struct TaskInfo {
    TaskID task_id;
    IRunnable* runnable;
    int total_tasks;
    int next_task_idx;
    int completed_tasks;
    int unresolved_deps;
    std::vector<TaskID> dependents;

    TaskInfo(TaskID id, IRunnable* r, int num_tasks, int num_deps)
        : task_id(id),
          runnable(r),
          total_tasks(num_tasks),
          next_task_idx(0),
          completed_tasks(0),
          unresolved_deps(num_deps) {}
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        void workerLoop();

        int num_threads_;
        std::vector<std::thread> workers_;
        std::mutex mutex_;
        std::condition_variable run_cv_;
        std::condition_variable sync_cv_;

        TaskID next_launch_id_{0};
        std::queue<TaskID> ready_tasks_;
        std::unordered_map<TaskID, std::shared_ptr<TaskInfo>> tasks_map_;
        bool killed_{false};
};

#endif
