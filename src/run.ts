import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);

    if (maxThreads === 0) {
        return runForInfiniteThreads(executor, queue);
    }

    // Решим сначала случай, когда maxThreads !== 0;

    const workedOnTargetIds: Set<number> = new Set();
    let usedThreads = 0;
    // Массив, указывающий, занят ли поток i или нет.
    const busyThreads: boolean[] = [...Array(maxThreads).keys()].map(k => false);

    // Отображение от номера потока (0...maxThreads) к его асинхронной функции, возвращающей task.targetId
    const asyncTasks: Map<number, Promise<number>> = new Map();

    // Отображение от task.targetId к его индексу в asyncTasks.
    const targetIdToIndex: Map<number, number> = new Map();
    // Обратное отображение.
    const indexToTargetId: Map<number, number> = new Map();

    const addToExecution = (task: ITask) => {
        const emptyThread = findEmptyThread();
        busyThreads[emptyThread] = true;
        workedOnTargetIds.add(task.targetId);
        targetIdToIndex.set(task.targetId, emptyThread);
        indexToTargetId.set(emptyThread, task.targetId);

        asyncTasks.set(emptyThread,
            executor.executeTask(task).then(() => {
                workedOnTargetIds.delete(task.targetId);
                busyThreads[emptyThread] = false;
                return task.targetId;
            })
        );
    }

    const findEmptyThread = () => {
        return busyThreads.findIndex(busy => !busy);
    }

    for await (const task of queue) {
        // Если мы не работаем с тем же targetId
        if (!workedOnTargetIds.has(task.targetId)) {
            const emptyThread = findEmptyThread();
            if (emptyThread !== -1) {
                addToExecution(task);
            } else {
                // Ожидание любого свободного потока
                const finishedTargetId = await Promise.race([...asyncTasks.values()]) as number;
                const threadIndex = targetIdToIndex.get(finishedTargetId)!;
                asyncTasks.delete(threadIndex);

                targetIdToIndex.delete(finishedTargetId);

                addToExecution(task);
            }
        } else {
            // Если этот task.targetId уже запланирован, просто добавьте новое выполнение к нему.
            const threadIndex = targetIdToIndex.get(task.targetId)!;
            const currentScheduledTaskTarget = asyncTasks.get(threadIndex)!;
            asyncTasks.set(threadIndex,
                currentScheduledTaskTarget.then(async () => {
                    workedOnTargetIds.add(task.targetId);
                    busyThreads[threadIndex] = true;
                    await executor.executeTask(task);
                    workedOnTargetIds.delete(task.targetId);
                    busyThreads[threadIndex] = false;
                    return task.targetId;
                })
            );
        }
    }

    // Ожидание завершения всех оставшихся задач;
    await Promise.all(asyncTasks.values()).then((values) => {
        console.log(values);
    });
}

async function runForInfiniteThreads(executor: IExecutor, queue: AsyncIterable<ITask>) {

    // Отображение от task.targetId к асинхронной функции (выполнению)
    const asyncTasks: Map<number, Promise<void>> = new Map();
    const asyncTaskThreadIndexById: Map<number, number> = new Map();
    let nextTaskI = 0;

    const addToExecution = (task: ITask) => {
        asyncTasks.set(task.targetId, executor.executeTask(task));
    }

    for await (const task of queue) {

        if (!asyncTasks.has(task.targetId)) {
            addToExecution(task)
        } else {
            const currentScheduledTaskTarget = asyncTasks.get(task.targetId)!;
            asyncTasks.set(task.targetId,
                currentScheduledTaskTarget.then(async () => {
                    await executor.executeTask(task);
                })
            );
        }
    }

    // Ожидание завершения всех оставшихся задач;
    await Promise.all(asyncTasks.values());
}

























