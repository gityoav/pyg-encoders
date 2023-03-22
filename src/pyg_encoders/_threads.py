from concurrent.futures import ThreadPoolExecutor

executors = {}

def executor_pool(max_workers = 4, name = None):
    """
    we want to have a pool of threads that we don't need to recreate all the times.
    We will use these to write to files rather than use the main threads
    """
    key = (max_workers, name)
    if key not in executors:
        executors[key] = ThreadPoolExecutor(max_workers=max_workers)
    return executors[key]

