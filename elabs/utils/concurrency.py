# coding:utf-8
from copy import deepcopy
from multiprocessing import Process, Queue
from threading import Thread


def task_split(taskes, num=5):
    """
      ([a,b,c,d,e,f,g] , 3) => [ (a,b,c),(e,f,g) ]
    """
    ss = []
    groups = []
    taskes = deepcopy(taskes)
    while taskes:
        ss.append(taskes.pop(0))
        if len(ss) >= num:
            groups.append(ss)
            ss = []
    if ss:
        groups.append(ss)
    return groups

def run(func,params,noreturn,group, q):
    for task in group:
        # print('exec task:',task)
        res = func(task, params)
        if not noreturn:
            q.put(res)

    print('--' * 20)

def multiprocess_task_split(func, params, task_list, n_core, concls=Process, noreturn=True):
    if  not task_list:
        return
    num_per_core = int(len(task_list) / n_core)
    if num_per_core <=0 :
        num_per_core = 1
    groups = task_split(task_list, num_per_core)
    process_list = []
    q = Queue()
    print('group size:', len(groups))



    for group in groups:
        p = concls(target=run, args=(func,params,noreturn,group, q))
        p.start()
        process_list.append(p)
    print('group:',len(groups) , len(task_list) , n_core)
    for p in process_list:
        p.join()
    res = []
    # print 'qsize:',q.qsize()
    if not noreturn:
        while q.qsize():
            data = q.get()
            res.append(data)

    return res


def multiprocess_task_split_inc(func, params, start, end, n_core, concls=Process, noreturn=True):
    num_per_core = int((end - start + 1) / n_core)
    p = start
    groups = []
    while p < end:
        n_step = min(num_per_core, end - p)
        groups.append([p, p + n_step])
        p += n_step

    process_list = []
    q = Queue()
    print('group size:', len(groups))

    def run(group, q):
        for task in range(group[0], group[1]):
            # print('exec task:',task)
            res = func(task, params)
            if not noreturn:
                q.put(res)

        print('--' * 20)

    for group in groups:
        p = concls(target=run, args=(group, q))
        p.start()
        process_list.append(p)

    for p in process_list:
        p.join()

    res = []
    # print 'qsize:',q.qsize()
    if not noreturn:
        while q.qsize():
            data = q.get()
            res.append(data)

    return res


def multiprocess_task_queue_publish(func, params, start, end, n_core, steps=10, concls=Process, noreturn=True):
    process_list = []
    task_q = Queue()
    ret_q = Queue()

    # print('group size:', len(groups))

    class SignalExit(object):
        pass

    def run(task_q, ret_q):
        while True:
            let = task_q.get(block=True)
            if isinstance(let, SignalExit):
                break
            if isinstance(let, (list, tuple)):
                group = let
                for task in range(group[0], group[1]):
                    # print('exec task:',task)
                    res = func(task, params)
                    if not noreturn:
                        ret_q.put(res)
            else:
                task = let
                res = func(task, params)
                if not noreturn:
                    ret_q.put(res)

        print('--' * 20)

    for _ in range(n_core):
        p = concls(target=run, args=(task_q, ret_q))
        p.start()
        process_list.append(p)

    p = start
    while p < end:
        n_step = min(steps, end - p)
        group = [p, p + n_step]
        p += n_step
        task_q.put(group)

    for p in process_list:
        p.join()
    res = []
    # print 'qsize:',q.qsize()
    if not noreturn:
        while ret_q.qsize():
            data = ret_q.get()
            res.append(data)

    return res


def test_multipleprocess():
    def my_func(task, params):
        print(task, params)
        return task

    data = multiprocess_task_split(my_func, [100, 101], range(10), 3)
    # print data


def test_multipleprocess_inc():
    def my_func(task, params):
        print(task, params)
        return task

    data = multiprocess_task_split_inc(my_func, [100, 101], 0, 10, 3)
    # print data


if __name__ == '__main__':
    test_multipleprocess_inc()
