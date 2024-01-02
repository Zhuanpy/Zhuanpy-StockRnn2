from multiprocessing import Process, Queue
import os, time, random


# 写数据进程执行的代码:
def _write(q, urls):
    print('Process(%s) is writing...' % os.getpid())
    for url in urls:
        q.put(url)
        print('Put %s to queue...' % url)
        time.sleep(random.random())


# 读数据进程执行的代码:
def _read(q):
    print('Process(%s) is reading...' % os.getpid())
    while True:
        url = q.get(True)
        print('Get %s from queue.' % url)


if __name__ == '__main__':
    # 父进程创建Queue，并传给各个子进程：
    q = Queue()
    _writer1 = Process(target=_write, args=(q, ['url_1', 'url_2', 'url_3']))
    _writer2 = Process(target=_write, args=(q, ['url_4', 'url_5', 'url_6']))
    _reader = Process(target=_read, args=(q,))

    # 启动子进程_writer，写入:
    _writer1.start()
    _writer2.start()

    # 启动子进程_reader，读取:
    _reader.start()

    # 等待_writer结束:
    _writer1.join()
    _writer2.join()

    # _reader进程里是死循环，无法等待其结束，只能强行终止:
    _reader.terminate()

'''
Process(7460) is writing...
Put url_1 to queue...
Process(13764) is writing...
Put url_4 to queue...
Process(13236) is reading...
Get url_1 from queue.
Get url_4 from queue.
Put url_2 to queue...
Get url_2 from queue.
Put url_5 to queue...
Get url_5 from queue.
Put url_6 to queue...
Get url_6 from queue.
Put url_3 to queue...
Get url_3 from queue.

'''
