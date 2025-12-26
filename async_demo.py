import asyncio
import random
from concurrent.futures import ThreadPoolExecutor, Future
import time

def blocking_funcion(product_count:int, n:float):
    """A sample blocking task that simulates a time-consuming operation."""
    time.sleep(n)
    return f" == 产品id == {product_count} 生产完成 {n:.2f} seconds"


def gauss_in_range(mu, sigma, low, high):
    """生成高斯分布随机数，限制在 [low, high] 范围内"""
    while True:
        value = random.gauss(mu, sigma)
        if low <= value <= high:
            return value


class AsyncScheduler:
    def __init__(self, init_size=5, batch_size=3, max_workers=6, total_max=20, factor=1.0):
        self.factor = factor
        self.batch_size = batch_size
        self.total_max = total_max
        self.init_size = init_size  # 首次生产和消费数量
        self.queue = asyncio.Queue(max_workers)
        self.executor = ThreadPoolExecutor(max_workers=max_workers+1)

        # 生产者和消费者之间的同步信号
        self.producer_fill = asyncio.Event()
        self.producer_fill.set() # 初始允许生产

        #
        self.running = True
        self.current_product_store = 0  # 当前库存数量
        self.total_product_count = 0   # 总生产数量
        self.totoal_consume_count = 0  # 总消费数量
        self.consumer_pending_futures = set()  # 消费者待处理的future数量



    async def producer(self):
        """ 生产者 """
        num_to_produce = self.init_size  # 首次生产数量
        while self.running:
            # 等待消费者通知
            await self.producer_fill.wait()
            if self.total_product_count >= self.init_size:
                num_to_produce = self.batch_size  # 每次生产 n 个，与消费匹配
            is_last_batch = self.total_product_count + self.batch_size > self.total_max
            if is_last_batch: # 最后一批生产，调整数量
                num_to_produce = self.total_max - self.total_product_count
                print("[生产者]：最后一批生产数量调整", num_to_produce)
            print("[生产者]：生产者计划生产", num_to_produce)

            for i in range(num_to_produce):
                # 线程池提交任务
                n_rand = gauss_in_range(2.5, 2.0, 1, 4)  # 高斯分布，均值 2.5，标准差 0.5，范围 1-4
                future: Future = self.executor.submit(blocking_funcion, self.total_product_count, n_rand)
                if self.total_product_count < self.init_size:
                    # print(" 初始Round Solbol采样")
                    pass
                else:
                    # print(" Batch Round ModelGP采样")
                    pass
                await self.queue.put(future)
                self.current_product_store += 1
                self.total_product_count += 1
                # print(f"  [生产者]: 开始生产 产品={self.total_product_count} | 总生产数量: {self.total_product_count}")

            # 检查是否达到最大，放入退出信号
            if self.total_product_count >= self.total_max:
                print("[生产者]：达到最大生产数量, 发送None, 停止生产")
                await self.queue.put(None)  # 通知消费者退出
                return
            
            print("[生产者]：生产者提交生产, 队列item数量 {} items".format(self.current_product_store))
            # 清除event flag 等待生效
            self.producer_fill.clear()
            await asyncio.sleep(0)  # 让出控制权

    async def consumer_sequential(self):
        """ 消费者 顺序 """
        while self.running:
            # print("[消费者]：消费者开始消费")
            comsum_n = self.batch_size
            if self.totoal_consume_count == 0: # 首次消费和初始生产匹配
                comsum_n = self.init_size
            for _ in range(int(comsum_n*self.factor)):
                # 从队列获取future并等待结果
                future = await self.queue.get()
                self.queue.task_done()
                if future is None:
                    print("[消费者]：收到退出信号，停止消费")
                    print("[消费者]：最后总消费数量", self.totoal_consume_count)
                    print(f"[消费者]: 最后剩余库存item数量 {self.current_product_store}")
                    self.running = False
                    self.producer_fill.set()  # 取消生产者的wait
                    self.executor.shutdown(wait=True)
                    return
                
                result = await asyncio.wrap_future(future)
                # print(f"  [消费者]: {result}")
                #
                self.totoal_consume_count += 1
                self.current_product_store -= 1

            print(f"[消费者]：消费者完成消费 {self.totoal_consume_count}, 剩余库存item数量 {self.current_product_store}")
            
            # 通知生产者补充生产
            self.producer_fill.set()

    async def consumer_outoforder(self):
        """ 消费者（无序） """
        current_comsum_n = 0
        comsum_n = int(self.init_size*self.factor)
        while self.running:
            # print("[消费者]：消费者开始消费")
            # 贪婪，一次性取出queue中所有任务，避免阻塞
            while not self.queue.empty():
                future = await self.queue.get()
                self.queue.task_done()
                if future is None:
                    print("[消费者]：收到退出信号，停止消费")
                    self.running = False
                    break

                warp = asyncio.wrap_future(future)
                self.consumer_pending_futures.add(warp)
           
            # 等待部分任务完成
            while len(self.consumer_pending_futures) > 0:
                done, pending = await asyncio.wait(
                    self.consumer_pending_futures, 
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for d in done:
                    result = await d
                    # print(f"  [消费者]: {result}")
                    self.totoal_consume_count += 1
                    self.current_product_store -= 1
                    self.consumer_pending_futures.remove(d)
                    current_comsum_n += 1

                if current_comsum_n >= comsum_n and self.running:
                    # 达到本次消费数量, 且不是最后一次，跳出while
                    self.producer_fill.set()
                    comsum_n = int(self.batch_size*self.factor)
                    current_comsum_n = 0
                    await asyncio.sleep(0.01)  # 让出控制权
                    
            print(f"  [消费者]: 总消费数量: {self.totoal_consume_count}, 剩余库存item数量 {self.current_product_store}")
            
            # pending耗尽， 队列为空，通知生产者生产
            if self.queue.empty():
                self.producer_fill.set()
                await asyncio.sleep(0.01)  # 等待更多任务加入
            
            # 检查退出信号
            if not self.running:
                print("[消费者]：停止消费，退出")
                print("[消费者]：最后总消费数量", self.totoal_consume_count)
                print(f"[消费者]: 最后剩余库存item数量 {self.current_product_store}")
                self.producer_fill.set() # 取消生产者的wait
                self.executor.shutdown(wait=True)
                return

            await asyncio.sleep(0.01)  # 让出控制权, 避免while死循环


    async def run(self, outoforder=True):
        t0 = time.perf_counter()
        producer_task = asyncio.create_task(self.producer())
        if outoforder:
            consumer_task = asyncio.create_task(self.consumer_outoforder())
        else:
            consumer_task = asyncio.create_task(self.consumer_sequential())
        await asyncio.gather(producer_task, consumer_task)
        t1 = time.perf_counter()
        print(f"同步率 {self.factor}, tasks completed in {t1 - t0:.2f} seconds")
        return t1 - t0


def test_scheduler():
        scheduler = AsyncScheduler(
            init_size=5,
            batch_size=3,
            max_workers=3, 
            total_max=63,
            factor=0.8
        )
        asyncio.run(scheduler.run())

def test_factor(test_runs=5):
    import numpy as np
    from matplotlib import pyplot as plt
    import matplotlib
    matplotlib.use('TkAgg')

    historys = []
    test_factors = [0.5, 0.8, 1.0, 1.0]
    test_ordered = [True, True, True, False]
    for factor, ordered in zip(test_factors, test_ordered):
        print("\n\n=== 测试同步率 factor =", factor, " ===")
        history = []
        for i in range(test_runs):
            scheduler = AsyncScheduler(
                init_size=20,
                batch_size=11,
                max_workers=7,
                total_max=93,
                factor=factor
            )
            ret = asyncio.run(scheduler.run(outoforder=ordered))
            history.append((i, ret))
        historys.append(history)
    print("\n\n=== 测试总结 ===")
    for factor, ret in history:
        print("同步率", factor, " 运行时间:", ret)

    # 绘图
    fig, ax = plt.subplots()
    for factor, ordered, history in zip(test_factors, test_ordered, historys):
        rounds = [h[0] for h in history]
        times = [h[1] for h in history]
        print(f" factor {factor} 平均时间={np.mean(times):.2f}")
        mean_time = np.mean(times)
        stdvar = np.std(times)
        mode = "outoforder" if ordered else "sequential"
        info = f"factor={factor} mean={mean_time:.2f}s std={stdvar:.2f}s mode={mode}"
        ax.plot(rounds, times, '-o', label=info)
    ax.set_xlabel("rounds")
    ax.set_ylabel("total cost (seconds)")
    ax.legend(loc='upper right')
    plt.title("sync-ratio on producer-consumer ")
    plt.savefig("async_producer_consumer_performance.png")


if __name__ == "__main__":
    # test_scheduler()
    test_factor(test_runs=10)