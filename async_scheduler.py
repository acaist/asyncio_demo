import asyncio
import time
from async_worker import ProductInterface, ConsumerInterface


class AsyncScheduler:
    def __init__(self, init_size=5, batch_size=3, max_workers=6, total_max=20, factor=1.0):
        self.factor = factor
        self.batch_size = batch_size
        self.total_max = total_max
        self.init_size = init_size  # 首次生产和消费数量
        self.queue = asyncio.Queue(max_workers)
        self.semp = asyncio.Semaphore(max_workers) # 信号量

        # 生产者和消费者之间的同步信号
        self.producer_fill = asyncio.Event()
        self.producer_fill.set() # 初始允许生产

        #
        self.running = True
        self.current_product_store = 0  # 当前库存数量
        self.total_product_count = 0   # 总生产数量
        self.totoal_consume_count = 0  # 总消费数量
        self.consumer_pendings = {}  # 消费者待处理的future数量

    def set_producer_consumer(self, producer: ProductInterface, consumer: ConsumerInterface):
        """ 设置生产者消费者同步率 """
        self.producer = producer
        self.consumer = consumer

    async def produce(self):
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
                await self.semp.acquire() # 获取信号量，限制并发数量
                # 线程池提交任务
                ctx = self.producer.produce(self.total_product_count)
                await self.queue.put(ctx)
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

    async def consume_sequential(self):
        """ 消费者 顺序 """
        while self.running:
            # print("[消费者]：消费者开始消费")
            comsum_n = self.batch_size
            if self.totoal_consume_count == 0: # 首次消费和初始生产匹配
                comsum_n = self.init_size
            
            ctxs = []
            for _ in range(int(comsum_n*self.factor)):
                # 从队列获取future并等待结果
                ctx = await self.queue.get()
                self.queue.task_done()
                if ctx is None:
                    print("[消费者]：收到退出信号，停止消费")
                    print("[消费者]：最后总消费数量", self.totoal_consume_count)
                    print(f"[消费者]: 最后剩余库存item数量 {self.current_product_store}")
                    self.running = False
                    self.producer_fill.set()  # 取消生产者的wait
                    return
                
                result = await asyncio.wrap_future(ctx.future)
                # print(f"  [消费者]: {result}")
                ctxs.append(ctx)
                #
                self.totoal_consume_count += 1
                self.current_product_store -= 1
                self.semp.release()  # 释放信号量

            # 批量消费
            self.consumer.consume(ctxs)

            print(f"[消费者]：消费者完成消费 {self.totoal_consume_count}, 剩余库存item数量 {self.current_product_store}")
            
            # 通知生产者补充生产
            self.producer_fill.set()

    async def consume_outoforder(self):
        """ 消费者（无序） """
        current_comsum_n = 0
        comsum_n = int(self.init_size*self.factor)
        while self.running:
            # print("[消费者]：消费者开始消费")
            # 贪婪，一次性取出queue中所有任务，避免阻塞
            while not self.queue.empty():
                ctx = await self.queue.get()
                self.queue.task_done()
                if ctx is None:
                    print("[消费者]：收到退出信号，停止消费")
                    self.running = False
                    break

                warp = asyncio.wrap_future(ctx.future)
                ctx.future = warp
                self.consumer_pendings[ctx.task_id] = ctx
           
            # 等待部分任务完成
            while len(self.consumer_pendings) > 0:
                futures = [ctx.future for ctx in self.consumer_pendings.values()]
                done, pending = await asyncio.wait(
                    futures,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                ctxs_done = []
                for d in done:
                    result = await d
                    # print(f"  [消费者]: {result}")
                    self.totoal_consume_count += 1
                    self.current_product_store -= 1
                    cur_ctx = self.consumer_pendings.pop(d.result()["task_id"])
                    ctxs_done.append(cur_ctx)
                    current_comsum_n += 1
                    self.semp.release()  # 释放信号量

                #
                self.consumer.consume(ctxs_done)

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
                return

            await asyncio.sleep(0.01)  # 让出控制权, 避免while死循环


    async def run(self, outoforder=True):
        t0 = time.perf_counter()
        producer_task = asyncio.create_task(self.produce())
        if outoforder:
            consumer_task = asyncio.create_task(self.consume_outoforder())
        else:
            consumer_task = asyncio.create_task(self.consume_sequential())
        await asyncio.gather(producer_task, consumer_task)
        t1 = time.perf_counter()
        print(f"同步率 {self.factor}, tasks completed in {t1 - t0:.2f} seconds")
        return t1 - t0