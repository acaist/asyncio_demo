import time
import random
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Protocol, List
from dataclasses import dataclass


@dataclass
class TaskContext:
    task_id: int
    future: Future
    metadata: dict


class ProductInterface(Protocol):
    def produce(self, product_count: int) -> TaskContext: ...


class ConsumerInterface(Protocol):
    def consume(self, tasks: List[TaskContext]) -> None: ...


class Producer:
    def __init__(self, max_work=5):
        self.executor = ThreadPoolExecutor(max_workers=max_work+1)

    @staticmethod
    def blocking_funcion(product_count:int, n:float):
        """A sample blocking task that simulates a time-consuming operation."""
        time.sleep(n)
        return {"task_id": product_count, "message":f"Produced cost {n:.2f} seconds"}
    
    @staticmethod
    def gauss_in_range(mu, sigma, low, high):
        """生成高斯分布随机数，限制在 [low, high] 范围内"""
        while True:
            value = random.gauss(mu, sigma)
            if low <= value <= high:
                return value
        
    def produce(self, product_count: int) -> TaskContext:
        # 实现生产逻辑
        n = self.gauss_in_range(2.5, 2.0, 1, 4)  # 高斯分布，均值 2.5，标准差 0.5，范围 1-4
        future: Future = self.executor.submit(self.blocking_funcion, product_count, n)
        return TaskContext(product_count, future, {"produce_time": n})
 
class Consumer:
    def consume(self, tasks: List[TaskContext]) -> None:
        # 实现消费逻辑
        for task in tasks:
            result = task.future.result()  # 阻塞等待结果
            print(f" {task.task_id}, {result}")