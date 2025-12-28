import asyncio
from async_worker import Producer, Consumer
from async_scheduler import AsyncScheduler


def test_scheduler():
    print("\n\n=== 测试 AsyncScheduler ===")
    max_workers = 5
    scheduler = AsyncScheduler(
        init_size=5,
        batch_size=3,
        max_workers=max_workers, 
        total_max=63,
        factor=0.8
    )
    producer = Producer(max_work=max_workers)
    consumer = Consumer()
    scheduler.set_producer_consumer(producer, consumer)
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
            max_workers = 7
            scheduler = AsyncScheduler(
                init_size=20,
                batch_size=11,
                max_workers=max_workers,
                total_max=93,
                factor=factor
            )
            producer = Producer(max_work=max_workers)
            consumer = Consumer()
            scheduler.set_producer_consumer(producer, consumer)
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
    test_scheduler()
    # test_factor(test_runs=3)