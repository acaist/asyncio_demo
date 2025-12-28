# 异步处理演示项目
异步处理项目专注于性能优化的批量处理

## 项目概述
本项目包含多个演示和工具：
- **异步处理演示**：使用 `asyncio.Queue` 和 `ThreadPoolExecutor` 实现生产者-消费者模式，支持并发任务处理。
- **性能测试**：内置基准测试和可视化工具。

## 依赖项
- Python 3.8+
- asyncio
- numpy
- matplotlib

## 安装
1. 克隆仓库：
   ```bash
   git clone https://github.com/acaist/asyncio_demo.git
   cd gamemap
   ```

2. 安装依赖：
   ```bash
   pip install numpy matplotlib
   ```

## 使用

### 运行异步演示
```bash
python async_demo.py
```
这将运行性能测试，生成 `async_producer_consumer_performance.png` 图表。

## 文件结构
- `async_demo.py`：异步生产者-消费者调度器，包含性能测试。

## 关键特性
- **异步优化**：支持不同同步率因子的批量处理。

## 贡献

欢迎提交 Issue 和 Pull Request。请确保代码符合项目风格，并添加必要的测试。

## 许可证

MIT License
