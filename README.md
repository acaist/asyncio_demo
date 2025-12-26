# 游戏地图生成项目 (Game Map Generation)

这是一个结合 Stable Diffusion、异步处理和 3D 模拟的游戏地图生成项目演示。项目专注于性能优化的批量处理，用于地图生成任务。

## 项目概述

本项目包含多个演示和工具：
- **异步处理演示**：使用 `asyncio.Queue` 和 `ThreadPoolExecutor` 实现生产者-消费者模式，支持并发任务处理。
- **3D 模拟**：基于 Genesis 框架创建 3D 场景，包括机器人和世界生成。
- **地图生成**：使用 NumPy 和 Matplotlib 生成程序化地形地图。
- **性能测试**：内置基准测试和可视化工具。

## 依赖项

- Python 3.8+
- asyncio
- numpy
- matplotlib
- genesis (自定义物理引擎)

## 安装

1. 克隆仓库：
   ```bash
   git clone https://github.com/acaist/asyncio_demo.git
   cd gamemap
   ```

2. 安装依赖：
   ```bash
   pip install numpy matplotlib
   # genesis 需要单独安装（请参考官方文档）
   ```

3. 设置环境变量（Genesis 需要）：
   ```bash
   export KMP_DUPLICATE_LIB_OK=True
   ```

## 使用

### 运行异步演示
```bash
python async_demo.py
```
这将运行性能测试，生成 `async_producer_consumer_performance.png` 图表。

### 运行 3D 模拟
```bash
python genesis_world_demo.py
```
或在 Jupyter 中运行 `worlddemo.ipynb`。

### 生成地图
在 Jupyter 中运行 `random_map.ipynb`，生成地形高度图。

### 绘制高斯分布
```bash
python plot_gaussian.py
```

## 文件结构

- `async_demo.py`：异步生产者-消费者调度器，包含性能测试。
- `genesis_world_demo.py`：Genesis 3D 模拟演示。
- `plot_gaussian.py`：高斯分布可视化工具。
- `random_map.ipynb`：程序化地图生成笔记本。
- `worlddemo.ipynb`：3D 世界演示笔记本。
- `.github/copilot-instructions.md`：AI 代理指导文件。

## 关键特性

- **异步优化**：支持不同同步率因子的批量处理。
- **3D 集成**：Genesis 框架用于物理模拟和场景构建。
- **可视化**：Matplotlib 用于性能图表和地图显示。
- **性能监控**：内置计时和绘图功能。

## 贡献

欢迎提交 Issue 和 Pull Request。请确保代码符合项目风格，并添加必要的测试。

## 许可证

MIT License