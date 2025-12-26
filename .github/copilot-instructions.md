# AI Coding Agent Instructions for Game Map Generation Project

## Project Overview
This codebase contains demos and utilities for procedural game map generation, combining Stable Diffusion concepts with 3D simulation and async processing patterns. The project focuses on performance-optimized batch processing for map generation tasks.

## Architecture Components

### Async Processing (`async_demo.py`)
- **Producer-Consumer Pattern**: Uses `asyncio.Queue` with `ThreadPoolExecutor` for concurrent task processing
- **Batch Synchronization**: Implements `AsyncScheduler` class with configurable `init_size`, `batch_size`, and `factor` parameters
- **Performance Testing**: Includes `test_factor()` function for benchmarking different sync ratios (e.g., 0.5, 0.8, 1.0)
- **Key Pattern**: `gauss_in_range(mu, sigma, low, high)` for constrained Gaussian random values

### 3D Simulation (`genesis_world_demo.py`, `worlddemo.ipynb`)
- **Genesis Framework**: Uses `gs.init(backend=gs.cpu)` for physics simulation
- **Scene Building**: Creates entities like planes and MJCF robots (e.g., Franka Panda)
- **World Generation**: Calls `gs.generate("a fantasy world with mountains and rivers")` for procedural content
- **Simulation Loop**: Runs `scene.step()` in a loop for physics updates

### Map Generation (`random_map.ipynb`)
- **Gradient Integration**: `integrate_gradient()` function averages neighboring cells for height map smoothing
- **Iterative Refinement**: Applies multiple integration passes (`int_num = 10`) to create natural-looking terrain
- **Visualization**: Uses `matplotlib.pyplot.imshow()` with 'viridis' colormap for map display

### Utilities (`plot_gaussian.py`)
- **Distribution Visualization**: Plots Gaussian PDFs with variance bounds (μ ± σ, μ ± 2σ)
- **Parameter Configuration**: Configurable `mu`, `sigma`, `low`, `high` for distribution constraints

## Critical Workflows

### Running Async Benchmarks
```python
scheduler = AsyncScheduler(init_size=20, batch_size=11, max_workers=7, total_max=93, factor=0.8)
asyncio.run(scheduler.run(outoforder=True))
```
- Use `outoforder=True` for concurrent consumer processing
- Monitor performance with saved PNG plots (`async_producer_consumer_performance.png`)

### Genesis Simulation Setup
```python
import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
gs.init(backend=gs.cpu)
scene = gs.Scene(show_viewer=True)
scene.build()
```

### Map Generation Pipeline
```python
gradient = np.random.random((100, 100))
for i in range(10):
    height_map = integrate_gradient(gradient)
    gradient = height_map
plot_image(height_map)
```

## Project-Specific Conventions

### Async Patterns
- **Queue Management**: Use `asyncio.Queue` with `max_workers` limit for bounded concurrency
- **Future Wrapping**: Always wrap ThreadPoolExecutor futures with `asyncio.wrap_future()`
- **Event Synchronization**: Use `asyncio.Event` for producer-consumer coordination
- **Shutdown Handling**: Call `executor.shutdown(wait=True)` after processing completion

### Simulation Setup
- **Environment Variables**: Set `KMP_DUPLICATE_LIB_OK='True'` before Genesis initialization
- **Backend Selection**: Prefer `gs.cpu` for development, `gs.gpu` for production
- **MJCF Loading**: Reference robot models from `xml/franka_emika_panda/panda.xml`

### Visualization Standards
- **Colormaps**: Use 'viridis' for terrain maps, standard colors for distributions
- **Plot Saving**: Save performance plots as PNG files for documentation
- **Grid Display**: Enable `plt.grid(True)` for distribution plots

### Code Style
- **Comments**: Mix English and Chinese comments for clarity
- **Type Hints**: Use `# type: ignore` for third-party imports like Genesis
- **Parameter Naming**: Use descriptive names like `product_count`, `total_max`, `factor`

## Dependencies & Environment
- **Core**: `asyncio`, `numpy`, `matplotlib`
- **Simulation**: `genesis` (custom physics engine)
- **Execution**: Run scripts directly with `python script.py`, notebooks via Jupyter

## Integration Points
- **Stable Diffusion**: Async patterns designed for batch image generation workflows
- **3D Rendering**: Genesis scenes can integrate generated maps as terrain
- **Performance Monitoring**: Built-in timing and plotting for optimization analysis

## Key Files to Reference
- `async_demo.py`: Exemplifies async architecture and performance testing
- `random_map.ipynb`: Core map generation algorithm
- `genesis_world_demo.py`: Simulation integration patterns</content>
<parameter name="filePath">.github/copilot-instructions.md