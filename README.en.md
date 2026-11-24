# Time Series Analysis

An early Python research workspace for turning a time-series factor into a trading signal, running a backtest, and reviewing trades and performance. The repository contains code from 2021 and keeps the original research layout.

| Start here | What it contains |
| --- | --- |
| [Backtest entry point](src/backtest/backtest_entry.py) | Loads the configured market data and strategy inputs and generates result files. |
| [Backtest configuration](src/backtest/test_config.py) | Example inputs for the original research workflow. |
| [Factor modules](src/factor/) | Feature and signal computation, including work intended to move from notebooks into reusable code. |
| [Notebook guide](notebook/Readme.md) | Research notebook workflow. |
| [Original README](README.md) | Project structure, interpretation of results, and screenshots. |

The original [requirements](requirements.txt) are pinned to the project's 2021 environment. Use that file as a starting point when reproducing the old run. Market samples, factors, and signals are separate inputs. For new examples, use your own authorized data or generate synthetic inputs.

The [commit history](https://github.com/danni2019/time_series_analysis/commits/main/) records the 2021 development work. The code license is in [LICENSE](LICENSE).
