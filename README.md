
# QuantGPT

This repository houses the `QuantGPT` project, an evolving framework for implementing quantitative trading strategies that use large language models (LLMs). It includes an experimental trading strategy, `StrategyBiotechNews`, as its primary implementation. This strategy uses an LLM (GPT-4) to evaluate the expected impact of biotech company press releases on their stock performance.

## Overview

`QuantGPT` provides a basis for developing and testing quantitative strategies that incorporate LLMs for sentiment analysis and other forms of textual analysis. The `StrategyBiotechNews` exemplifies this, combining market signals with sentiment output derived from press releases to generate trading signals.

Please note that while `StrategyBiotechNews` was backtested and showed a Sharpe ratio of ~2 over a 2-year window, these results might be biased, and this strategy, like all other aspects of `QuantGPT`, is intended for educational purposes and should not be used for live trading without thorough additional testing and validation.

## Dependencies

Dependencies for this project are managed using a `requirements.txt` file.

## Configuration

Strategies implemented in `QuantGPT` use a configuration file in JSON format. Here's an example:

```json
{
    "calendar_name": "NYSE",
    "allocation_config": {
        "base": "core",
        "interior": "config",
        "config_type": "allocation",
        "prod_type": "test",
        "name": "test_alloc_v0p0"
    },
    "delta_to_close_timestamp": "00:00:00",
    "max_cores": 8,
    "db_connections": {
        "scraped": "csv",
        "test_equities": "csv",
        "test_scraped": "csv"
    },
    "trading_times": "nyc_daily_open",
    "observed_data_lookback": "30_days"
}
```

## Data

The input data for the strategies consists of various data types, depending on the specific strategy. For `StrategyBiotechNews`, daily Open/High/Low/Close (OHLC) stock price data and press releases, both scraped from Yahoo Finance, are used.

## Output

The output of the strategies includes results specific to each strategy. For `StrategyBiotechNews`, this includes PnL results and position results, stored in CSV format. The PnL results contain information about new trades and positional trades for each timestamp, while the position results contain information about the average price, quantity, symbol, and timestamp for each position.

## Usage

To run a strategy, use a command following this format:

```bash
python3 -m quantgpt.core.main --start 2020-08-01 --global_config_path_str  quantgpt_core_config_global_test_test_simple_biotech_news_v0p0
```

## Limitations

`QuantGPT` is a work in progress. The framework and its strategies are experimental and should be used as-is and for educational purposes only. Further validation and testing are required for live trading.

---

This draft is intended to be flexible and can be modified to better suit the evolving nature of your `QuantGPT` project.
