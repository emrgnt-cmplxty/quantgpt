import logging

logger = logging.getLogger(__name__)


class PerformancePlotter:
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def plot_backtest(self):
        # pnl_df = pd.DataFrame(self.portfolio.PnL)
        # cum_returns = (1 + pnl_df).cumprod() - 1
        # trace = go.Scatter(x=cum_returns.index, y=cum_returns, mode='lines', name='Cumulative Returns')
        # layout = go.Layout(title='Backtest Results', xaxis={'title': 'Date'}, yaxis={'title': 'Returns'})
        # fig = go.Figure(data=[trace], layout=layout)
        # fig.show()
        pass

    def plot_positions(self):
        # positions_df = pd.DataFrame(self.portfolio.get_positions())
        # positions_df['date'] = pd.to_datetime(positions_df['date'])
        # positions_df = positions_df.set_index('date')
        # fig = go.Figure()
        # for symbol in positions_df['symbol'].unique():
        #     trace = go.Scatter(x=positions_df[positions_df['symbol'] == symbol].index,
        #                        y=positions_df[positions_df['symbol'] == symbol]['quantity'],
        #                        mode='lines+markers', name=symbol)
        #     fig.add_trace(trace)
        # layout = go.Layout(title='Open AggregatedPositions', xaxis={'title': 'Date'}, yaxis={'title': 'Quantity'})
        # fig.update_layout(layout)
        # fig.show()
        pass

    def plot(self):
        self.plot_positions()
        self.plot_backtest()
