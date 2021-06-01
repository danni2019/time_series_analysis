import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def draw_overlay_lines(
        data: pd.DataFrame,
        secondary_y: [str, False] = False,
        width: int = 16,
        height: int = 9,
        filename: [str, None] = None,
        **kwargs,
):
    img = data.plot(
        figsize=(width, height),
        secondary_y=secondary_y,
        colormap='tab20',
        **kwargs,
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_singal_line(
        data: pd.DataFrame,
        width: int = 16,
        height: int = 9,
        filename: [str, None] = None
):
    img = data.plot(
        figsize=(width, height),
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_scatter(
        data: pd.DataFrame,
        x: str,
        y: str,
        groupby: [str, None] = None,
        width: int = 16,
        height: int = 9,
        filename: [str, None] = None
):
    sns.set(rc={'figure.figsize': (width, height)})
    fig = plt.figure(figsize=(width, height))
    img = sns.scatterplot(data=data, x=x, y=y, hue=groupby, )
    if filename is None:
        return img
    else:
        plt.savefig(filename)
    plt.close()






class Plot_ts:

    def plot_pnl_period_cumsum(self,meta_data):
        meta_data['pnl_period_cumsum'].plot()
        plt.show()

class Plot_cs:
    pass