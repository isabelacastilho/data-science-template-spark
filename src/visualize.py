import seaborn as sb
from import_data import import_csv_data
import matplotlib.pyplot as plt


def setup_seaborn(context="talk", palette="muted"):
    """Example palettes:
    BuGn_d, GnBu_d, Paired, PRGn, YlGnBu
    Example contexts:
    paper, notebook, talk, poster"""
    sb.set(context=context)
    sb.set_palette(palette)


def plot_distribution(data, column, save_as = 'not to save'):
    """Plots a histogram and fits a KDE"""
    sb.distplot(data[column])

    if save_as != 'not to save':
        plt.savefig("../reports/figures/" + save_as + ".png")
    sb.plt.show()


def plot_categorical_count(data, column, save_as = 'not to save'):
    """Plots a histogram of a categorical variable"""
    sb.countplot(x=column, data=data)

    if save_as != 'not to save':
        plt.savefig("../reports/figures/" + save_as + ".png")
    sb.plt.show()


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv')
    setup_seaborn()
    plot_distribution(raw_data, 'price', "fig1")
