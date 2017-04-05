import seaborn as sb
from import_data import import_csv_data
import matplotlib.pyplot as plt


def setup_seaborn():
    sb.set(context="talk")
    # sb.set_palette("BuGn_d")
    # sb.set_palette("GnBu_d")
    # sb.set_palette("Paired")
    # sb.set_palette("PRGn")
    # sb.set_palette("YlGnBu")
    # sb.set_palette("Paired")
    sb.set_palette("muted")


def plot_distribution(data, column, save_as = 'not to save'):
    """Plots a histogram and fits a KDE"""
    sb.distplot(data[column])

    if save_as != 'not to save':
        plt.savefig("../reports/figures/" + save_as + ".png")
    sb.plt.show()


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv')
    setup_seaborn()
    plot_distribution(raw_data, 'price', "fig1")