import matplotlib.pyplot as plt
from typing import Union


def parse_number(s: str) -> Union[int, float]:
    try:
        return int(s)
    except ValueError:
        return float(s)


def create_figure(csv_path: str, y_label: str, with_text, save_path: str):
    headers = list()
    data = list()
    with open(csv_path) as f:
        for header in f.readline()[:-1].split(","):
            headers.append(header)
            data.append(list())
        for line in f.readlines():
            i = 0
            for number in line[:-1].split(","):
                data[i].append(parse_number(number))
                i += 1

    fig, ax = plt.subplots()
    fig.set_size_inches(6, 4)
    fig.set_dpi(200)
    fig.tight_layout(pad=2)
    fig.suptitle(y_label)
    ax.grid(True, which="both")
    ax.set_xlabel("year")
    ax.set_ylabel(y_label)
    ax.set_xticks(
        data[0],
        data[0],
        rotation=-45
    )
    for i in range(1, len(data)):
        ax.plot(data[0], data[i], label=headers[i], marker=".", antialiased=True)
        if with_text:
            for j in range(len(data[0])):
                ax.text(data[0][j], data[i][j], f"{round(data[i][j], 2)}\n")
    ax.legend()
    fig.savefig(save_path, pad_inches=0)
    plt.close()


if __name__ == "__main__":
    create_figure("results/Figure 1.1.csv", "Number of ASes", False, "results/Figure 1.1.png")
    create_figure("results/Figure 1.2.csv", "Number of AS links", False, "results/Figure 1.2.png")
    create_figure("results/Figure 7.csv", "Average AS Path Length", False, "results/Figure 7.png")
    create_figure("results/Figure 8.csv", "Fraction of all ASes present in IPv6 graph", False, "results/Figure 8.png")
