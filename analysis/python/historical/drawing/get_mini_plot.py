import matplotlib.pyplot as plt
import seaborn as sns
import base64
from io import BytesIO
from matplotlib.figure import Figure
import numpy as np

sns.set_style("whitegrid")
def get_plot_as_img(x_vals,y_vals):
    # metric_values should be a dict where keys are interval names (2019A)
    # and values are metric values
    buf = BytesIO()
    plt.figure(figsize=(4,1.5))
    x_labels = x_vals
    x_vals = np.arange(float(len(x_labels)))
    fig = sns.lineplot(x=x_vals, y=y_vals, color="#666666")
    plt.xticks(x_vals)
    fig.set_xlim((0,len(x_vals)-1))
    #fig.set_yticklabels(fig.get_yticklabels(), fontsize='x-small')
    fig.set_xticklabels(x_labels, rotation=90, horizontalalignment='center', fontsize='x-small')
    plt.savefig(buf, format="png", bbox_inches='tight')
    buf.seek(0)
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return f"<img src='data:image/png;base64,{data}'/>"


if __name__ == "__main__":
    import historical.util as util
    data = [0.00,0.00,0.00,0.00,0.03,0.02,0.06,0.07,0.07,0.10,0.13,0.12,0.10,0.15,0.12,0.13,0.06,0.14,0.10,0.10,0.07,0.14]
    ys = list(util.iter_yearseason())
    print(get_plot_as_img(ys, data))
