import plotly.graph_objects as go
import numpy as np
import csv
import os
import sys
import historical.util as util
import plotly
import textwrap

#WEB_PREFIX = "https://cs.princeton.edu/~rbamos/policies"
#WEB_DIR = os.path.expanduser("~/public_html/policies")

def get_frequency_data(fn,maxCount=-1):
    with open(fn, "r") as f:
        reader = csv.reader(f)
        ct = 0
        for line in reader:
            label = line[0].replace("\\n", "\n").replace("\\\\", "\\")
            data = line[1:]
            yield label, data
            ct += 1
            if ct == maxCount:
                return

def get_lines_url(filename, *args, **kwargs):
    fn,url = util.get_web_fn('graphs', "lines", '%s.html' % os.path.basename(filename))
    return url

def draw_lines(filename, topX, freq_data=None):
    """
    Frequency data is an array of tuples. The first item is the phrase,
    the second item is the number of occurances at each interval
    """

    opts = np.get_printoptions()
    np.set_printoptions(threshold=sys.maxsize)


    fn,url = util.get_web_fn('graphs', "lines", '%s.html' % os.path.basename(filename))
    if os.path.exists(fn):
#        print("Skipping line drawing")
        return url
    
    yss = [ys for ys in util.iter_yearseason()]



    if freq_data is None:
        freq_data = get_frequency_data(filename,maxCount=topX)

    freq_data = list(freq_data)
    
    
    fig = go.Figure()

#    layout=go.Layout(
#            legend=dict(x=-.1, y=-0.5*(len(freq_data) // 10))
#        )

    
    for label, data in freq_data:
        #wrappedlabel = textwrap.fill(label, 40)
        if len(label) >= 40:
            wrappedlabel = label[:37] + "..."
        else:
            wrappedlabel = label
        fig.add_trace(go.Scatter(x=yss, y=data,
                                 mode='lines',
                                 name=wrappedlabel))
    plotly.offline.plot(fig, filename=fn, auto_open=False)
    np.set_printoptions(opts)
    return url
            
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""Usage:
python3 -m historical.draw_lines <filename> <topX>""")
        exit(-1)
    filename = sys.argv[1]
    topX = int(sys.argv[2])
    url = draw_lines(filename, topX)
    print("Published to: %s " % url)



