import plotly.graph_objects as go
import numpy as np
import csv
import os
import sys
import historical.util as util
import plotly
import math

#WEB_PREFIX = "https://cs.princeton.edu/~rbamos"
#WEB_DIR = os.path.expanduser("~/public_html/")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""Usage:
python3 -m historical.draw_freq_dist <n>""")
        exit(-1)
    n = sys.argv[1]
    
    np.set_printoptions(threshold=sys.maxsize)

    yss = [ys for ys in util.iter_yearseason()]
    
    fig = go.Figure()
    
    b=2
    max_count = 0
    for ys in yss:
        freqs = [r[0] for r in util.load_grams(n,ys)]
        max_count = max(max_count, len(freqs))
    log_index = [int(math.pow(b,i)) for i in range(int(math.log(max_count,b)))]
    for ys in yss:
        freqs = [r[0] for r in util.load_grams(n,ys)]
        freqs2 = [freqs[i] if i < len(freqs) else 0 for i in log_index]
        fig.add_trace(go.Scatter(x=log_index, y=freqs2,
                                 mode='lines',
                                 name=ys))
    fig.update_layout(xaxis_type="log")
    fn,url = util.get_web_fn('graphs', "lines", "aggregate", 'dist_%s.html' % n)
#    fn = os.path.join(WEB_DIR, 'graphs', "lines", "aggregate", 'dist_%s.html' % n)
    plotly.offline.plot(fig, filename=fn, auto_open=False)
#    url="%s/graphs/lines/aggregate/dist_%s.html" % (WEB_PREFIX, n)
    print("Published to: %s " % url)



