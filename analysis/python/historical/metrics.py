import numpy as np
import functools
import scipy.signal

#Metrics functions
#Metric signature: metric(vals, pseudocount, slope_thresh)

#variance
def __variance(vals,*args):
    return np.var(vals)

def __diff(vals,*args):
    return abs(np.max(vals) - np.min(vals))

def __gain(vals,*args):
    return vals[-1] - vals[0]

def __loss(vals,*args):
    return -vals[-1] + vals[0]

def __sum(vals,*args):
    return sum(vals)

def __spike(vals,*args):
    proms = scipy.signal.find_peaks(vals,prominence=0)[1]["prominences"]
    if len(proms) > 0:
        return max(proms)
    else:
        return -100000

def local_pos_slope(w, vals, pseudocount, slope_thresh):
    ar = [vals[i] - vals[i-w] for i in range(w,len(vals))]
    return max(ar)

def local_neg_slope(w, vals, pseudocount, slope_thresh):
    ar = [vals[i] - vals[i-w] for i in range(w,len(vals))]
    return -min(ar)

def local_pos_growth(w, vals, pseudocount, slope_thresh):
    ar = [(vals[i] - vals[i-w] + pseudocount) / (vals[i-w] + pseudocount)
          for i in range(w,len(vals))
          if (vals[i-w] > slope_thresh or vals[i] > slope_thresh)
    ]
    if len(ar) > 0:
        return max(ar)
    else:
        return -100000

def local_neg_growth(w, vals, pseudocount, slope_thresh):
    ar = [(vals[i] - vals[i-w] + pseudocount) / (vals[i] + pseudocount)
          for i in range(w,len(vals))
          if (vals[i-w] > slope_thresh or vals[i] > slope_thresh)
    ]
    if len(ar) > 0:
        return -min(ar)
    else:
        return -100000

def __add_windowed_metric(metric_name,metric_fxn,metric_hname,w_start,w_stop):
    for w in range(w_start,w_stop+1):
        metrics[metric_name.format(w)]=(
            functools.partial(metric_fxn,w),
            metric_hname.format(w)
        )
        
metrics = {
    "VAR":(
        __variance,
        "Variance"
    ),
    "DIFF":(
        __diff,
        "Largest max-min"
    ),
    "GAIN":(
        __gain,
        "Largest overall gain"
    ),
    "LOSS":(
        __loss,
        "Largest overall loss"
    ),
    "SUM":(
        __sum,
        "Largest sum"
    ),
    "SPIKE":(
        __spike,
        "Largest peak"
    ),
}

__add_windowed_metric("LOCAL_{0}_POS_SLOPE",local_pos_slope,
                      "Largest positive jump over {0} intervals",1,2)

__add_windowed_metric("LOCAL_{0}_NEG_SLOPE",local_neg_slope,
                      "Largest negative jump over {0} intervals",1,2)

__add_windowed_metric("LOCAL_{0}_POS_GROWTH",local_pos_growth,
                      "Largest positive growth over {0} intervals",1,2)

__add_windowed_metric("LOCAL_{0}_NEG_GROWTH",local_neg_growth,
                      "Largest negative growth over {0} intervals",1,2)



def get_metrics():
    return metrics


if __name__ == "__main__":
    vals = [
        0.09,0.13,0.11,0.12,0.13,0.12,0.12,0.13,0.13,0.14,0.14,0.15,0.15,0.16,0.15,0.17,0.17,0.19,0.24,0.22,0.19,0.18
        ]
    print("Values:\n%s" % ",".join((str(v) for v in vals)))
    for metric_name,(score_function,human_name) in metrics.items():
        print("%s: %f" % (human_name,score_function(vals,0.01,0.05)))


    
