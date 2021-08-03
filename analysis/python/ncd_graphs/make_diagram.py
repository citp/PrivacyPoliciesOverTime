import networkx as nx
import csv
import scipy.sparse as sparse
import numpy as np
import heapq
import traceback

import pygraphviz as pgv
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use("Agg")
import os

try:
    import pathlib2 as path
except ImportError:
    import pathlib as path

from networkx import community
import matplotlib.patches as mpatches

def load_histogram(fn):
    #Returns the distances pdf and cdf
    with open(fn, 'r') as f:
        read = csv.reader(f)
        names = next(read)[1:]
        print("Reading matrix size %d x %d" % (len(names),len(names)))
        total_vol = len(names) * len(names) / 2

        buckets = {}

        i = 0
        old_perc = -1
        for row in read:
            j = -1
            for elem in row:
                if i == j:
                    continue
                if j == -1:
                    j += 1
                    continue
                if elem == '':
                    break
                val = int(float(elem)*100)
                if val not in buckets:
                    buckets[val] = 1
                else:
                    buckets[val] += 1
                j += 1
            vol = i * (j-1) / 2
            new_perc = vol * 100 / total_vol
            if old_perc != new_perc:
                print("%d%%" % new_perc)
                old_perc = new_perc
            i += 1

        total_vol_f = float(total_vol)
        pdf = [buckets[val] / total_vol_f if val in buckets else 0 for val in range(max(buckets)+1) ]
        cdf = [0] * len(pdf)
        for val in range(len(cdf)):
            if val != 0:
                cdf[val] = cdf[val-1]
            cdf[val] += pdf[val]
            
        return pdf,cdf

def load_names(fn):
    with open(fn, 'r') as f:
        read = csv.reader(f)
        names = next(read)[1:]
        return names

    
def load_diagram(fn, cutoff=0):
    with open(fn, 'r') as f:
        read = csv.reader(f)
        names = next(read)[1:]
        print("Reading matrix size %d x %d" % (len(names),len(names)))
        total_vol = len(names) * len(names) / 2
        coords = ([],([],[]))
        i = 0
        old_perc = -1
        for row in read:
            j = -1
            for elem in row:
                if i == j: continue
                if j == -1:
                    j += 1
                    continue
                if elem == '':
                    break
                val = float(elem)
                if val < cutoff:
                    coords[0].append(val)
                    coords[1][0].append(i)
                    coords[1][1].append(j)
                j += 1
            vol = i * (j-1) / 2
            new_perc = vol * 100 / total_vol
            if old_perc != new_perc:
                print("%d%%" % new_perc)
                old_perc = new_perc
            i += 1
#            if new_perc >= 20:
#                break
        
        return coords,names #sparse.coo_matrix(coords,shape=(len(names),len(names)), dtype=np.float32)

def do_dist():
    #Writes the PDF and CDF of distances
    pdf,cdf = load_histogram("ncd.csv")
    with open("cdf.csv", "w+") as f:
        for v in range(len(cdf)):
            f.write("%.2f,%f\n" % (v / 100.0,cdf[v]))

    with open("pdf.csv", "w+") as f:
        for v in range(len(cdf)):
            f.write("%.2f,%f\n" % (v / 100.0,cdf[v]))


def get_cc_hist(fn,steps=100,offset=0):
    #Generates and writes histogram and other data about connect components at increasing cutoffs
    edges,names=load_diagram(fn,cutoff=1)
    weights = [(x,edges[0][x]) for x in range(len(edges[0]))]

    if offset < 0:
        offset = len(weights) + offset
    
    total_edges = len(weights) - offset
    print("Sorting...")
    if offset == 0:
        weights.sort(key=lambda x: -x[1])
    else:
        weights = heapq.nsmallest(total_edges, weights, key=lambda w: w[1])
        weights = list(reversed(weights))
    
    step_size = total_edges/steps
    hist=[0]*(steps+1)
    step_list=[total_edges-(i*step_size) for i in range(steps+1)]
    print("Making graph")
    G = nx.Graph()
    
    print("Adding edges from %f to %f" % (weights[0][1], weights[-1][1]))
    for i in range(len(weights)):
        j = weights[i][0]
        G.add_edge(edges[1][0][j],edges[1][1][j],weight=edges[0][j])
    with open("cc.csv", "w+") as f:
        try:
            writer = csv.writer(f)
            writer.writerow(["Dist", "Edges", "Non-trivial CCs", "CCs", "NT CC vals"])
            for i in range(steps):
                start = i * step_size
                end = min((i + 1) * step_size, len(weights))

                print("Step %d (%d -> %d)" % (i, start, end))
                print("Removing edges from %f to %f" % (weights[start][1], weights[end-1][1]))
                for k in range(start,end):
                    j = weights[k][0]
                    G.remove_edge(edges[1][0][j],edges[1][1][j])
                lens=[len(cc) for cc in nx.connected_components(G)]
                #if any((x != 1 for x in lens)):
                #    print(lens)
                nt_lens = sorted([ l for l in  lens if l > 1], reverse=True)
                hist[i] = len(nt_lens)
                writer.writerow([
                    weights[end-1][1],
                    step_list[i],
                    hist[i],
                    len(lens),
                    "{%s}" % (",".join((str(l) for l in nt_lens)))
                ])
        except:
            traceback.print_exc()
    return step_list, hist

def load_graph(cutoff=0):

    ajl_name = "graph_%0.2f.ajl" % cutoff
    if path.Path(ajl_name).is_file():
        G = nx.read_adjlist(ajl_name)
        names = load_names("ncd.csv")
#        names = []
#        names = [G[i]["label"] for i in G.nodes()]
    else:
        coords,names = load_diagram("ncd.csv",cutoff=cutoff)

        name_used = [False] * len(names)
        for x in range(len(coords[0])):
            name_used[coords[1][0][x]] = True
            name_used[coords[1][1][x]] = True
        print("%d nodes" % sum(name_used))

        G=nx.Graph()
        _names = []
        for x in range(len(names)):
            if name_used[x]:
                G.add_node(x, label=names[x])

        names = _names
            
        for x in range(len(coords[0])):
            G.add_edge(coords[1][0][x], coords[1][1][x],weight=coords[0][x])

        if cutoff != 1:
            nx.write_adjlist(G, ajl_name)
    return G,names

        
    

#Uncomment these two lines to get the distance distribution
#do_dist()
#exit()


#Uncomment these two lines to connected component histogram
steps,hist = get_cc_hist("ncd.csv",steps=1000, offset=999900)
exit()

G,names = load_graph(cutoff=0.58)

i = 0
for cc in nx.connected_components(G):
    sg = G.subgraph(cc)
    if len(sg.nodes) == 1:
        continue
#    communities = list(community.girvan_newman(G))
    communities = list(community.greedy_modularity_communities(sg))
    colorCt = len(communities)
    if colorCt <= 20:
        colors = [plt.get_cmap("tab20").colors[j] for j in range(colorCt)]
    elif colorCt <= 40:
        colors = [plt.get_cmap("tab20").colors[j] for j in range(20)] + [plt.get_cmap("tab20b").colors[j] for j in range(colorCt-20)]
    else:
        raise Error("Too many colors")
    node_colors = [(0,0,0)] * len(sg.nodes)
    keys = list(sg.nodes.keys())
    node_map = {keys[i]: i for i in range(len(sg.nodes))}
    for j in range(len(communities)):
        for n in communities[j]:
            node_colors[node_map[n]] = colors[j]
    patches = [mpatches.Patch(color=colors[j], label="Group %d" % (j+1)) for j in range(colorCt)]
    nx.draw_spring(sg, node_size=5, node_color=node_colors)
    plt.legend(handles=patches)
    plt.savefig('data/cc_%d.png' % i)
    plt.clf()
    print("Saved cc_%d.png" % i)

    with open("data/cc_%d.csv" % i, "w+") as f:
        writer = csv.writer(f)
        writer.writerow(["Group #", "Policies"])
        for j in range(colorCt):
            try:
                writer.writerow([str((j+1))] + [names[int(k)] for k in communities[j]])
            except:
                print j
                print k
                print communities[j]
                raise
    
    i += 1
    
# print("Making layout")
# G.layout()
# G.write("graph_%f.dot" % (cutoff))

# print("Drawing")
# G.draw('diagram.svg', prog='neato')
