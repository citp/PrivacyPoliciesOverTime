import historical.util as util
import igraph as ig
import os
import argparse
import networkx as nx
import plotly
import chart_studio.plotly as py
import plotly.graph_objects as go
import matplotlib.pyplot as plt

WEB_DIR = util.WEB_DIR

def flatten_doms(doms, all_doms_l):
    ret = set()
    #print(doms)
    for domId in range(len(doms)):
        for ysId in range(len(doms[domId])):
            if doms[domId][ysId] == 1:
                ret.add("%s_%s" % (ysId, all_doms_l[domId]))
    return ret

sim_cache = {}
dom_cache = {}
def similarity(phrase1, phrase2,n):
    if phrase2 < phrase1:
        tmp = phrase1
        phrase1 = phrase2
        phrase2 = tmp
    t = (phrase1,phrase2)
    if t in sim_cache:
        return sim_cache[t]
    doms1, all_doms_l1 = util.fetch_domains(phrase1,dom_cache,n=n)
    doms2, all_doms_l2 = util.fetch_domains(phrase2,dom_cache,n=n)
    doms1 = flatten_doms(doms1, all_doms_l1)
    doms2 = flatten_doms(doms2, all_doms_l2)
    if len(doms1) == 0:
        print(phrase1)
    if len(doms2) == 0:
        print(phrase2)
    sim = len(doms1 & doms2) / len(doms1 | doms2)
    sim_cache[t] = sim
    return sim

def gen_network(phrases,n):
    G = nx.Graph()
    G2 = nx.Graph()
    for p1,mets1 in phrases.items():
        for p2,mets2 in phrases.items():
            if p1 == p2:
                continue
            sim = similarity(p1,p2,n)
            G.add_edge(p1,p2,weight=sim)
            G2.add_node(p1)
            G2.add_node(p2)
            if sim > 0.5:
                G2.add_edge(p1,p2,weight=sim)
    return G, G2

def gen_igraph_network(phrases,n):
    G = ig.Graph()
    for p1,_ in phrases.items():
        G.add_vertex(p1,label=p1)
    for p1,mets1 in phrases.items():
        for p2,mets2 in phrases.items():
            if p1 == p2:
                continue
            sim = similarity(p1,p2,n)
            if sim > 0.5:
                G.add_edge(p1,p2,weight=sim)

    return G

                

def draw_networkx_phrases(n,t):
    fn, url = util.get_web_fn("graphs", "networks", '%s_%s.png' % (n,t))
    phrases = {}
    for phrase, metrics in util.load_top_phrases(n,t=t):
        phrases[phrase]=metrics
    G, G2 = gen_network(phrases,n)
    pos = nx.spring_layout(G)  # positions for all nodes

    elarge = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] > 0.5]
    esmall = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] <= 0.5]
    
    # nodes
    nx.draw_networkx_nodes(G, pos, node_size=70)

    # edges
    nx.draw_networkx_edges(G, pos, edgelist=elarge,width=2)
    #nx.draw_networkx_edges(G, pos, edgelist=esmall,width=2, alpha=0.5, edge_color='b', style='dashed')

    # labels
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')
    plt.axis('off')

    
    plt.savefig(fn)
    
    print("Published to: %s " % url)

def draw_network_phrases(n,t):
    return draw_igraph_phrases(n,t)
    
def draw_igraph_phrases(n,t):
    fn, url = util.get_web_fn("networks", '%s_%s.html' % (n,t))
    phrases = {}
    for phrase, metrics in util.load_top_phrases(n,t=t):
        phrases[phrase]=metrics
    G = gen_igraph_network(phrases,n)

    labels=list(G.vs['label'])
    N=len(labels)
    E=[e.tuple for e in G.es]# list of edges
    layt=G.layout('kk') #kamada-kawai layout

    Xn=[layt[k][0] for k in range(N)]
    Yn=[layt[k][1] for k in range(N)]
    Xe=[]
    Ye=[]
    for e in E:
        Xe+=[layt[e[0]][0],layt[e[1]][0], None]
        Ye+=[layt[e[0]][1],layt[e[1]][1], None]

    trace1=go.Scatter(x=Xe,
                      y=Ye,
                      mode='lines',
                      line= dict(color='rgb(210,210,210)', width=1),
                      hoverinfo='none'
    )
    trace2=go.Scatter(x=Xn,
                      y=Yn,
                      mode='markers',
                      name='ntw',
                      marker=dict(symbol='circle-dot',
                                  size=5,
                                  color='#6959CD',
                                  line=dict(color='rgb(50,50,50)', width=0.5)
                      ),
                      text=labels,
                      hoverinfo='text'
    )
    
    axis=dict(showline=False, # hide axis line, grid, ticklabels and  title
              zeroline=False,
              showgrid=False,
              showticklabels=False,
              title=''
    )
    
    width=800
    height=800
    layout=go.Layout(title= "Similarity for phrases with n=%s" % n,
                  font= dict(size=12),
                  showlegend=False,
                  autosize=False,
                  width=width,
                  height=height,
                  xaxis=go.layout.XAxis(axis),
                  yaxis=go.layout.YAxis(axis),
                  margin=go.layout.Margin(
                      l=40,
                      r=40,
                      b=85,
                      t=100,
                  ),
                  hovermode='closest',
                  annotations=[
                      dict(
                          showarrow=False,
                          text='This igraph.Graph has the Kamada-Kawai layout',
                          xref='paper',
                          yref='paper',
                          x=0,
                          y=-0.1,
                          xanchor='left',
                                                                 yanchor='bottom',
                          font=dict(
                              size=14
                          )
                      )
                  ]
    )
    
    data=[trace1, trace2]
    fig=go.Figure(data=data, layout=layout)
    plotly.offline.plot(fig, filename=fn, auto_open=False)
    
    print("Published to: %s " % url)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Runs basic analytics over pre-sorted n-grams')
    parser.add_argument(dest="n", type=str,
                                            help='An integer for n-grams, or "w" or "s" for words and sentences respectively')
    parser.add_argument(dest="metric", type=str,
                                            help='A string indicating the ranking metric. "var" and "diff" are currently supported')
    util.add_arguments(parser)

    args = parser.parse_args()
    util.process_arguments(args)

    n = args.n
    t = args.metric
    G = draw_network_phrases(n,t)
