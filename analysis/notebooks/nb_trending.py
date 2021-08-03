import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandarallel
import re
import os

#pandarallel.pandarallel.initialize()


DEFAULT_MIN_YEAR = 2009

cache = {}

sns.set_style("whitegrid")


DEFAULT_START_YEAR = 2000
DEFAULT_PLOT_COLOR = "steelblue"
INTERVAL_COL_NAME = "year_season"
FIGURES_FOLDER = "figures"
#MIN_YEAR = df.year.min()
#MIN_YEAR
DEFAULT_MIN_YEAR = 2009
DEFAULT_PLOT_COLOR = "steelblue"
INTERVAL_COL_NAME = "year_season"

def search_for_context(df, search, regex=False, case=False, min_year=DEFAULT_MIN_YEAR, label=None,final=False, dfId=None):
    
    text_search = None
    url_search = None
    if type(search) != str:
        text_search = search[0]
        url_search = search[1]
    else:
        text_search = search
        
    if regex and not case:
        flags = re.IGNORECASE
    else:
        flags = 0
        
    if 'policy_text' in df.columns:
        print("Text search")
        # policy text
        if url_search is not None:
            matches = df[df.links_str.str.contains(url_search, regex=regex, case=case, na=False, flags=flags) | df.policy_text.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        else:
            matches = df[df.policy_text.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        #matches = df[search_parallel(df.policy_text, search, regex=regex, case=case, na=False)]
    else:
        print("HTML Search")
        # policy html dataframe -- policy html sources
        if url_search is not None:
            matches = df[df.links_str.str.contains(url_search, regex=regex, case=case, na=False, flags=flags) | df.policy_source.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        else:
            matches = df[df.policy_source.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        #matches = df[search_parallel(df.policy_source, search, regex=regex, case=case, na=False)]
        
    if len(matches) < 100:
        matches_sample = matches
    else:
        matches_sample = matches.sample(100)
        
    text_re = re.compile(text_search,flags=re.IGNORECASE)
    for idx, row in matches_sample.iterrows():
        print(f"{row.site_url} {row.year_season}")
        pos = 0
        while True:
            match = text_re.search(row.policy_text,pos)
            if match == None:
                break
            start = max(match.start()-100,0)
            end = min(match.end()+100,len(row.policy_text))
            pos = match.end() + 1
            print(row.policy_text[start:end])
            print("-"*40)
        print("="*40)

def search_parallel(series, query, regex=False, case=False):
    if regex:
        q_re = re.compile(query)
        flags = 0
        if not case:
            flags |= re.IGNORECASE
        def search_fxn(s):
            return bool(q_re.match(s,flags))
    else:
        if not case:
            query = query.lower()
        def search_fxn(s):
            if not case:
                s = s.lower()
            return query in s
    return series.parallel_map(search_fxn,na_action="ignore")

def search_term(df, search, regex=False, case=False, min_year=DEFAULT_MIN_YEAR, label=None,final=False, dfId=None,dump_matches=False):
    """Search a given search term in the policy_text dataframe."""
    if dfId is not None:
        identifier = (label,dfId,regex,case,min_year,final,search)
    else:
        identifier = (label,regex,case,min_year,final,search)
    if identifier in cache:
        return cache[identifier]
    
    if label is None:
        label = search
    
    text_search = None
    url_search = None
    if type(search) != str:
        text_search = search[0]
        url_search = search[1]
    else:
        text_search = search
    
    policy_counts = df[df.year>=min_year].groupby('year_season').size().to_dict()
    if regex and not case:
        flags = re.IGNORECASE
    else:
        flags = 0
    if 'policy_text' in df.columns:
        # policy text
        if url_search is not None:
            matches = df[df.links_str.str.contains(url_search, regex=regex, case=case, na=False, flags=flags) | df.policy_text.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        else:
            print(f"Policy text without URLs. {text_search}, regex={regex}, case={case}, flags={flags}")
            matches = df[df.policy_text.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        #matches = df[search_parallel(df.policy_text, search, regex=regex, case=case, na=False)]
    else:
        # policy html dataframe -- policy html sources
        if url_search is not None:
            matches = df[df.links_str.str.contains(url_search, regex=regex, case=case, na=False, flags=flags) | df.policy_source.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        else:
            matches = df[df.policy_source.str.contains(text_search, regex=regex, case=case, na=False, flags=flags)]
        #matches = df[search_parallel(df.policy_source, search, regex=regex, case=case, na=False)]
    abs_counts = matches.groupby('year_season').size().to_dict()
    percentages = [
        (interval, 100*abs_counts.get(interval, 0)/policy_count, label if final else "%s | %s" % (label,search))
                for interval, policy_count in policy_counts.items()]
    # return a dataframe of ['interval', 'percentage', 'search_term']
    # where percentage is normalized frequency of policies that contain the
    # given search term
    results_df = pd.DataFrame(percentages, columns=['interval', 'percentage', 'search_term'])
    
    if dump_matches:
        chop_str = lambda s: s[:50] if len(s) > 50 else s
        fn = "matches/%s.csv" % ",".join(map(chop_str,map(str,identifier[:-1]))).replace("/","-")
        #matches["homepage_snapshot_url","year"].groupby("year")
        
        
        if len(matches) < 100:
            matches_sample = matches
        else:
            matches_sample = matches.sample(100)
            
        if regex:
            text_re_search = re.compile(text_search,flags=flags)
            if url_search is not None:
                url_re_search = re.compile(url_search,flags=flags)
            def search_text(s):
                match = text_re_search.search(s.policy_text)
                if match is None:
                    match = url_re_search.search(s.links_str)
                return match.group(0)
            matches_sample["match_str"] = matches_sample.apply(search_text,axis=1)
        else:
            matches_sample["match_str"] = text_search
        matches_sample.to_csv(fn,columns=["year_season","policy_snapshot_url","match_str"])
        
    cache[identifier] = results_df
    
    return results_df


def plot_term(df, term, regex=False, case=False, min_year=DEFAULT_MIN_YEAR, save_figure=True):
    plt.figure(figsize=(10,5))
    percentages = search_term(df, term, regex, case, min_year)
    fig = sns.lineplot(x="interval", y="percentage", data=percentages)
    fig.set_xticklabels(fig.get_xticklabels(), rotation=45, fontsize='small')
    if not final:
        title = "Query: %s\n(Min-year: %s, Regex: %s, Case sensitive: %s)" % (term, min_year, regex, case)
        fig.set_title(title)
    
    plt.ylim(ymin=0)
    
    if save_figure:
        s_fig = fig.get_figure()
        s_fig.savefig("figures/%s_%s_%s_%s.png" % (term, min_year, regex, case), bbox_inches='tight')
    return fig


def plot_terms(df, terms, regex=False, case=False, labels=None, min_year=DEFAULT_MIN_YEAR, save_figure=False,markers=False,final=False,bottom_left=False, dfId=None, thin=False,plotkwargs=None,figkwargs=None,legendkwargs=None,nofig=False,style=None,dump_matches=True):
    
    if nofig:
        pass
    elif figkwargs:
        plt.figure(**figkwargs)
    elif final:
        if thin:
            plt.figure(figsize=(6,2))
        else:
            plt.figure(figsize=(7,4))
    else:
        plt.figure(figsize=(10,5))
        
    if labels is None:
        labels = terms
    if len(labels) != len(terms):
        raise Exception("Labels & Terms need to be the same length")
        
        
    if style is None:
        style = labels
    assert len(style) == len(terms)
        
    # append the results in a single df, so we can plot easily
    all_results = pd.DataFrame(columns=['interval', 'percentage', 'search_term'])
    # search terms one by one
    for label,term,style_val in zip(labels,terms,style):
        percentages = search_term(df, term, regex=regex, case=case, min_year=min_year,label=label,final=final,dfId=dfId,dump_matches=dump_matches)
        percentages["style"] = style_val
        all_results = all_results.append(percentages)
    # plot the dataframe
    
    all_results = all_results.sort_values(["interval","percentage"],ascending=True)
    
    if plotkwargs:
        fig = sns.lineplot(x="interval", y="percentage", hue='search_term', data=all_results,markers=markers,**plotkwargs)
    else:
        try:
            fig = sns.lineplot(x="interval", y="percentage", hue='search_term', data=all_results,markers=markers,style="style")
        except ValueError:
            fig = sns.lineplot(x="interval", y="percentage", hue='search_term', data=all_results,markers=markers)
    
    if not final:
        title = "Min-year: %s, Regex: %s, Case sensitive: %s" % (min_year, regex, case)
        fig.set_title(title)
    
    handles,figlabels = fig.get_legend_handles_labels()
    
    if style == labels:
        handles = handles[1:-len(style)-1]
        figlabels = figlabels[1:-len(style)-1]
    if legendkwargs:
        plt.legend(handles=handles, labels=figlabels,**legendkwargs)
    elif final:
        if bottom_left:
            plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,0),loc="lower left")
        else:
            plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,1),loc="upper left")
    else:
        plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,-.18),loc="upper left")
    
    ymin,ymax = plt.ylim()
    if ymin > 0:
        ymin = -0.05 * ymax
    plt.ylim(ymin,ymax)

    if final:
        set_x_ticks(all_results)
    else:
        xlabels = sorted(list(set(all_results.interval)))
        xlabels = map(lambda s: s.replace("_",""), xlabels)
        #fig.set_xticklabels(fig.get_xticklabels())
        fig.set_xticklabels(xlabels, rotation=90, fontsize='small')
    
    fig.set_ylabel("Percentage of policies")
    fig.set_xlabel(None)
    
    def save():
        labelN = ", ".join(labels).replace("/","-")
        if len(labelN) > 50:
            labelN = labelN[:50]
        s_fig = fig.get_figure()
        extension = "pdf" if final else "png"
        if type(save_figure) is str:
            fn = save_figure
        elif dfId is None:
            fn = "%s_%s_RE-%s_CS-%s.%s" % (labelN, min_year, regex, case, extension)
        else:
            fn = "%s_%s_RE-%s_CS-%s-%s.%s" % (labelN, min_year, regex, case, dfId, extension)
        fn = "figures/%s" % fn
        s_fig.savefig(fn, bbox_inches='tight')

    return fig, save, all_results



def plot_terms_many_sources(df_list, terms, regex=False, case=False, labels=None, df_labels=None, min_year=DEFAULT_MIN_YEAR, save_figure=False,markers=False,final=False,bottom_left=False, thin=False,plotkwargs=None,figkwargs=None,legendkwargs=None,nofig=False,dump_matches=True):
    
    if nofig:
        pass
    elif figkwargs:
        plt.figure(**figkwargs)
    elif final:
        if thin:
            plt.figure(figsize=(6,2))
        else:
            plt.figure(figsize=(7,3))
    else:
        plt.figure(figsize=(10,5))
        
    if len(labels) != len(terms):
        raise Exception("Labels & Terms need to be the same length")
        
    # append the results in a single df, so we can plot easily
    all_results = pd.DataFrame(columns=['interval', 'percentage', 'search_term'])
    # search terms one by one
    for df,df_label in zip(df_list,df_labels):
        for label,term in zip(labels,terms):
            #label = df_label % label
            #label = label
            percentages = search_term(df, term, regex=regex, case=case, min_year=min_year,label=label,final=final,dfId=df_label,dump_matches=dump_matches)
            percentages["df_label"] = df_label
            all_results = all_results.append(percentages)
    # plot the dataframe
    
    all_results = all_results.sort_values(["interval","percentage"],ascending=True)
    
#     print(all_results)
    fig = sns.lineplot(x="interval", y="percentage", hue='search_term', data=all_results,markers=markers,style="df_label")
    
    if not final:
        title = "Min-year: %s, Regex: %s, Case sensitive: %s" % (min_year, regex, case)
        fig.set_title(title)
    
    handles,figlabels = fig.get_legend_handles_labels()
    
    handles = handles[0:]
    figlabels = figlabels[0:]
    if legendkwargs:
        plt.legend(handles=handles, labels=figlabels,**legendkwargs)
    elif final:
        if bottom_left:
            plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,0),loc="lower left")
        else:
            plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,1),loc="upper left")
    else:
        plt.legend(handles=handles, labels=figlabels,bbox_to_anchor=(0,-.18),loc="upper left")
    
    ymin,ymax = plt.ylim()
    if ymin > 0:
        ymin = -0.05 * ymax
    plt.ylim(ymin,ymax)

    if final:
        set_x_ticks(all_results)
    else:
        xlabels = sorted(list(set(all_results.interval)))
        xlabels = map(lambda s: s.replace("_",""), xlabels)
        #fig.set_xticklabels(fig.get_xticklabels())
        fig.set_xticklabels(xlabels, rotation=90, fontsize='small')
    
    fig.set_ylabel("Percentage of policies")
    fig.set_xlabel(None)
    
    def save():
        labelN = ", ".join(labels).replace("/","-")
        if len(labelN) > 50:
            labelN = labelN[:50]
        s_fig = fig.get_figure()
        extension = "pdf" if final else "png"
        if type(save_figure) is str:
            fn = save_figure
        else:
            fn = "%s_%s_RE-%s_CS-%s-%s.%s" % (labelN, min_year, regex, case, "-".join(df_labels), extension)
        fn = "figures/%s" % fn
        s_fig.savefig(fn, bbox_inches='tight')

    return fig, save

def set_x_ticks(df):
    try:
        unique_intervals = df['interval'].unique()
    except:
        unique_intervals = df['year_season'].unique()
    num_intervals = len(unique_intervals)
    unique_years = sorted(set([int(interval.split("_")[0]) for interval in unique_intervals]))
    plt.xticks(np.arange(0, num_intervals, 2)-0.5, unique_years, rotation="vertical")
    

def set_odd_x_ticks(df):
    try:
        unique_intervals = df['interval'].unique()
    except:
        unique_intervals = df['year_season'].unique()
    num_intervals = len(unique_intervals)
    unique_years = sorted(set([int(interval.split("_")[0]) for interval in unique_intervals]))
    plt.xticks(np.arange(0, num_intervals, 2)-0.5, unique_years, rotation="vertical")

def save_figure(fig, filename, out_dir=FIGURES_FOLDER):
    s_fig = fig.get_figure()
    s_fig.savefig(os.path.join(out_dir, filename), bbox_inches='tight')
    

def make_clickable(link):
    clickable = '<a href="%s">%s</a>' % (link, link)
    return clickable


def perc_func(df, func, min_year=DEFAULT_MIN_YEAR, label=None):
    """Search a given search term in the policy_text dataframe."""
#     identifier = (search,regex,case,min_year,label)
#     if identifier in cache:
#         return cache[identifier]
    
    if label is None:
        label = "Unlabelled"
    
    policy_counts = df[df.year>=min_year].groupby('year_season').size().to_dict()
    try:
        # policy text
        matches = df[df.policy_source.apply(func)]
    except Exception:
        # policy html dataframe -- policy html sources
        matches = df[df.policy_text.apply(func)]
    abs_counts = matches.groupby('year_season').size().to_dict()
    percentages = [
        (interval, 100*abs_counts.get(interval, 0)/policy_count, "%s" % (label))
                for interval, policy_count in policy_counts.items()]
    # return a dataframe of ['interval', 'percentage', 'search_term']
    # where percentage is normalized frequency of policies that contain the
    # given search term
    results_df = pd.DataFrame(percentages, columns=['interval', 'percentage', 'search_term'])
    #cache[identifier] = results_df
    return results_df


def plot_func(df, func, label="", min_year=DEFAULT_MIN_YEAR, save_figure=False,markers=False):
    plt.figure(figsize=(10,5))
    # append the results in a single df, so we can plot easily
    all_results = pd.DataFrame(columns=['interval', 'percentage', 'search_term'])
    # search terms one by one
    percentages = perc_func(df, func, min_year=min_year,label=label)
    all_results = all_results.append(percentages)
    # plot the dataframe
    fig = sns.lineplot(x="interval", y="percentage", hue='search_term', data=all_results,markers=markers)
    
    title = "Min-year: %s" % (min_year)
    fig.set_title(title)
    
    plt.legend(bbox_to_anchor=(0,-.18),loc="upper left")
    
    ymin,ymax = plt.ylim()
    if ymin > 0:
        ymin = -0.05 * ymax
    plt.ylim(ymin,ymax)
    
    xlabels = sorted(list(set(all_results.interval)))
    xlabels = map(lambda s: s.replace("_",""), xlabels)
    #fig.set_xticklabels(fig.get_xticklabels())
    fig.set_xticklabels(xlabels, rotation=90, fontsize='small')
    
    if save_figure:
        s_fig = fig.get_figure()
        s_fig.savefig("figures/func_%s_%s.png" % (label,min_year), bbox_inches='tight')

    return fig


def continuous_func(df, func, min_year=DEFAULT_MIN_YEAR, nonzero=False, label=None):
    """Search a given search term in the policy_text dataframe."""
#     identifier = (search,regex,case,min_year,label)
#     if identifier in cache:
#         return cache[identifier]
    
    if label is None:
        label = "Unlabelled"
    
    policy_counts = df[df.year>=min_year].groupby('year_season').size().to_dict()
    try:
        # policy text
        df["scores"] = df.policy_source.apply(func)
    except Exception:
        # policy html dataframe -- policy html sources
         df["scores"] = df.policy_text.apply(func)
    if nonzero:
        df2 = df[df.scores != 0]
    else:
        df2 = df
    abs_counts = df2.groupby('year_season').scores.mean().to_dict()
    #avg_policy_count = np.average(list(policy_count for interval, policy_count in policy_counts.items()))
    
    percentages = [
        (interval, abs_counts.get(interval, 0), "%s" % (label))
                for interval, policy_count in policy_counts.items()]
    # return a dataframe of ['interval', 'percentage', 'search_term']
    # where percentage is normalized frequency of policies that contain the
    # given search term
    results_df = pd.DataFrame(percentages, columns=['interval', 'average_count', 'search_term'])
    #cache[identifier] = results_df
    return results_df


def plot_continuous_func(df, func, label="", min_year=DEFAULT_MIN_YEAR, nonzero=False, save_figure=False,markers=False):
    plt.figure(figsize=(10,5))
    # append the results in a single df, so we can plot easily
    all_results = pd.DataFrame(columns=['interval', 'average_count', 'search_term'])
    # search terms one by one
    percentages = continuous_func(df, func, min_year=min_year,label=label,nonzero=nonzero)
    all_results = all_results.append(percentages)
    # plot the dataframe
    fig = sns.lineplot(x="interval", y="average_count", hue='search_term', data=all_results,markers=markers)
    
    title = "Min-year: %s" % (min_year)
    fig.set_title(title)
    
    plt.legend(bbox_to_anchor=(0,-.18),loc="upper left")
    
    ymin,ymax = plt.ylim()
    if ymin > 0:
        ymin = -0.05 * ymax
    plt.ylim(ymin,ymax)
    
    xlabels = sorted(list(set(all_results.interval)))
    xlabels = map(lambda s: s.replace("_",""), xlabels)
    #fig.set_xticklabels(fig.get_xticklabels())
    fig.set_xticklabels(xlabels, rotation=90, fontsize='small')
    
    if save_figure:
        s_fig = fig.get_figure()
        s_fig.savefig("figures/func_%s_%s.png" % (label,min_year), bbox_inches='tight')

    return fig


def set_plot_params():
    plt.rcParams['figure.figsize'] = [7, 4]
    # plt.rcParams['figure.figsize'] = [7, 1.5]
    plt.rcParams['axes.titlesize'] = "xx-large"
    sns.set(font_scale = 1)
    plt.rcParams["errorbar.capsize"] = 0.35
    sns.set_style("whitegrid")
    sns.set_style(rc={"pdf.fonttype": 1, 'pdf.use14corefonts':True, 'text.usetex':True})


def count_plot_per_interval(df, y_title="", start_year=DEFAULT_START_YEAR, save_to_file=True, filetype="pdf"):
    set_plot_params()
    df = df[df.year_season>=str(start_year)]
    fig = sns.countplot(x=INTERVAL_COL_NAME, data=df, color=DEFAULT_PLOT_COLOR)
    set_x_ticks(df)
    # fig.set_xticklabels(fig.get_xticklabels(), rotation=45, fontsize='small')
    fig.set(xlabel='Interval', ylabel=y_title)
    fig.set_title("%s per interval" % y_title)
    if save_to_file:
        save_figure(fig, "countplot_%s_%s_onwards.%s" % (y_title, start_year, filetype))
    return fig

def barplot_per_interval(df, col_name, y_title="", start_year=DEFAULT_START_YEAR, agg_method='median', save_to_file=True, filetype="pdf"):
    if not y_title:
        y_title = col_name
    set_plot_params()
    # aggregate per interval
    if start_year is not None:
        df = df[df.year_season>=str(start_year)]
    if agg_method:
        x = df.groupby(INTERVAL_COL_NAME, as_index=False)[[col_name]].agg(agg_method)
    else:
        x = df
    fig = sns.barplot(y=col_name, x=INTERVAL_COL_NAME, data=x, color=DEFAULT_PLOT_COLOR)
    set_x_ticks(df)
    
    #fig.set_xticklabels(fig.get_xticklabels(), rotation=45, fontsize='small')
    fig.set(xlabel='Interval', ylabel=y_title)
    fig.set_title(agg_method.capitalize() + " " + y_title + " per interval")
    if save_to_file:
        save_figure(fig, "barplot_%s_%s_%s_onwards.%s" % (agg_method, col_name, start_year, filetype))
    return fig

def set_legend(ax, legend_title, legend_loc="upper left"):
    handles, labels = ax.get_legend_handles_labels()
    handles, labels = list(zip(*list(zip(handles,labels))[1:]))
    if legend_loc == "outside":
        ax.legend(handles=handles, labels=labels, bbox_to_anchor=(1.05, 1),
        loc=2, borderaxespad=0., title=legend_title)
    elif legend_loc:
        ax.legend(handles=handles, labels=labels, title=legend_title, loc=legend_loc)
    else:
        ax.legend(handles=handles, labels=labels, title=legend_title)

dash_styles = [
    (1, 10), (1, 1), (1, 5), (5, 10), (5, 5), (5, 1), (3, 10, 1, 10),
    (3, 5, 1, 5), (3, 1, 1, 1), (3, 5, 1, 5, 1, 5), (3, 10, 1, 10, 1, 10), (3, 1, 1, 1, 1, 1)]

PRINT_DFS = True


def lineplot_per_interval(df, y, hue, y_title="",
        start_year=DEFAULT_MIN_YEAR, save_to_file=True, filetype="pdf",
        legend_loc="upper right", hue_order=None,
        legend_title="Snapshot rank", estimator=np.median):
    fig, ax = plt.subplots()
    if not y_title:
        if hue:
            y_title = hue.title()
        else:
            y_title = ""

    # filter by start year    
    df = df[df.year_season>=str(start_year)]

    set_plot_params()

    if hue:
        try:
            fig = sns.lineplot(data=df, y=y, x=INTERVAL_COL_NAME, hue=hue, style=hue, hue_order=hue_order, estimator=estimator,ci=95)
        except:
            markers = list(range(1, len(hue_order)+1))
            print(markers)
            fig = sns.lineplot(data=df, y=y, x=INTERVAL_COL_NAME, style=hue, hue=hue, dashes=False, markers=markers, hue_order=hue_order)
        set_legend(ax, legend_title, legend_loc)
        plt.setp(fig.get_legend().get_texts(), fontsize='10') # for legend text
        plt.setp(fig.get_legend().get_title(), fontsize='10') # for legend text

    else:
        fig = sns.lineplot(data=df, y=y, x=INTERVAL_COL_NAME, estimator=estimator)

            
    set_x_ticks(df)
    
    #fig.set(xlabel='Interval', ylabel=y_title)
    ax.set_xlabel('Interval')
    ax.set_ylabel(y_title, fontsize = 10)

    

    # fig.set_title(y_title + " per interval")
    fig_filename = "lineplot_%s.%s" % ("_".join(
        y_title.replace("%", "pct").replace("(", "").replace(")", "").lower().split()), filetype)
    if save_to_file:
        save_figure(fig, fig_filename)
    return fig

## Email and link analysis functions
RANK_BINS = [1, 1000, 10000, 100000, 1000000]
RANK_BIN_LABELS_1M = ['(1, 1K]', '(1K, 10K]', '(10K, 100K]', '(100K, 1M]']
PLUS_1M_LABEL = '> 1M'
RANK_BIN_LABELS = RANK_BIN_LABELS_1M + [PLUS_1M_LABEL]
    
def add_rank_bins_to_df(df):
    """Add rank bin labels to the df."""
    # check if the df is already prepped
    df['binned_rank'] = pd.cut(df['alexa_rank'], RANK_BINS, labels=RANK_BIN_LABELS_1M)
    df['binned_rank'] = df['binned_rank'].cat.add_categories(PLUS_1M_LABEL)
    # label snapshots without ranks as 1M+
    df.fillna(value={'binned_rank': PLUS_1M_LABEL}, inplace=True)

COUNT_COL_NAME = "count"
PERCENTAGE_COL_NAME = "percentage"
BINNED_RANK_COL_NAME = "binned_rank"


def get_group_pcts(df, factor_column, group_by=[BINNED_RANK_COL_NAME, INTERVAL_COL_NAME]):
    """factor_column should be a binary"""
    df_g = df.groupby(group_by)[factor_column].value_counts(normalize=True)
    df_g = df_g.mul(100).rename(PERCENTAGE_COL_NAME).reset_index()
    # only take percentage where factor_column is True
    return df_g[df_g[factor_column]==True]

ALEXA_RANK_START_YEAR = 2009
LAST_INTERVAL = "2019_B"

def lineplot_percentages_by_factor_and_binned_rank(
    df, factor_column,
        min_year=DEFAULT_MIN_YEAR, y_title="", legend_loc='upper left', by_rank=True, print_df=False):
    if by_rank and min_year < ALEXA_RANK_START_YEAR:
        print("Alexa rank data before 2009 is not available")
        return
    if by_rank:
        df_g = get_group_pcts(df[df.year>=min_year], factor_column)
    else:
        df_g = get_group_pcts(df[df.year>=min_year], factor_column, group_by=INTERVAL_COL_NAME)
    set_plot_params()
    if print_df:
        print(df_g)
    hue = "binned_rank" if by_rank else False
    lineplot_per_interval(df_g, y=PERCENTAGE_COL_NAME, hue=hue, y_title=y_title,
        legend_loc=legend_loc, hue_order=RANK_BIN_LABELS, start_year=min_year)
    

def lineplot_top_x_in_time(df, factor_column, n_policies_per_interval, n_top=5,
        start_year=2009, y_title="", use_last_interval=True, legend_loc="upper left", legend_title="", top_x=None):
    df = df[df.year_season>str(start_year)]
    df = df.drop_duplicates(('year_season', 'home_domain', factor_column))
    if top_x is None:
        if use_last_interval:
            top_x = df[df.year_season==LAST_INTERVAL][factor_column]\
                .value_counts().head(n_top).index
        else:
            # one vote for each site, value pair
            top_x = df.drop_duplicates(['home_domain', factor_column])[factor_column]\
                .value_counts().head(n_top).index

    counts_df = df[df[factor_column].isin(top_x)].\
        groupby([INTERVAL_COL_NAME, factor_column]).size().\
            unstack(fill_value=0).stack().to_frame(COUNT_COL_NAME).reset_index()

    counts_df[PERCENTAGE_COL_NAME] = counts_df.apply(
            lambda row: 100*row[COUNT_COL_NAME]/n_policies_per_interval[
                row[INTERVAL_COL_NAME]], axis=1)
    # print(counts_df)
    lineplot_per_interval(
        df=counts_df, y=PERCENTAGE_COL_NAME,
        hue=factor_column, y_title=y_title, start_year=start_year, hue_order=top_x,
            legend_loc=legend_loc, legend_title=legend_title)