import historical.util as util
import argparse
from collections import defaultdict
import historical.find_near_documents as fnd
import simhash

def get_num_policy_families():
    for ys in util.iter_yearseason():
        util.__init_domains_cache(ys)
        yield len(util.domainsets_cache[ys])

def get_total_policies_in_families():
    for ys in util.iter_yearseason():
        util.__init_domains_cache(ys)
        yield sum((len(s) for s in util.domainsets_cache[ys].values()))

def get_largest_families():
    for ys in util.iter_yearseason():
        util.__init_domains_cache(ys)
        yield max((len(s) for s in util.domainsets_cache[ys].values()))
        
def get_average_policy_family_size():
    for polCt,famCt in zip(get_total_policies_in_families(),get_num_policy_families()):
        yield polCt / famCt


def get_num_policies():
    util.__init_db()
    for y,s in util.iter_year_season():
        yield util.policies_db.execute("SELECT Count() FROM policy_texts WHERE year==? AND season==?;", (y,s)).fetchone()[0]

def find_changed_policies():
    policies_cache = defaultdict(lambda: defaultdict(dict))
    for h,d,y,s in util.get_pool().map(
            fnd.hash_text, ((data["policy_text"],data["site_url"],data["year"],data["season"]) for data, cols in util.load_all_policies())):
        policies_cache[d][y][s] = h
    

    changed_pols = defaultdict(lambda: defaultdict(lambda: 0))
    all_pols = defaultdict(lambda: defaultdict(lambda: 0))

    for dom in policies_cache:
        print(policies_cache[dom])
        prev_pol = None
        for y,s in util.iter_year_season():
            try:
                print(y,s)
                print(policies_cache[dom][y])
                pol = policies_cache[dom][y][s]
            except KeyError:
                continue
            if prev_pol is not None:
                if len(simhash.find_all([prev_pol,pol],4,3)) != 0:
                    changed_pols[y][s] += 1
                all_pols[y][s] += 1
    return ([changed_pols[y][s] for y,s in util.iter_year_season()],
            [all_pols[y][s] for y,s in util.iter_year_season()])
                
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find and flag duplicate documents')
    util.add_arguments(parser)
    args = parser.parse_args()
    util.process_arguments(args)

    cgd,al = find_changed_policies()
    print(cgd)
    print(al)
    print("Policy change percentages:")
    print([c/a for c,a in zip(cgd,al)])
    print()
    
    print("Number of policies:")
    print(get_num_policies())
    print()

    print("Number of policy families:")
    print(get_num_policy_families())
    print()

    print("Largest families:")
    print(get_largest_families())
    print()

    print("Average family size:")
    print(get_average_policy_family_size())
    print()

    
