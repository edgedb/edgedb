##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Quine-McCluskey boolean function minimization algorithm"""


import itertools


def minimize(ones, dontcare=None):
    numargs = max(map(len, ones))
    # Pad the cubes to the necessary length
    ones = set(tuple(itertools.chain(itertools.repeat(0, numargs - len(i)), i)) for i in ones)
    if dontcare:
        dontcare = set(tuple(itertools.chain(itertools.repeat(0, numargs - len(i)), i)) for i in dontcare)
    else:
        dontcare = set()

    prime_implicants = _quine_prime_implicants(ones | dontcare, numargs)

    essential, rest, coverage = _get_coverage(prime_implicants, ones)

    gaps = ones - set().union(*(coverage[e] for e in essential))

    if gaps:
        essential |= _minimize_gaps({t: p for t, p in rest.items() if t in gaps})

    assert ones == set().union(*(coverage[e] for e in essential))

    return essential

def _quine_prime_implicants(cubes, numargs):
    def diff(c1, c2):
        diffs = False
        result = []
        for i, j in zip(c1, c2):
            if i is not j:
                if j is None or j is None or diffs:
                    return None
                diffs = True
                result.append(None)
            else:
                result.append(i)

        if not diffs:
            result = None
        return result

    primes = set()
    groups = [list(g[1]) for g in itertools.groupby(sorted(cubes, key=sum), key=sum)]

    while groups:
        newgroups = []
        merged = set()

        # Go through adjacent groups and try to merge each cube
        for group1, group2 in zip(groups[:-1], groups[1:]):
            newgroups.append(set())
            for c1, c2 in itertools.product(group1, group2):
                d = diff(c1, c2)
                if d:
                    newgroups[-1].add(tuple(d))
                    merged |= {c1, c2}
        primes |= set(itertools.chain.from_iterable(groups)) - merged
        groups = newgroups

    return primes

def _get_coverage(primes, miniterms):
    essential = {}
    rest = {}
    coverage = {}

    for prime, term in itertools.product(primes, miniterms):
        diff = sum(itertools.starmap(lambda i, j: not (i is j or i is None or j is None), zip(prime, term)))
        if not diff:
            if term not in essential and term not in rest:
                essential[term] = prime
            else:
                if term not in rest:
                    rest[term] = {essential[term], prime}
                    del essential[term]
                else:
                    rest[term].add(prime)

            if prime not in coverage:
                coverage[prime] = {term}
            else:
                coverage[prime].add(term)

    return set(essential.values()), rest, coverage

def _minimize_gaps(implicant_matrix):
    product = list(implicant_matrix.values())

    sum_ = {frozenset((i,)) for i in product[0]}

    # Produce a sum of products from a product of sums using distributivity law
    for prod in product[1:]:
        newsum = set()
        for i, j in itertools.product(sum_, [frozenset((i,)) for i in prod]):
            if i == j:
                newsum.add(i)
            newsum.add(i | j)
        sum_ = _minimize_sum(newsum)

    l = [(sum(i is not None for i in item), item) for item in sum_]
    return set(min(l, key=lambda i: i[0])[1])

def _minimize_sum(sum_):
    # Attempt to minimize the sum by absorption (X + X ^ Y = X)
    ssum = sorted(sum_, key=len)

    redundant = set()

    for i, prefix in enumerate(ssum):
        if prefix in redundant:
            continue

        for item in ssum[i + 1:]:
            if not (prefix - item):
                redundant.add(item)

    return sum_ - redundant
