"""Candidate pair generation and union-find clustering utilities."""

from __future__ import annotations
import itertools
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple
import pandas as pd

from blocking import block_keys
from similarity import pair_score


def build_pairs(df: pd.DataFrame, threshold: float = 0.80) -> List[Tuple[int, int]]:
    blocks: Dict[str, List[int]] = defaultdict(list)
    for idx, row in df.iterrows():
        for k in block_keys(row):
            blocks[k].append(idx)

    seen = set()
    pairs: List[Tuple[int, int]] = []
    for _, idxs in blocks.items():
        if len(idxs) < 2:
            continue
        for a, b in itertools.combinations(sorted(idxs), 2):
            if (a, b) in seen:
                continue
            seen.add((a, b))
            s = pair_score(df.loc[a], df.loc[b])
            if s >= threshold:
                pairs.append((a, b))
    return pairs


class DSU:
    def __init__(self):
        self.p: Dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self.p:
            self.p[x] = x
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def connected_components(pairs: Sequence[Tuple[int, int]]) -> List[List[int]]:
    dsu = DSU()
    nodes = set()
    for a, b in pairs:
        nodes.add(a)
        nodes.add(b)
        dsu.union(a, b)
    groups = defaultdict(list)
    for n in nodes:
        groups[dsu.find(n)].append(n)
    return list(groups.values())

