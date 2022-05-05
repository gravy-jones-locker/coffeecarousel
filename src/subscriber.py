from __future__ import annotations

import pandas as pd
import random

from pandas import DataFrame as Df
from typing import Any

class Subscriber:
    """
    Corresponds to an individual signed up to the carousel.
    """
    def __init__(self, sub_data: pd.Series, hist_df: Df) -> None:
        self._data = sub_data
        self._hist = self._configure_history(hist_df)
        self.partner = None
    
    def __repr__(self) -> str:
        return self._data["email"]
    
    def __str__(self) -> str:
        return self._data["email"]

    def match(self, subs: list) -> Subscriber:
        """
        Pick the best match out of the list of other subs.
        """
        past_matches = []
        for sub in subs:
            past_matches.append(self._get_pair_match_count(sub))
        min_matches = min(past_matches)
        candidates = []
        for i, match_count in enumerate(past_matches):
            if match_count != min_matches:
                continue  # True if have already matched more than the minimum
            candidates.append(subs[i])
        self.partner = self._select_random_item(candidates)
        self.partner.partner = self
    
    def _get_pair_match_count(self, sub: Subscriber) -> int:
        pair = self._get_pair(sub)
        if pair is None:
            return 0
        return int(pair["count"])

    def _get_pair(self, sub: Subscriber) -> Df:
        pair = self._hist.loc[self._hist["email2"] == sub._data["email"]]
        if len(pair) == 0:
            return None
        return pair

    def _create_pair(self, sub: Subscriber) -> Df:
        return Df([[self._data["email"], sub._data["email"], '1']], 
                    columns=self._hist.columns)

    def _is_involved(self, week_no: int) -> bool:
        return week_no % int(self._data["interval"]) == 0
    
    def _total_matches(self) -> int:
        if len(self._hist) == 0:
            return 0
        return sum([int(x) for x in self._hist["count"].values])
    
    def _select_random_item(self, items: list) -> Any:
        return items[random.randint(0, len(items)-1)]
    
    def _configure_history(self, hist_df: Df) -> None:
        return hist_df.loc[hist_df["email1"] == self._data["email"]]