from __future__ import annotations

import pandas as pd
import random
import time

from pandas import DataFrame as Df
from typing import Any

class Subscriber:
    """
    Corresponds to an individual signed up to the carousel.
    """
    def __init__(self, sub_data: pd.Series, hist_df: Df) -> None:
        self._data = sub_data
        self._hist = self._configure_history(hist_df)
        self._days = self._data["days"].split(',')
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
            days_match = self._get_pair_day_coincidence(sub)
            if days_match == 0 and self._data["exclusive"] == 'TRUE':
                continue
            match_count = self._get_pair_match_count(sub)
            past_matches.append((match_count, days_match, sub))
        if len(past_matches) == 0:
            return
        
        # Sort by days match (secondary) then by match count (primary)
        past_matches.sort(key=lambda x: x[1], reverse=True)
        past_matches.sort(key=lambda x: x[0])

        self.partner = past_matches[0][2]
        self.partner.partner = self
    
    def _get_pair_match_count(self, sub: Subscriber) -> int:
        pair = self._get_pair(sub)
        if pair is None:
            return 0
        return len(pair)
    
    def _get_pair_day_coincidence(self, sub: Subscriber) -> int:
        return len([x for x in self._days if x in sub._days])

    def _get_pair(self, sub: Subscriber) -> Df:
        pair = self._hist.loc[self._hist["email2"] == sub._data["email"]]
        if len(pair) == 0:
            return None
        return pair

    def create_row(self, sub: Subscriber) -> Df:
        return Df([[
            self._data["email"],
            sub._data["email"],
            time.ctime(),
            str(int(time.time()))]], 
            columns=self._hist.columns)

    def _is_involved(self, week_no: int) -> bool:
        return week_no % int(self._data["interval"]) == 0
    
    def _total_matches(self) -> int:
        return len(self._hist)
    
    def _select_random_item(self, items: list) -> Any:
        return items[random.randint(0, len(items)-1)]
    
    def _configure_history(self, hist_df: Df) -> None:
        return hist_df.loc[hist_df["email1"] == self._data["email"]]