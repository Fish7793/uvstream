from importlib.util import find_spec
from typing import Any
from datetime import timedelta

if not find_spec('polars'):
    raise ModuleNotFoundError(name='polars')

import polars as pl
from polars._typing import IntoExprColumn
from uvstream.stream import *

PLPredicate = IntoExprColumn | Iterable[IntoExprColumn] | bool | list[bool] | Any

class PLStream(Stream[pl.DataFrame, pl.DataFrame]):

    def __init__(self, upstream = None, fn = None, name = None, *args, **kwargs):
        super().__init__(upstream, fn, name, *args, **kwargs)

    def pl_filter(self, *predicates:PLPredicate, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.filter(*predicates, **constraints))
    
    def pl_select(self, *predicates:PLPredicate, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.select(*predicates, **constraints))
    
    def pl_with_columns(self, *predicates:PLPredicate, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.with_columns(*predicates, **constraints))
    
    def pl_insert_column(self, index:int, column:IntoExprColumn) -> 'PLStream':
        return PLStream(self, lambda x: x.insert_column(index, column))


class PLFuncWindow(PLStream):
    def __init__(self, upstream=None, fn=None, name=None, *args, **kwargs):
        super().__init__(upstream, fn, name, *args, **kwargs)
        self._buffer:pl.DataFrame = None

    def _update_buffer(self, x:pl.DataFrame, *predicates:PLPredicate):
        if self._buffer:
            self._buffer = self._buffer.extend(x)
        else:
            self._buffer = x.clone()
        self._buffer = self._buffer.filter(predicates)



class PLTimeWindow(PLFuncWindow):

    def __init__(self, upstream=None, t:timedelta|float=0.0, time_column:IntoExprColumn=pl.col('ts'), *args, **kwargs):
        super().__init__(upstream, None, *args, **kwargs)
        if isinstance(t, timedelta):
            t = t.total_seconds()
        self.t = t
        self.time_column = time_column


    async def update(self, x:pl.DataFrame, who:PLStream):
        self._update_buffer(x, (pl.lit(datetime.now()) - self.time_column).dt.total_seconds() < self.t)
        self(self._buffer)
