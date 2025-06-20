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

    def pl_filter(self, *predicates:PLPredicate, name=None, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.filter(*predicates, **constraints), name=name)
    
    def pl_map(self, fn:Callable[[pl.DataFrame], pl.DataFrame]) -> 'PLStream':
        return PLStream(self, fn)

    def pl_select(self, *predicates:PLPredicate, name=None, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.select(*predicates, **constraints), name=name)
    
    def pl_with_columns(self, *predicates:PLPredicate, name=None, **constraints:Any) -> 'PLStream':
        return PLStream(self, lambda x: x.with_columns(*predicates, **constraints), name=name)
    
    def pl_insert_column(self, index:int, column:IntoExprColumn, name=None) -> 'PLStream':
        return PLStream(self, lambda x: x.insert_column(index, column), name=name)


class PLFuncWindow(PLStream):
    def __init__(self, upstream=None, fn=None, name=None, min_samples:int=0, unique_col:Optional[IntoExprColumn]=None, *args, **kwargs):
        super().__init__(upstream, fn, name, *args, **kwargs)
        self._buffer:pl.DataFrame = None
        self.min_samples = min_samples
        self.unique_col = unique_col
        self.on_append = Event[Any, None]()

    def _update_buffer(self, x:pl.DataFrame, *predicates:PLPredicate, **constraints:Any):
        if self._buffer is not None:
            if self.unique_col is not None:
                x = x.filter(self.unique_col.is_in(self._buffer.select(self.unique_col).to_series()).not_())
            self._buffer = self._buffer.vstack(x)
        else:
            self._buffer = x.clone()

        self.on_append(self._buffer, x)
        if len(self._buffer) > self.min_samples:
            self._buffer = self._buffer.filter(*predicates, **constraints)


@register_stream(PLStream)
class PLTimeWindow(PLFuncWindow):

    def __init__(self, upstream=None, t:timedelta|float=0.0, time_column:IntoExprColumn=pl.col('ts'), *args, **kwargs):
        super().__init__(upstream, *args, **kwargs)
        if isinstance(t, timedelta):
            t = t.total_seconds()
        self.t = t
        self.time_column = time_column


    async def update(self, x:pl.DataFrame, who:PLStream):
        self._update_buffer(x, ((pl.lit(datetime.now()) - self.time_column).dt.total_seconds()) < self.t)
        await self(self._buffer)
