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

    def __init__(self, upstream:Stream=None, fn:Optional[Callable[[pl.DataFrame], pl.DataFrame]]=None, name:str=None, *args, **kwargs):
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
        if x is None:
            if self._buffer is not None:
                self._buffer = self._buffer.filter(*predicates, **constraints)
            return

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
    def __init__(self, 
                 upstream=None, 
                 t:timedelta|float=float('inf'), 
                 time_column:IntoExprColumn=pl.col('ts'), 
                 realtime:bool=False,
                 *args, 
                 **kwargs):
        super().__init__(upstream, *args, **kwargs)

        if isinstance(t, timedelta):
            t = t.total_seconds()
        self.t = t
        if isinstance(time_column, str):
            time_column = pl.col(time_column)
        self.time_column = time_column
        self.realtime = realtime


    async def update(self, x:pl.DataFrame, who:PLStream):
        if self.realtime:
            self._update_buffer(x, ((pl.lit(datetime.now()) - self.time_column).dt.total_seconds()) < self.t)
        else:
            self._update_buffer(x, ((self.time_column.max() - self.time_column).dt.total_seconds()) < self.t)
        await self(self._buffer)