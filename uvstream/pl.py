from importlib.util import find_spec
from typing import Any
from datetime import timedelta

if not find_spec('polars'):
    raise ModuleNotFoundError(name='polars')

import polars as pl
from polars._typing import IntoExprColumn, IntoExpr
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


class PLMapBy(PLStream):

    def __init__(self, upstream:Stream=None, by:IntoExpr|Iterable[IntoExpr]=None, fn:Optional[Callable[[pl.DataFrame], pl.DataFrame]]=None, name:str=None, *args, **kwargs):
        super().__init__(upstream, fn, name, *args, **kwargs)
        self.by = by

    async def update(self, x:pl.DataFrame, who = None):
        return await self(x.group_by(self.by).map_groups(self.fn))


class PLAggBy(PLStream):

    def __init__(self, upstream:Stream=None, by:IntoExpr|Iterable[IntoExpr]=None, agg:IntoExpr|Iterable[IntoExpr]=None, name:str=None, *args, **kwargs):
        super().__init__(upstream, name, *args, **kwargs)
        self.by = by
        self.agg = agg

    async def update(self, x:pl.DataFrame, who = None):
        if isinstance(self.agg, dict): 
            return await self(x.group_by(self.by).agg(**self.agg))
        return await self(x.group_by(self.by).agg(self.agg))


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


@register_stream(PLStream)
class PLConcat(PLStream):
    def __init__(self, upstream=None, how='diagonal_relaxed', *args, **kwargs):
        super().__init__(upstream, *args, **kwargs)
        self.how=how
 

    async def update(self, x:Iterable[pl.DataFrame], who:PLStream):
        await self(pl.concat(x, how=self.how))


def pl_concat(*args:PLStream, how='diagonal_relaxed', **kwargs):
    return PLConcat(Zip(args, **kwargs), how=how)


@register_stream(PLStream)
class PLJoin(PLStream):
    def __init__(self, upstream=None, on=None, how='full', *args, **kwargs):
        super().__init__(upstream, *args, **kwargs)
        self.on = on
        self.how = how


    async def update(self, x:Iterable[pl.DataFrame], who:PLStream):
        x0:pl.DataFrame = x[0]
        for xi in x[1:]:
            x0 = x0.join(xi, on=self.on, how=self.how)
            for col in x0.columns:
                if col.endswith('_right'):
                    x0 = x0.drop(col)
        await self(x0)


def pl_join(*args:PLStream, on=None, how='full', **kwargs):
    return PLJoin(Zip(args, **kwargs), on=on, how=how)