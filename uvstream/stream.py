from datetime import timedelta
import asyncio 
import inspect
from typing import Callable, Iterable, Literal, Coroutine
from collections import OrderedDict
import uuid
from functools import partial
import uvloop as uv


class Event[Inbound, Outbound]:
    def __init__(self):
        self.delegates:set[Callable[[Inbound], Outbound]] = set()
        self.awaiting:set[uuid.UUID] = set()
        self.awaited_args:dict[uuid.UUID, Inbound] = dict()

    def emit(self, *args:Inbound) -> Iterable[Outbound]:
        for id in self.awaiting:
            self.awaited_args[id] = args
        self.awaiting.clear()
        return [delegate(*args) for delegate in self.delegates]

    def __call__(self, *args:Inbound) -> Iterable[Outbound]:
        return self.emit(*args)
    
    def __add__(self, other:Callable[[Inbound], Outbound]):
        self.delegates.add(other)

    def __sub__(self, other:Callable[[Inbound], Outbound]):
        if other in self.delegates:
            self.delegates.remove(other)

    @property
    async def invoked(self) -> Coroutine:
        id = uuid.uuid4()
        self.awaiting.add(id)
        while id in self.awaiting:
            await asyncio.sleep(0)
        return self.awaited_args.pop(id)

    


class Stream[Inbound, Outbound]:

    def __init__(self, 
                 upstream:'Stream|Iterable[Stream]|None'=None, 
                 fn:Callable[[Inbound], Outbound]|None=None,
                 *args,
                 **kwargs):
        
        self.id = str(uuid.uuid4())
        self.upstream:set['Stream'] = set()
        self.downstream:set['Stream'] = set()
        self.on_done:Event[None, None] = Event()

        if upstream is not None:
            if isinstance(upstream, Iterable):
                _ = [self.add_upstream(u) for u in upstream]
            else:
                self.add_upstream(upstream)

        self.fn = fn
        self.args = args
        self.kwargs = kwargs


        self._vis_node_props = {
            'shape':'ellipse',
            'color':'gray',
            'size':10,
            'border_size':2,
            'border_color':'black',
        }
        self._vis_edge_props = dict()


    def add_upstream(self, other:'Stream'):
        self.upstream.add(other)
        other.downstream.add(self)


    def remove_upstream(self, other:'Stream'):
        if other in self.upstream:
            self.upstream.remove(other)
        if self in other.downstream:
            other.downstream.remove(self)

        
    def add_downstream(self, other:'Stream'):
        self.downstream.add(other)
        other.upstream.add(self)


    def remove_downstream(self, other:'Stream'):
        if other in self.downstream:
            self.downstream.remove(other)
        if self in other.upstream:
            other.upstream.remove(self)


    def clear_connections(self):
        for other in self.downstream:
            other.upstream.remove(self)
        for other in self.upstream:
            other.downstream.remove(self)
        self.upstream = set()
        self.downstream = set()


    async def __call__(self, x:Outbound):
        await asyncio.gather(*[i.update(x, who=self) for i in self.downstream])
        self.on_done()



    async def update(self, x:Inbound, who:'Stream'=None):
        if self.fn is None:
            await self(x)
            return
                
        if inspect.iscoroutinefunction(self.fn):
            await self(await self.fn(x, *self.args, **self.kwargs))
            return
        
        await self(self.fn(x, *self.args, **self.kwargs))


    def alias(self, name):
        setattr(self, 'name', name)


    def __str__(self) -> str:
        if hasattr(self, 'name'):
            s = getattr(self, 'name')
        else:
            s = f"{self.__class__.__name__}"
        if self.fn is not None:
            fn_args = []
            fn_kwargs = {}
            fn_name = ''
            if isinstance(self.fn, partial):
                fn_args = self.fn.args
                fn_kwargs = self.fn.keywords
                fn_name = self.fn.func.__name__
            elif isinstance(self.fn, Callable):
                fn_args = self.args
                fn_kwargs = self.kwargs
                fn_name = self.fn.__name__

            all_args = [
                *fn_args, 
                *[f"{k}={v}" for k, v in fn_kwargs.items()]
            ]

            arg_str = ''
            if len(all_args) > 0:
                arg_str = f'({", ".join(all_args)})'

            s = f"{s}: {fn_name}{arg_str}"
        return s 
    

    def _edge_tuples(self) -> list[tuple[int, int, dict]]:
        return [(stream.id, self.id, self._vis_edge_props) for stream in self.upstream]
    


class Source[T](Stream[None, T]):
    def __init__(self):
        super().__init__(None, None)
        self.event_loop = None
        self._vis_node_props['color'] = 'white'
        self._vis_node_props['size'] = 15


    def emit(self, x:T):
        uv.run(self(x))



class Sink[T](Stream[T, None]):
    def __init__(self, 
                 upstream:Stream|None=None, 
                 fn:Callable[[T], None]|None=None,
                 *args,
                 **kwargs):
        
        super().__init__(upstream, fn, *args, **kwargs)
        self._vis_node_props['size'] = 15
        self._vis_node_props['color'] = 'black'
            

    async def update(self, x:T, who:'Stream'=None):
        if self.fn is None:
            return
        
        if inspect.iscoroutinefunction(self.fn):
            await self.fn(x, *self.args, **self.kwargs)
            return
        
        self.fn(x, *self.args, **self.kwargs)



class Map[Inbound, Outbound](Stream[Inbound, Outbound]):
    pass



class Zip[T](Stream[T, tuple[T]]):

    def __init__(self, 
                upstream:Stream|Iterable[Stream], 
                require:Stream|Iterable[Stream]|None=None, 
                wait_for_all:bool=True,
                purge_on_partial:bool=False,
                window:timedelta=timedelta(seconds=0),
                *args,
                **kwargs):
        
        self._buffer:OrderedDict[Stream, T|None] = OrderedDict()
        self._track:dict[Stream, bool] = dict()

        if isinstance(require, Stream):
            require = set([require])

        self.require:set[Stream] = set(require) if require else set()
        self.wait_for_all:bool = wait_for_all
        self.purge_on_partial:bool = purge_on_partial
        self.window:timedelta = window

        super().__init__(upstream, None, *args, **kwargs)
        self._vis_node_props['shape'] = 'rectangle'


    def add_upstream(self, other:'Stream'):
        super().add_upstream(other)
        if other in self._buffer:
            return
        self._buffer[other] = None
        self._track[other] = False


    def remove_upstream(self, other:'Stream'):
        super().remove_upstream(other)
        if other not in self._buffer:
            return
        self._buffer.pop(other)
        self._track.pop(other)


    def _reset_buffer(self):
        self._track.update((k, False) for k in self._track.keys())
        self._buffer.update((k, None) for k in self._buffer.keys())


    async def update(self, x:T, who:Stream=None):
        self._buffer[who] = x
        self._track[who] = True

        if not self.wait_for_all:
            await asyncio.sleep(self.window.total_seconds())
            if not any(self._track.values()):
                return
            if not all(self._track[req] for req in self.require):
                return
            vals = tuple(self._buffer.values())
            if all(self._track.values()) or self.purge_on_partial:
                self._reset_buffer()                
            await super().update(vals, who=who)        

        elif all(self._track.values()):
            vals = tuple(self._buffer.values())
            self._reset_buffer()
            await super().update(vals, who=who)        



class Filter[T](Stream):

    def __init__(self, 
                 upstream:Stream|None=None, 
                 fn:Callable[[T], bool]|None=None, 
                 *args, 
                 **kwargs):
        
        super().__init__(upstream, fn, *args, **kwargs)


    async def update(self, x:T, who:Stream=None):
        if self.fn is None:
            return
        
        p = False
        if inspect.iscoroutinefunction(self.fn):
            p = await self.fn(x, *self.args, **self.kwargs)
        else:        
            p = self.fn(x, *self.args, **self.kwargs)
        if not p:
            return
        
        await self(x)



class Window[T](Stream[T, Iterable[T]]):

    def __init__(
            self, 
            upstream:Stream|None=None, 
            window_size:int=1, 
            emit_partial=True, 
            *args, 
            **kwargs):
        
        super().__init__(upstream, *args, **kwargs)
        self.window_size = window_size
        self.emit_partial = emit_partial
        self._buffer = []


    async def __call__(self, x:Iterable[T]):
        return await super().__call__(x)


    async def update(self, x:T, who:Stream=None):
        if len(self._buffer) > self.window_size:
            self._buffer.pop(0)
        
        self._buffer.append(x)

        if self.emit_partial or len(self._buffer) == self.window_size:
            await self(self._buffer)

        

class Unpack[T](Stream[Iterable[T], T]):

    def __init__(
            self, 
            upstream:Stream=None, 
            fn:Callable[[T], T]=None, 
            *args, 
            **kwargs):
        
        super().__init__(upstream, fn, *args, **kwargs)

    
    async def __call__(self, x:T):
        return await super().__call__(x)
    

    async def update(self, x:Iterable[T], who:Stream=None):
        if not isinstance(x, Iterable):
            await super().update(x)
            return 
        
        for item in x:
            await super().update(item, who)


class WaitTillDone[T](Stream[T, T]):
    def __init__(
            self, 
            upstream:Stream=None, 
            require:Iterable[Stream]|Stream=None,
            *args, 
            **kwargs):
        
        if isinstance(require, Stream):
            require = set([require])
        self.require = set(require) if require else set()
        super().__init__(upstream, None, *args, **kwargs)


    async def update(self, x:T, who:Stream=None):
        await asyncio.gather(*[req.on_done.invoked for req in self.require])
        await super().update(x, who)


class Pipeline[Inbound]:

    def __init__(self, source:Source[Inbound], *streams:Stream):
        self.source:Source[Inbound] = source
        self.streams:set[Stream] = set([source, *self._traverse(source, 'down'), *streams])


    def __call__(self, x:Inbound):
        self.source.emit(x)


    def _traverse(self, start:Stream|Iterable[Stream], direction:Literal['up', 'down', 'bi']):
        if isinstance(start, Stream):
            start = [start]
        front = set(start).copy()
        done = set()
        order = []
        while len(front) > 0:
            cur = front.pop()
            if cur is None:
                continue
            done.add(cur)
            order.append(cur)

            next_set = dict(
                up=cur.upstream,
                down=cur.downstream,
                bi=set().union(cur.upstream, cur.downstream)
            )[direction]

            for item in next_set:
                if item in done:
                    continue
                front.add(item)
        return order
    

    def to_nx(self, **kwargs):
        try:
            import networkx as nx
        except ModuleNotFoundError as e:
            raise e
        
        G = nx.DiGraph(**kwargs)
        nodes = []
        edges = []
        for stream in self.streams:
            nodes.append((stream.id, dict(id=stream.id, name=str(stream), **stream._vis_node_props)))
            edges.extend(stream._edge_tuples())
        
        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        return G

    
    def visualize_gv(self, **kwargs):
        try:
            import gravis as gv
        except ModuleNotFoundError as e:
            raise e
        
        return gv.d3(self.to_nx(), node_label_data_source='name', **kwargs)


    def __str__(self):
        return '\n'.join([f"{idx}: {str(stream)}" for idx, stream in enumerate(self._traverse(self.source, 'down'))])



if __name__ == '__main__':

    def pass_print(x):
        print(x)
        return x

    async def wait(x, time:float):
        await asyncio.sleep(time)
        return x


    source = Source()
    node = Filter[float](Stream(source, wait, time=0.25), lambda x: x % 2)
    delay1 = Stream(node, wait, time=0.25)
    delay2 = Stream(node, wait, time=1.0)
    Sink(delay1, print)
    Sink(delay2, print)
    w = WaitTillDone(node, [delay1, delay2])
    sink = Sink(w, lambda x: print(f"{x} done!"))

    pipeline = Pipeline(source) 
    pipeline.visualize_gv(
        edge_curvature=0.1, 
        use_many_body_force=True,
        many_body_force_strength=-200,
        links_force_distance=125,
    ).export_html('_.html', overwrite=True)
    _ = [source.emit(v) for v in [1, 2, 3, 4, 5]] 
    from pl import *
    PLStream()