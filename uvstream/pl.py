from importlib.util import find_spec

if not find_spec('polars'):
    raise ModuleNotFoundError(name='polars')

import polars as pl

from stream import *

PLStream = Stream[pl.DataFrame, pl.DataFrame]
PLSource = Source[pl.DataFrame]
PLSink = Sink[pl.DataFrame]
PLMap = Map[pl.DataFrame, pl.DataFrame]
PLZip = Zip[pl.DataFrame]
PLFilter = Filter[pl.DataFrame]
PLWindow = Window[pl.DataFrame]
PLUnpack = Unpack[pl.DataFrame]