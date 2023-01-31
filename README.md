### ts

Currently implemented:
* Bar-aggregation (TIME, VOLUME)
* BarFeatures

#### Example:
```python
filepath = "/Users/jacob/projects/data/continuous_futures/CME-HO.parquet"
tick_supplier = TickSupplier(instrument="CME-HO")
tick_supplier.from_parquet(filepath)

pprint(tick_supplier.instruments)
```
#### Output:
```python
['CME-HO']
```
---
#### Example:
```python
pprint(
    BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=1_000
        )
    ).bars
)
```
#### Output:
```python
['bar-CME-HO-volume-1000-OPEN',
 'bar-CME-HO-volume-1000-LOW',
 'bar-CME-HO-volume-1000-HIGH',
 'bar-CME-HO-volume-1000-CLOSE',
 'bar-CME-HO-volume-1000-VOLUME',
 'bar-CME-HO-volume-1000-TIMESTAMP',
 'bar-CME-HO-volume-1000-TIMEDELTA',
 'bar-CME-HO-volume-1000-ASK_SIZE',
 'bar-CME-HO-volume-1000-BID_SIZE',
 'bar-CME-HO-volume-1000-SIZE',
 'bar-CME-HO-volume-1000-RETURN']
```
---
#### Example:
```python
pprint(
    BarFeatureSupplier(
        supplier=BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=1_000
        )
    ).bar_features
)
```
#### Output:
```python
['bar_features-bar-CME-HO-volume-1000-RETURN_TIMEDELTA',
 'bar_features-bar-CME-HO-volume-1000-BID_SIZE_TIMEDELTA',
 'bar_features-bar-CME-HO-volume-1000-ASK_SIZE_TIMEDELTA',
 'bar_features-bar-CME-HO-volume-1000-RETURN_BID_SIZE',
 'bar_features-bar-CME-HO-volume-1000-RETURN_ASK_SIZE',
 'bar_features-bar-CME-HO-volume-1000-VOLUME_DELTA',
 'bar_features-bar-CME-HO-volume-1000-CUMULATIVE_VOLUME_DELTA',
 'bar_features-bar-CME-HO-volume-1000-INTERNAL_BAR_STRENGTH',
 'bar_features-bar-CME-HO-volume-1000-OFI',
 'bar_features-bar-CME-HO-volume-1000-OFI_NORMALIZED',
 'bar_features-bar-CME-HO-volume-1000-OPEN_HIGH',
 'bar_features-bar-CME-HO-volume-1000-OPEN_LOW',
 'bar_features-bar-CME-HO-volume-1000-OPEN_CLOSE',
 'bar_features-bar-CME-HO-volume-1000-SIGNAL']
```
---

### API Draft
Join multiple bar aggregations / time-frames
```python
from ts.supplier import TickSupplier
from ts.supplier import BarSupplier, Bar
from ts.supplier import BarAggregation
from ts.supplier import BarFeatureSupplier, BarFeatures

from ts.supplier import MultiplexSupplier
from ts.supplier import JOIN_TYPE

filepath = "/Users/jacob/projects/data/continuous_futures/CME-HO.parquet"
tick_supplier = TickSupplier(instrument="CME-HO")
tick_supplier.from_parquet(filepath)


supplier = MultiplexSupplier(
    join_on=JOIN_TYPE.ON_TIME,
    suppliers=[
        RollingNormSupplier(
            supplier=BarFeatureSupplier(
                supplier=BarSupplier(
                    tick_supplier,
                    bar_aggregation=BarAggregation.VOLUME,
                    size=1_000
                )
            ),
            window=200,
        ),
        RollingNormSupplier(
            supplier=BarFeatureSupplier(
                supplier=BarSupplier(
                    tick_supplier,
                    bar_aggregation=BarAggregation.VOLUME,
                    size=10_000
                )
            ),
            window=20,
        ),
    ]
)
train, test = split(supplier)
model.fit(
    train.drop_future()
)
```


### To-do
* SyntheticInstrumentSupplier: Build signal off multiple assets / signals.
* SpreadSupplier: Calculates spread based off multiple assets / signals.
* RollingFeaturesSupplier: Calculates different features over a rolling window.
* Feature calculations:
  * Features.CORRELATION
  * Features.VWAP
  * Features.TWAP
  * Features.KALMANFILTER
  * Features.EWMA
  * Features.MA
  * Features.VOLATILITY
* RollingNormSupplier: Normalize assets / features / bars according.
