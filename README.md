# ts
_ts_ is an experimental Python 3 library for tick-data processing.

#### Example:
```python
filepath = "/data/continuous_futures/CME-HO.parquet"
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

### Example
Join multiple suppliers and featurize them and calculate rolling z-scores.
```python
filepath = "/data/continuous_futures/CBOT-ZN.parquet"
tick_supplier = TickSupplier(instrument="CBOT-ZN")
tick_supplier.from_parquet(filepath)

suppliers = [
    BarFeatureSupplier(
        supplier=BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=10
        )
    ),
    BarFeatureSupplier(
        supplier=BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=50
        )
    ),
    BarFeatureSupplier(
        supplier=BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=500
        )
    )
]

multiplex_supplier = MultiplexSupplier(suppliers=suppliers)

rolling_feat_supplier = RollingFeatureSupplier(
    supplier=multiplex_supplier,
    functions=[Function.Z_SCORE],
    type_attributes=[
        # OFI / Tape
        BarFeature.OFI_NORMALIZED,
        BarFeature.OFI,

        BarFeature.ASK_SIZE,
        BarFeature.BID_SIZE,

        BarFeature.ASK_SIZE_TIMEDELTA,
        BarFeature.BID_SIZE_TIMEDELTA,

        BarFeature.RETURN_TIMEDELTA,
        BarFeature.RETURN_BID_SIZE,
        BarFeature.RETURN_ASK_SIZE,

        # Volume
        BarFeature.CUMULATIVE_VOLUME_DELTA,
        BarFeature.VOLUME_DELTA,
        BarFeature.VOLUME,

        # Volatility
        BarFeature.POS_REALIZED_VARIANCE,
        BarFeature.NEG_REALIZED_VARIANCE,

        #
        BarFeature.INTERNAL_BAR_STRENGTH
    ],
    window_size=5
)
```


### TODO:
* SyntheticInstrumentSupplier: Build signal off multiple assets / signals.
* SpreadSupplier: Calculates spread based off multiple assets / signals.
* Function:
  * Function.CORRELATION
  * Function.VWAP
  * Function.TWAP
  * Function.KALMANFILTER
  * Function.EWMA
  * Function.MA
  * Function.VOLATILITY
