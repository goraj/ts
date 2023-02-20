from ts.supplier import TickSupplier
from ts.supplier import BarSupplier, Bar
from ts.supplier import BarAggregation
from ts.supplier import BarFeatureSupplier, BarFeatures



JOIN_vON_TIME = "TIME"

filepath = "/Users/jacobgora/projects/data/continuous_futures/CME-HO.parquet"
tick_supplier = TickSupplier(instrument="CME-HO")
tick_supplier.from_parquet(filepath)
print(tick_supplier.instruments)
# MultiplexSupplier
# SyntheticInstrumentSupplier
# SpreadSupplier


# Features.CORRELATION
# Features.VWAP
# Features.TWAP
# Features.EWMA
# Features.MA
# Features.VOLATILITY

# supplier.drop_future()

# RollingFeaturesSupplier





def test_barsupplier_volume_bars():
    bar_supplier = BarSupplier(
        supplier=tick_supplier,
        bar_aggregation=BarAggregation.VOLUME,
        size=100
    )
    bar_supplier.data[[bar_supplier_thousand.index]]


bar_supplier_hundred = BarSupplier(
    supplier=tick_supplier,
    bar_aggregation=BarAggregation.TIME_MINUTES,
    size=100
)

print(tick_supplier.data)
bar_supplier_hundred = BarSupplier(
    supplier=tick_supplier,
    bar_aggregation=BarAggregation.VOLUME,
    size=100
)
print(bar_supplier_hundred.data.head())
bar_supplier_thousand = BarSupplier(
    supplier=tick_supplier,
    bar_aggregation=BarAggregation.VOLUME,
    size=1_000
)

from pprint import pprint
pprint(
    BarFeatureSupplier(
        supplier=BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=1_000
        )
    ).bar_features
)

from pprint import pprint
pprint(
    BarSupplier(
            supplier=tick_supplier,
            bar_aggregation=BarAggregation.VOLUME,
            size=1_000
    ).bars
)

