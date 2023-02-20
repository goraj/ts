import pytest
import polars as pl
import datetime
import zoneinfo
from ts.supplier import TickSupplier, BarSupplier, BarAggregation, MultiplexSupplier



def make_tick_supplier(instrument: str):
    supplier = TickSupplier(instrument=instrument)
    supplier.data = pl.DataFrame(
        {
            "timestamp": [
                datetime.datetime(2019, 12, 4, 8, 55, 49, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern')),
                datetime.datetime(2019, 12, 4, 8, 55, 49, 1000, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern')),
                datetime.datetime(2019, 12, 4, 8, 56, 2, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern')),
                datetime.datetime(2019, 12, 4, 8, 56, 19, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern')),
                datetime.datetime(2019, 12, 4, 8, 56, 30, tzinfo=zoneinfo.ZoneInfo(key='US/Eastern'))
            ],
            "side": [1, 1, 1, 1, 1],
            "price": [19094.0, 19094.0, 19096.0, 19100.0, 19097.0],
            "quantity": [1, 1, 1, 1, 1],
        }
    )
    return supplier

@pytest.fixture
def tick_supplier():
    return make_tick_supplier(instrument="CME-HO")
@pytest.fixture
def bar_supplier():
    supplier = BarSupplier(tick_supplier, bar_aggregation=BarAggregation.VOLUME, size=1)
    return supplier

@pytest.fixture
def bar_suppliers():
    return [
        BarSupplier(make_tick_supplier(instrument="CME-HO"), bar_aggregation=BarAggregation.VOLUME, size=1),
        BarSupplier(make_tick_supplier(instrument="CME-NG"), bar_aggregation=BarAggregation.VOLUME, size=1),
    ]

class TestBarSupplier:
    def test_instruments(self, tick_supplier):
        bar_supplier = BarSupplier(tick_supplier, bar_aggregation=BarAggregation.VOLUME, size=1)
        assert bar_supplier.instruments == ["CME-HO"]

    def test_bars(self, tick_supplier):
        bar_supplier = BarSupplier(tick_supplier, bar_aggregation=BarAggregation.VOLUME, size=1)
        assert sorted(bar_supplier.bars) == sorted(bar_supplier.data.columns)

    def test_bar_aggregation_volume(self, tick_supplier):
        bar_supplier = BarSupplier(tick_supplier, bar_aggregation=BarAggregation.VOLUME, size=2)
        volume_col = [e for e in bar_supplier.bars if e.endswith("volume")][0]

        # first bar has the wrong size due to our cumsum group-by aggregation
        assert bar_supplier.data[volume_col].to_list() == [1, 2, 2]

    def test_bar_aggregation_time(self, tick_supplier):
        bar_supplier = BarSupplier(tick_supplier, bar_aggregation=BarAggregation.TIME_SECONDS, size=30)
        assert len(bar_supplier.data) == 3

class TestMultiplexSupplier:
    def test_instruments(self, bar_suppliers):
        multiplex_supplier = MultiplexSupplier(suppliers=bar_suppliers)
        assert multiplex_supplier.instruments == ['CME-HO', 'CME-NG']

    def test_data(self, bar_suppliers):
        multiplex_supplier = MultiplexSupplier(suppliers=bar_suppliers)
        assert len(multiplex_supplier.data.columns) == 20
