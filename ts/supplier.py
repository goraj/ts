import logging
from abc import ABC, abstractmethod
from re import match

import numpy as np
import polars as pl

logger = logging.getLogger()


class SupplierType:
    TICK = "tick"
    BAR = "bar"
    BAR_FEATURES = "bar_features"
    ROLLING_FEATURES = "rolling_features"
    MULTIPLEX = "multiplex"


class BarAggregation:
    VOLUME = "volume_agg"
    TIME_MILLISECONDS = "time_milliseconds_agg"
    TIME_SECONDS = "time_seconds_agg"
    TIME_MINUTES = "time_minutes_agg"


class TradeTick:
    @staticmethod
    def alias():
        return SupplierType.TICK

    @staticmethod
    def get_members():
        return [e for e in list(TradeTick.__dict__) if e.upper() == e and "__" not in e]

    QUANTITY = "quantity"
    PRICE = "price"
    TIMESTAMP = "timestamp"
    SIDE = "side"


class Bar:
    @staticmethod
    def alias():
        return SupplierType.BAR

    @staticmethod
    def get_members():
        return [e for e in list(Bar.__dict__) if e.upper() == e and "__" not in e]

    # private index used for processing only
    __INDEX__ = "index"

    OPEN = "open"
    LOW = "low"
    HIGH = "high"
    CLOSE = "close"
    VOLUME = "volume"
    TIMESTAMP = "timestamp"
    TIMEDELTA = "timedelta"

    ASK_SIZE = "ask_size"
    BID_SIZE = "bid_size"
    # SIZE = "size"

    RETURN = "return"
    LOG_RETURN = "log_return"


def match_col(col_alias: str, col_attr: str, column: str) -> bool:
    """Matches column name against col_alias, col_attr ie: (Bar, Bar.OPEN)"""
    return match(f"^{col_alias}.*-{col_attr}($|-.*)", column) is not None


class BarFeature(Bar):
    @staticmethod
    def alias():
        return SupplierType.BAR_FEATURES

    @staticmethod
    def get_members():
        return [
            e for e in list(BarFeature.__dict__) if e.upper() == e and "__" not in e
        ]

    # velocity ratios
    RETURN_TIMEDELTA = f"{Bar.RETURN}_{Bar.TIMEDELTA}"
    BID_SIZE_TIMEDELTA = f"{Bar.BID_SIZE}_{Bar.TIMEDELTA}"
    ASK_SIZE_TIMEDELTA = f"{Bar.ASK_SIZE}_{Bar.TIMEDELTA}"

    RETURN_BID_SIZE = f"{Bar.RETURN}_{Bar.BID_SIZE}"
    RETURN_ASK_SIZE = f"{Bar.RETURN}_{Bar.ASK_SIZE}"

    VOLUME = f"{Bar.VOLUME}"
    VOLUME_DELTA = f"{Bar.VOLUME}_delta"

    CUMULATIVE_VOLUME_DELTA = f"cumulative_{Bar.VOLUME}_delta"
    INTERNAL_BAR_STRENGTH = "internal_bar_strength"

    OFI = "ofi"
    OFI_NORMALIZED = "ofi_normalized"

    OPEN_HIGH = "open_high"
    OPEN_LOW = "open_low"
    OPEN_CLOSE = "open_close"
    SIGNAL = "signal"

    POS_REALIZED_VARIANCE = "pos_realized_variance"
    NEG_REALIZED_VARIANCE = "neg_realized_variance"


class BaseSupplier(ABC):
    supplier_type = "BaseSupplier"

    def __init__(self):
        raise NotImplemented

    @property
    @abstractmethod
    def instruments(self):
        pass


class TickSupplier(BaseSupplier):
    supplier_type = "TickSupplier"

    def __init__(self, instrument: str):
        self.instrument = instrument
        self.data = None

    def from_parquet(self, filepath: str):
        self.data = pl.read_parquet(filepath)

    @property
    def instruments(self) -> list[str]:
        return [self.instrument]


class BarSupplier(BaseSupplier):
    supplier_type = "BarSupplier"

    def __init__(self, supplier: TickSupplier, bar_aggregation: str, size: int):
        self.supplier = supplier
        self.instrument = supplier.instrument
        self.bar_aggregation = bar_aggregation
        self.size = size
        self.alias = f"{SupplierType.BAR}-{self.instrument}-{bar_aggregation}-{size}"

        match bar_aggregation:
            case BarAggregation.VOLUME:
                self.index = f"{self.alias}-{Bar.VOLUME}"
                self.data = self._aggregate_bar(
                    data=self.supplier.data,
                    bar_aggregation=bar_aggregation,
                    size=self.size,
                ).with_columns(
                    pl.col(f"{self.alias}-{Bar.CLOSE}")
                    .pct_change()
                    .fill_null(0)
                    .alias(f"{self.alias}-{Bar.RETURN}")
                )
            case elem if elem in (
                BarAggregation.TIME_MILLISECONDS,
                BarAggregation.TIME_SECONDS,
                BarAggregation.TIME_MINUTES,
            ):
                self.index = f"{self.alias}-{Bar.TIMESTAMP}"
                self.data = self._aggregate_bar(
                    data=self.supplier.data,
                    bar_aggregation=bar_aggregation,
                    size=self.size,
                )
            case _:
                raise NotImplemented

        self.data = self.data.with_columns(
            [
                pl.col(f"{self.alias}-{Bar.CLOSE}")
                .pct_change()
                .fill_null(0)
                .alias(f"{self.alias}-{Bar.RETURN}"),
                np.log1p(
                    pl.col(f"{self.alias}-{Bar.CLOSE}").pct_change().fill_null(0)
                ).alias(f"{self.alias}-{Bar.LOG_RETURN}"),
            ]
        )

    def _aggregate_bar(
        self, data: pl.DataFrame, bar_aggregation: str, size: int
    ) -> pl.DataFrame:
        # bar calculations
        agg_args = [
            pl.col(TradeTick.PRICE).first().alias(f"{self.alias}-{Bar.OPEN}"),
            pl.col(TradeTick.PRICE).min().alias(f"{self.alias}-{Bar.LOW}"),
            pl.col(TradeTick.PRICE).max().alias(f"{self.alias}-{Bar.HIGH}"),
            pl.col(TradeTick.PRICE).last().alias(f"{self.alias}-{Bar.CLOSE}"),
            pl.col(TradeTick.QUANTITY).sum().alias(f"{self.alias}-{Bar.VOLUME}"),
            pl.col(TradeTick.TIMESTAMP).last().alias(f"{self.alias}-{Bar.TIMESTAMP}"),
            (
                (
                    pl.col(TradeTick.TIMESTAMP).last()
                    - pl.col(TradeTick.TIMESTAMP).first()
                ).dt.milliseconds()
            ).alias(f"{self.alias}-{Bar.TIMEDELTA}"),
            ((pl.col(TradeTick.SIDE) == 0) * pl.col(TradeTick.QUANTITY))
            .sum()
            .alias(f"{self.alias}-{Bar.ASK_SIZE}"),
            ((pl.col(TradeTick.SIDE) == 1) * pl.col(TradeTick.QUANTITY))
            .sum()
            .alias(f"{self.alias}-{Bar.BID_SIZE}"),
        ]

        match bar_aggregation:
            case BarAggregation.VOLUME:
                temp_alias = f"{self.alias}-{Bar.__INDEX__}"
                data = (
                    data.with_columns(
                        (
                            (pl.col(TradeTick.QUANTITY).cumsum() / size).cast(
                                pl.UInt64, strict=False
                            )
                            * size
                        ).alias(temp_alias)
                    )
                    .groupby(temp_alias)
                    .agg(agg_args)
                    .sort(f"{self.alias}-{Bar.TIMESTAMP}")
                    .drop([temp_alias])
                )
                return data

            case BarAggregation.TIME_MILLISECONDS:
                data = (
                    data.groupby_dynamic(TradeTick.TIMESTAMP, every=f"{size}ms")
                    .agg(agg_args)
                    .sort(f"{self.alias}-{Bar.TIMESTAMP}")
                )
                return data
            case BarAggregation.TIME_SECONDS:
                data = (
                    data.groupby_dynamic(TradeTick.TIMESTAMP, every=f"{size}s")
                    .agg(agg_args)
                    .sort(f"{self.alias}-{Bar.TIMESTAMP}")
                )
                return data
            case BarAggregation.TIME_MINUTES:
                data = (
                    data.groupby_dynamic(TradeTick.TIMESTAMP, every=f"{60 * size}s")
                    .agg(agg_args)
                    .sort(f"{self.alias}-{Bar.TIMESTAMP}")
                    .drop(TradeTick.TIMESTAMP)
                )
                return data
            case _:
                raise NotImplementedError

    @property
    def bars(self) -> list[str]:
        bar_attributes = Bar.get_members()
        return [
            f"{self.alias}-{bar_attribute.lower()}" for bar_attribute in bar_attributes
        ]

    @property
    def instruments(self) -> list[str]:
        return [self.instrument]

    def from_parquet(self, filepath: str):
        self.data = pl.read_parquet(filepath)

    def get_col(self, col_type: Bar | BarFeature, type_attr: str) -> str | None:
        columns = [
            col
            for col in self.data.columns
            if match_col(col_type.alias(), type_attr, col)
        ]
        if not columns:
            raise ValueError(f"{col_type = } has no {type_attr =}")

        return columns[0]


class BarFeatureSupplier(BaseSupplier):
    supplier_type = "BarFeatureSupplier"

    def __init__(self, supplier: BarSupplier):
        self.supplier = supplier
        self.instrument = supplier.instrument
        self.bar_aggregation = supplier.bar_aggregation
        self.size = supplier.size
        self.alias = f"{SupplierType.BAR_FEATURES}-{supplier.alias}"
        self.index = supplier.index

        self.data = supplier.data.with_columns(
            [
                (
                    pl.col(f"{supplier.alias}-{Bar.RETURN}")
                    / pl.col(f"{supplier.alias}-{Bar.TIMEDELTA}")
                ).alias(f"{self.alias}-{BarFeature.RETURN_TIMEDELTA}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.BID_SIZE}")
                    / pl.col(f"{supplier.alias}-{Bar.TIMEDELTA}")
                )
                .fill_nan(0)
                .alias(f"{self.alias}-{BarFeature.BID_SIZE_TIMEDELTA}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.ASK_SIZE}")
                    / pl.col(f"{supplier.alias}-{Bar.TIMEDELTA}")
                ).alias(f"{self.alias}-{BarFeature.ASK_SIZE_TIMEDELTA}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.ASK_SIZE}")
                    - pl.col(f"{supplier.alias}-{Bar.BID_SIZE}")
                ).alias(f"{self.alias}-{BarFeature.VOLUME_DELTA}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.BID_SIZE}")
                    + pl.col(f"{supplier.alias}-{Bar.ASK_SIZE}")
                ).alias(f"{self.alias}-{BarFeature.VOLUME}"),
            ]
        ).with_columns(
            [
                pl.when(
                    pl.col(
                        f"{self.alias}-{BarFeature.ASK_SIZE_TIMEDELTA}"
                    ).is_infinite()
                    | pl.col(f"{self.alias}-{BarFeature.ASK_SIZE_TIMEDELTA}").is_nan()
                )
                .then(float(0))
                .otherwise(pl.col(f"{self.alias}-{BarFeature.ASK_SIZE_TIMEDELTA}"))
                .keep_name(),
                # pl.when(
                #     pl.col(f"{supplier.alias}-{Bar.BID_SIZE}").is_infinite()
                #     | pl.col(f"{supplier.alias}-{Bar.BID_SIZE}").is_nan()
                # )
                # .then(float(0))
                # .otherwise(pl.col(f"{self.alias}-{BarFeatures.BID_SIZE_TIMEDELTA}"))
                # .keep_name(),
                pl.when(
                    pl.col(f"{self.alias}-{BarFeature.RETURN_TIMEDELTA}").is_infinite()
                    | pl.col(f"{self.alias}-{BarFeature.RETURN_TIMEDELTA}").is_nan()
                )
                .then(float(0))
                .otherwise(pl.col(f"{self.alias}-{BarFeature.RETURN_TIMEDELTA}"))
                .keep_name(),
                (
                    pl.col(f"{supplier.alias}-{Bar.BID_SIZE}")
                    - pl.col(f"{supplier.alias}-{Bar.ASK_SIZE}")
                ).alias(f"{self.alias}-{BarFeature.OFI}"),
                (
                    (
                        pl.col(f"{supplier.alias}-{Bar.BID_SIZE}")
                        - pl.col(f"{supplier.alias}-{Bar.ASK_SIZE}")
                    )
                    / pl.col(f"{self.alias}-{Bar.VOLUME}")
                ).alias(f"{self.alias}-{BarFeature.OFI_NORMALIZED}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.OPEN}")
                    / pl.col(f"{supplier.alias}-{Bar.HIGH}")
                ).alias(f"{self.alias}-{BarFeature.OPEN_HIGH}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.OPEN}")
                    / pl.col(f"{supplier.alias}-{Bar.LOW}")
                ).alias(f"{self.alias}-{BarFeature.OPEN_LOW}"),
                (
                    pl.col(f"{supplier.alias}-{Bar.OPEN}")
                    / pl.col(f"{supplier.alias}-{Bar.CLOSE}")
                ).alias(f"{self.alias}-{BarFeature.OPEN_CLOSE}"),
                (
                    pl.when(
                        (
                            pl.col(f"{supplier.alias}-{Bar.HIGH}")
                            - pl.col(f"{supplier.alias}-{Bar.LOW}")
                        )
                        == 0
                    )
                    .then(float(0))
                    .otherwise(
                        (
                            pl.col(f"{supplier.alias}-{Bar.CLOSE}")
                            - pl.col(f"{supplier.alias}-{Bar.LOW}")
                        )
                        / (
                            pl.col(f"{supplier.alias}-{Bar.HIGH}")
                            - pl.col(f"{supplier.alias}-{Bar.LOW}")
                        )
                    )
                ).alias(f"{self.alias}-{BarFeature.INTERNAL_BAR_STRENGTH}"),
            ]
        )

        self.data = self.data.with_columns(
            [
                pl.when(pl.col(self.get_col(Bar, Bar.RETURN)) > 0)
                .then(
                    np.sqrt(np.square(pl.col(self.get_col(Bar, Bar.RETURN))).cumsum())
                    .over(pl.col(self.get_col(Bar, Bar.TIMESTAMP)).dt.epoch(tu="d"))
                    .alias(f"{self.alias}-{BarFeature.POS_REALIZED_VARIANCE}")
                )
                .otherwise(0),
                pl.when(pl.col(self.get_col(Bar, Bar.RETURN)) < 0)
                .then(
                    np.sqrt(np.square(pl.col(self.get_col(Bar, Bar.RETURN))).cumsum())
                    .over(pl.col(self.get_col(Bar, Bar.TIMESTAMP)).dt.epoch(tu="d"))
                    .alias(f"{self.alias}-{BarFeature.NEG_REALIZED_VARIANCE}")
                )
                .otherwise(0),
            ]
        )

    @property
    def instruments(self) -> list[str]:
        return [self.instrument]

    @property
    def bars(self) -> list[str]:
        return self.supplier.bars

    @property
    def bar_features(self) -> list[str]:
        feature_attributes = BarFeature.get_members()
        return [
            f"{self.alias}-{feature_attribute}"
            for feature_attribute in feature_attributes
        ]

    def get_col(self, col_type: type[Bar | BarFeature], type_attr: str) -> str | None:
        columns = [
            col
            for col in self.data.columns
            if match_col(col_type.alias(), type_attr, col)
        ]
        if not columns:
            raise ValueError(f"{col_type = } has no {type_attr =}")

        return columns[0]


class MultiplexSupplier(BaseSupplier):
    supplier_type = "MultiplexSupplier"

    def __init__(self, suppliers: list[BarSupplier | BarFeatureSupplier]):
        self.alias = SupplierType.MULTIPLEX
        self._instruments = []
        self._bar_features = []

        if not all(
            [
                isinstance(supplier, BarSupplier)
                or isinstance(supplier, BarFeatureSupplier)
                for supplier in suppliers
            ]
        ):
            raise RuntimeError(f"Only BarSupplies supported. Passed: {suppliers = }.")

        aggregation_types = [supplier.bar_aggregation for supplier in suppliers]
        if not all(
            [agg_type == suppliers[0].bar_aggregation for agg_type in aggregation_types]
        ):
            raise RuntimeError(
                f"Require common BarAggregation type. Passed: {aggregation_types}"
            )

        aggregation_sizes = [supplier.size for supplier in suppliers]
        min_aggregation_size = min(aggregation_sizes)
        if not all(
            [(agg_size % min_aggregation_size) == 0 for agg_size in aggregation_sizes]
        ):
            raise RuntimeError(
                f"BarAggregation sizes need to be integer factor of highest frequency."
            )

        suppliers = [suppliers[i] for i in np.argsort(aggregation_sizes)]

        left_supplier = suppliers[0]
        left_index_col = left_supplier.index

        self.index = left_index_col

        self.data = left_supplier.data
        self._instruments.append(left_supplier.instrument)
        for supplier in suppliers[1:]:
            right_index_col = supplier.index
            self.data = self.data.join_asof(
                supplier.data, left_on=left_index_col, right_on=right_index_col
            )
            if supplier.instrument not in self._instruments:
                self._instruments.append(supplier.instrument)
        self.data = self.data.fill_null(strategy="forward")

    @property
    def instruments(self) -> list[str]:
        return self._instruments

    @property
    def bar_features(self) -> list[str]:
        return [
            column
            for column in self.data.columns
            if SupplierType.BAR_FEATURES in column
        ]

    def get_cols(
        self, col_type: type[Bar | BarFeature], type_attr: str
    ) -> list[str] | None:
        return [
            col
            for col in self.data.columns
            if match_col(col_type.alias(), type_attr, col)
        ]


class Function:
    Z_SCORE = "z_score"

    @staticmethod
    def alias():
        return SupplierType.ROLLING_FEATURES

    @staticmethod
    def z_score(column: str, window_size: int):
        return (
            pl.col(column)
            - pl.col(column).rolling_mean(window_size)
            / pl.col(column).rolling_std(window_size)
        ).alias(f"{Function.alias()}-{column}-{Function.Z_SCORE}-{window_size}")


class RollingFeatureSupplier(BaseSupplier):
    supplier_type = "RollingFeaturesSupplier"

    def __init__(
        self,
        supplier: BarFeatureSupplier | MultiplexSupplier,
        type_attributes: list[str],
        functions: list[str],
        window_size: int = 10,
    ):
        self.alias = SupplierType.MULTIPLEX
        self.data = supplier.data

        with_columns_arg = []
        if isinstance(supplier, BarFeatureSupplier):
            for function in functions:
                for type_attr in type_attributes:
                    column = supplier.get_col(BarFeature, type_attr)

                    try:
                        func = getattr(Function, function)
                    except AttributeError:
                        raise ValueError(
                            f"{Function = } has no attribute {function = }."
                        )

                    with_columns_arg.extend([func(column, window_size)])

        elif isinstance(supplier, MultiplexSupplier):
            for function in functions:
                for type_attr in type_attributes:
                    columns = supplier.get_cols(BarFeature, type_attr)

                    try:
                        func = getattr(Function, function)
                    except AttributeError:
                        raise ValueError(
                            f"{Function = } has no attribute {function = }."
                        )

                    with_columns_arg += [
                        func(column, window_size) for column in columns
                    ]
        else:
            raise ValueError(f"{supplier = } type not supported.")

        self.data = self.data.with_columns(with_columns_arg)

    @property
    def instruments(self) -> list[str]:
        return []

    @property
    def bar_features(self) -> list[str]:
        return []
