mod ffi;
mod rolling_stats;
use polars::prelude::*;
use pyo3::types::{PyFloat, PyDateTime};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use std::collections::HashMap;
use polars::export::chrono::Timelike;

fn rolling_stats_impl(datetimes: &Series, values: &Series) -> PolarsResult<Series> {
    let mut binned_rolling_stats = rolling_stats::BinnedRollingStatistics::default();
    Ok(
        datetimes
        .datetime()?
            .as_datetime_iter()
            .map(|x| x.unwrap()) // shouldn't this be T<DateTime> or something? Not i64?
        .zip(
            values.f64()?
                .into_iter()
                .map(|x| x.unwrap())
        )
        .map(|(dt, value)| {
            Ok(
                Some(
                    binned_rolling_stats.update_and_return_z_score(
                        // WRONG (???) why is this i64 what we really want is something like
                        dt.hour() as u8,
                        dt.minute() as u8,
                        value as f64
                    )
                )
            )
        })
        .collect::<PolarsResult<Series>>()?
    )
}

#[pyfunction]
fn rolling_stats(py_datetimes: &PyAny, py_values: &PyAny) -> PyResult<PyObject> {
    let series_a = ffi::py_series_to_rust_series(py_datetimes)?;
    let series_b = ffi::py_series_to_rust_series(py_values)?;

    let out = rolling_stats_impl(&series_a, &series_b)
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out.into_series())
}

#[pymodule]
fn my_polars_functions(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(rolling_stats)).unwrap();
    Ok(())
}
