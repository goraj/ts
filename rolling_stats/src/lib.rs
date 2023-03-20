mod ffi;
mod rolling_stats;
use polars::prelude::*;
use pyo3::types::{PyFloat, PyDateTime, PyInt, PyLong};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use std::collections::HashMap;
use polars::export::chrono::Timelike;

fn rolling_stats_impl(datetimes: &Series, values: &Series, window_size: usize) -> PolarsResult<Series> {
    let mut binned_rolling_stats = rolling_stats::BinnedRollingStatistics::new(window_size);
    Ok(
        datetimes
        .datetime()?
            .as_datetime_iter()
            .map(|x| x.unwrap())
        .zip(
            values.f64()?
                .into_iter()
                .map(|x| x.unwrap())
        )
        .map(|(dt, value)| {
            Ok(
                Some(
                    binned_rolling_stats.update_and_return_z_score(
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
fn pl_rolling_stats(py_datetimes: &PyAny, py_values: &PyAny, py_window_size: usize) -> PyResult<PyObject> {
    let series_a = ffi::py_series_to_rust_series(py_datetimes)?;
    let series_b = ffi::py_series_to_rust_series(py_values)?;

    let window_size: usize = py_window_size;
    let out = rolling_stats_impl(&series_a, &series_b, window_size)
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out.into_series())
}

#[pymodule]
fn polars_rollingstats(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(pl_rolling_stats)).unwrap();
    Ok(())
}
