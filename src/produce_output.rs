use pyo3::prelude::*;

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::types::PyList;

use fluvio::{ProduceOutput as NativeProduceOutput, RecordMetadata as NativeRecordMetadata};
use fluvio_future::{
    // io::{Stream, StreamExt},
    task::run_block_on,
};

use crate::error::FluvioError;
use crate::RecordMetadata;

#[pyclass]
pub struct ProduceOutput {
    // inner is placed into an Option because the native ProduceOutput
    // is consumed by the `wait` method, which is not possible on the python interface
    // (only `&self` or `&mut self` is allowed)
    pub inner: Option<NativeProduceOutput>,
}

#[pymethods]
impl ProduceOutput {
    fn wait(&mut self, py: Python) -> Result<Option<RecordMetadata>, FluvioError> {
        // wait on `inner` consumes `self`, but we only have a `&mut self` reference
        // so we take it out of the `Option` and consume it that way
        // a subsequent call to `wait` will return `None`
        let inner = self.inner.take();
        inner
            .map(|produce_output| {
                run_block_on(produce_output.wait())
                    .map(|metadata| RecordMetadata { inner: metadata })
                    .map_err(FluvioError::FluvioErr)
            })
            .transpose()
    }

    fn async_wait<'b>(&'b mut self, py: Python<'b>) -> PyResult<Bound<'b, PyAny>> {
        let inner = self.inner.take();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            let record_metadata = match inner {
                Some(produce_output) => {
                    let pout = produce_output
                        .wait()
                        .await
                        .map(|metadata| RecordMetadata { inner: metadata })
                        .map_err(FluvioError::FluvioErr)?;
                    Some(pout)
                }
                None => None,
            };
            Python::with_gil(|py| {
                let out = match record_metadata {
                    Some(record_metadata) => record_metadata.into_py(py),
                    None => py.None(),
                };
                Ok(out)
            })
        })
    }
}
