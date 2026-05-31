use gaussflow_core::TypeSafeDag;
use pyo3::prelude::*;
use pyo3_asyncio::tokio as pyo3_tokio;
use serde_json::Value;

/// Validate a workflow JSON string. Returns basic stats or raises an exception.
#[pyfunction]
fn validate(workflow_json: &str) -> PyResult<(String, usize, usize)> {
    let dag = TypeSafeDag::from_json(workflow_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    Ok((dag.name, dag.graph.node_count(), dag.graph.edge_count()))
}

/// Execute a workflow asynchronously. Returns a Python awaitable (coroutine).
#[pyfunction]
fn execute_py<'a>(
    py: Python<'a>,
    workflow_json: &'a str,
    input_json: Option<&'a str>,
) -> PyResult<&'a PyAny> {
    let wf = workflow_json.to_owned();
    let inp = input_json.map(|s| s.to_owned());
    pyo3_tokio::future_into_py(py, async move {
        let dag = TypeSafeDag::from_json(&wf)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let input_val: Value = match inp {
            Some(s) => serde_json::from_str(&s)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            None => Value::Null,
        };
        let res = gaussflow_runtime::execute(dag, input_val)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(pyo3::Python::with_gil(|py| {
            serde_json::to_string(&res).unwrap().into_py(py)
        }))
    })
}

#[pymodule]
fn gaussflow_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(execute_py, m)?)?;

    // Convenience constant for version.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
