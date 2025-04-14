use pyo3::prelude::*;

#[pyfunction]
fn say_hello() -> String {
    "Hello from Rust!".to_string()
}

#[pymodule]
fn pyrustpipe(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(say_hello, m)?)?;
    Ok(())
}