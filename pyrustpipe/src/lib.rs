#![allow(unsafe_op_in_unsafe_fn)]
use pyo3::prelude::*;
use regex::Regex;

#[pyfunction]
fn say_hello() -> String {
    "Hello from Rust!".to_string()
}

#[pyfunction]
fn is_valid_email(email: &str) -> bool {
    let re = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();
    re.is_match(email)
}

#[pymodule]
fn pyrustpipe(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(say_hello, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_email, m)?)?;
    Ok(())
}