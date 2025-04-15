#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::useless_conversion)]
#![allow(clippy::type_complexity)]

use pyo3::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

#[pyfunction]
fn say_hello() -> String {
    "Hello from Rust!".to_string()
}

#[pyfunction]
fn is_valid_email(email: &str) -> bool {
    let re = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();
    re.is_match(email)
}

#[pyfunction]
fn is_valid_number(value: &str) -> bool {
    value.parse::<f64>().is_ok()
}

#[pyfunction]
fn validate_json_field(json_str: &str, field: &str, field_type: &str) -> PyResult<bool> {
    let json: Value = serde_json::from_str(json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid JSON: {}", e)))?;
    match field_type {
        "email" => {
            let email = json.get(field)
                .and_then(|v| v.as_str())
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("No '{}' field or not a string", field)))?;
            Ok(is_valid_email(email))
        }
        "number" => {
            let number = json.get(field)
                .and_then(|v| v.as_str())
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("No '{}' field or not a string", field)))?;
            Ok(is_valid_number(number))
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!("Unsupported field type: {}", field_type))),
    }
}

#[pyfunction]
fn validate_json_stream(lines: Vec<String>, field: &str, field_type: &str) -> PyResult<Vec<bool>> {
    let mut results = Vec::new();
    for line in lines {
        let json: Value = match serde_json::from_str(&line) {
            Ok(json) => json,
            Err(_) => {
                results.push(false);
                continue;
            }
        };
        let value = json.get(field).and_then(|v| v.as_str());
        let result = match field_type {
            "email" => value.map(is_valid_email).unwrap_or(false),
            "number" => value.map(is_valid_number).unwrap_or(false),
            "url" | "boolean" => match value {
                Some(val) => Python::with_gil(|py| {
                    let module = PyModule::import_bound(py, "pyrustpipe_rules")?;
                    let rule_class = module.getattr("Rule")?;
                    let rule = rule_class.call1((field_type,))?;
                    let result = rule.call_method1("validate", (val,))?.extract::<bool>()?;
                    Ok::<bool, PyErr>(result)
                })?,
                None => false,
            },
            _ => return Err(pyo3::exceptions::PyValueError::new_err(format!("Unsupported field type: {}", field_type))),
        };
        results.push(result);
    }
    Ok(results)
}

#[pyfunction]
fn process_json_stream(lines: Vec<String>, field: &str, field_type: &str) -> PyResult<Vec<String>> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let mut results = Vec::new();
    for line in lines {
        let json: Value = match serde_json::from_str(&line) {
            Ok(json) => json,
            Err(_) => continue,
        };
        let value = json.get(field).and_then(|v| v.as_str());
        let valid = match field_type {
            "email" => value.map(is_valid_email).unwrap_or(false),
            "number" => value.map(is_valid_number).unwrap_or(false),
            "url" | "boolean" => match value {
                Some(val) => Python::with_gil(|py| {
                    let module = PyModule::import_bound(py, "pyrustpipe_rules")?;
                    let rule_class = module.getattr("Rule")?;
                    let rule = rule_class.call1((field_type,))?;
                    let result = rule.call_method1("validate", (val,))?.extract::<bool>()?;
                    Ok::<bool, PyErr>(result)
                })?,
                None => false,
            },
            _ => return Err(pyo3::exceptions::PyValueError::new_err(format!("Unsupported field type: {}", field_type))),
        };
        if valid {
            let transformed = match field_type {
                "email" => value.map(|v| v.to_uppercase()),
                "number" => value.map(|v| format!("{:.2}", v.parse::<f64>().unwrap())),
                "url" => value.map(|v| {
                    let result = if let Some(caps) = url_re.captures(v) {
                        format!("{}://{}{}", caps[1].to_lowercase(), caps[3].to_lowercase(), &caps[4])
                    } else {
                        v.to_string()
                    };
                    eprintln!("URL Input: {}, Output: {}", v, result);
                    result
                }),
                "boolean" => value.map(|v| v.to_lowercase()),
                _ => None,
            };
            if let Some(t) = transformed {
                results.push(t);
            }
        }
    }
    Ok(results)
}

#[pyfunction]
fn process_json_stream_with_errors(lines: Vec<String>, field: &str, field_type: &str) -> PyResult<(Vec<String>, Vec<(usize, String, String)>)> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let json: Value = match serde_json::from_str(line) {
            Ok(json) => json,
            Err(e) => {
                errors.push((index, line.clone(), format!("Invalid JSON: {}", e)));
                continue;
            }
        };
        let value = json.get(field).and_then(|v| v.as_str());
        let valid = match field_type {
            "email" => match value {
                Some(email) => is_valid_email(email),
                None => {
                    errors.push((index, line.clone(), format!("No '{}' field", field)));
                    false
                }
            },
            "number" => match value {
                Some(number) => is_valid_number(number),
                None => {
                    errors.push((index, line.clone(), format!("No '{}' field", field)));
                    false
                }
            },
            "url" | "boolean" => match value {
                Some(val) => match Python::with_gil(|py| {
                    let module = PyModule::import_bound(py, "pyrustpipe_rules")?;
                    let rule_class = module.getattr("Rule")?;
                    let rule = rule_class.call1((field_type,))?;
                    let result = rule.call_method1("validate", (val,))?.extract::<bool>()?;
                    Ok::<bool, PyErr>(result)
                }) {
                    Ok(result) => result,
                    Err(e) => {
                        errors.push((index, line.clone(), format!("Validation error: {}", e)));
                        false
                    }
                },
                None => {
                    errors.push((index, line.clone(), format!("No '{}' field", field)));
                    false
                }
            },
            _ => {
                errors.push((index, line.clone(), format!("Unsupported field type: {}", field_type)));
                continue;
            }
        };
        if valid {
            let transformed = match field_type {
                "email" => value.map(|v| v.to_uppercase()),
                "number" => value.map(|v| format!("{:.2}", v.parse::<f64>().unwrap())),
                "url" => value.map(|v| {
                    let result = if let Some(caps) = url_re.captures(v) {
                        format!("{}://{}{}", caps[1].to_lowercase(), caps[3].to_lowercase(), &caps[4])
                    } else {
                        v.to_string()
                    };
                    eprintln!("URL Input: {}, Output: {}", v, result);
                    result
                }),
                "boolean" => value.map(|v| v.to_lowercase()),
                _ => None,
            };
            if let Some(t) = transformed {
                results.push(t);
            }
        } else if value.is_some() {
            errors.push((index, line.clone(), format!("Invalid {}: {}", field_type, value.unwrap())));
        }
    }
    Ok((results, errors))
}

#[pyfunction]
fn process_json_stream_batch(lines: Vec<String>, field_types: Vec<(String, String)>) -> PyResult<HashMap<String, Vec<String>>> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let mut results: HashMap<String, Vec<String>> = field_types.iter().map(|(_, ft)| (ft.clone(), Vec::new())).collect();
    for (field, field_type) in field_types {
        for line in &lines {
            let json: Value = match serde_json::from_str(line) {
                Ok(json) => json,
                Err(_) => continue,
            };
            let value = json.get(&field).and_then(|v| v.as_str());
            let valid = match field_type.as_str() {
                "email" => value.map(is_valid_email).unwrap_or(false),
                "number" => value.map(is_valid_number).unwrap_or(false),
                "url" | "boolean" => match value {
                    Some(val) => Python::with_gil(|py| {
                        let module = PyModule::import_bound(py, "pyrustpipe_rules")?;
                        let rule_class = module.getattr("Rule")?;
                        let rule = rule_class.call1((field_type.as_str(),))?;
                        let result = rule.call_method1("validate", (val,))?.extract::<bool>()?;
                        Ok::<bool, PyErr>(result)
                    })?,
                    None => false,
                },
                _ => continue,
            };
            if valid {
                let transformed = match field_type.as_str() {
                    "email" => value.map(|v| v.to_uppercase()),
                    "number" => value.map(|v| format!("{:.2}", v.parse::<f64>().unwrap())),
                    "url" => value.map(|v| {
                        let result = if let Some(caps) = url_re.captures(v) {
                            format!("{}://{}{}", caps[1].to_lowercase(), caps[3].to_lowercase(), &caps[4])
                        } else {
                            v.to_string()
                        };
                        eprintln!("URL Input: {}, Output: {}", v, result);
                        result
                    }),
                    "boolean" => value.map(|v| v.to_lowercase()),
                    _ => None,
                };
                if let Some(t) = transformed {
                    results.get_mut(&field_type).unwrap().push(t);
                }
            }
        }
    }
    Ok(results)
}

#[pymodule]
fn pyrustpipe(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(say_hello, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_email, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_number, m)?)?;
    m.add_function(wrap_pyfunction!(validate_json_field, m)?)?;
    m.add_function(wrap_pyfunction!(validate_json_stream, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream_with_errors, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream_batch, m)?)?;
    Ok(())
}