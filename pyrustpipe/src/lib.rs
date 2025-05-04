#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::useless_conversion)]
#![allow(clippy::type_complexity)]

use pyo3::prelude::*;
use pyo3::types::PyIterator;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead};
use rayon::prelude::*;

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
fn normalize_url(url: &str) -> String {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    if let Some(caps) = url_re.captures(url) {
        let result = format!("{}://{}{}", caps[1].to_lowercase(), caps[3].to_lowercase(), &caps[4]);
        eprintln!("Normalize URL Input: {}, Output: {}", url, result);
        result
    } else {
        url.to_string()
    }
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
    let results: Vec<bool> = lines.par_iter() // Changed to par_iter()
        .map(|line| {
            eprintln!("Validating line: {}", line);
            let json: Value = match serde_json::from_str(line) {
                Ok(json) => json,
                Err(_) => return false,
            };
            let value = json.get(field).and_then(|v| v.as_str());
            eprintln!("Field value for {}: {:?}", field, value);
            match field_type {
                "email" => value.map(is_valid_email).unwrap_or(false),
                "number" => value.map(is_valid_number).unwrap_or(false),
                "url" | "boolean" => match value {
                    Some(val) => Python::with_gil(|py| {
                        eprintln!("Validating {}: {}", field_type, val);
                        let module = match PyModule::import_bound(py, "pyrustpipe_rules") {
                            Ok(module) => module,
                            Err(e) => {
                                eprintln!("Failed to import pyrustpipe_rules: {}", e);
                                return false;
                            }
                        };
                        let rule_class = match module.getattr("Rule") {
                            Ok(rule_class) => rule_class,
                            Err(e) => {
                                eprintln!("Failed to get Rule class: {}", e);
                                return false;
                            }
                        };
                        let rule = match rule_class.call1((field_type,)) {
                            Ok(rule) => rule,
                            Err(e) => {
                                eprintln!("Failed to create Rule instance: {}", e);
                                return false;
                            }
                        };
                        let result = match rule.call_method1("validate", (val,)) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Failed to call validate method: {}", e);
                                return false;
                            }
                        };
                        match result.extract::<bool>() {
                            Ok(result) => {
                                eprintln!("Validation result for {}: {}", val, result);
                                result
                            }
                            Err(e) => {
                                eprintln!("Failed to extract boolean result: {}", e);
                                false
                            }
                        }
                    }),
                    None => false,
                },
                _ => false,
            }
        })
        .collect();
    Ok(results)
}

#[pyfunction]
fn process_json_stream(lines: Vec<String>, field: &str, field_type: &str) -> PyResult<Vec<String>> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let results: Vec<String> = lines.par_iter() // Changed to par_iter()
        .filter_map(|line|  {
            eprintln!("Processing line: {}", line);
            let json: Value = match serde_json::from_str(line) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("Invalid JSON in process_json_stream: {} for line: {}", e, line);
                    return None;
                }
            };
            let value = json.get(field).and_then(|v| v.as_str());
            eprintln!("Field value for {}: {:?}", field, value);
            let valid = match field_type {
                "email" => value.map(is_valid_email).unwrap_or(false),
                "number" => value.map(is_valid_number).unwrap_or(false),
                "url" | "boolean" => match value {
                    Some(val) => Python::with_gil(|py| {
                        eprintln!("Validating {}: {}", field_type, val);
                        let module = match PyModule::import_bound(py, "pyrustpipe_rules") {
                            Ok(module) => module,
                            Err(e) => {
                                eprintln!("Failed to import pyrustpipe_rules: {}", e);
                                return false;
                            }
                        };
                        let rule_class = match module.getattr("Rule") {
                            Ok(rule_class) => rule_class,
                            Err(e) => {
                                eprintln!("Failed to get Rule class: {}", e);
                                return false;
                            }
                        };
                        let rule = match rule_class.call1((field_type,)) {
                            Ok(rule) => rule,
                            Err(e) => {
                                eprintln!("Failed to create Rule instance: {}", e);
                                return false;
                            }
                        };
                        let result = match rule.call_method1("validate", (val,)) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Failed to call validate method: {}", e);
                                return false;
                            }
                        };
                        match result.extract::<bool>() {
                            Ok(result) => {
                                eprintln!("Validation result for {}: {}", val, result);
                                result
                            }
                            Err(e) => {
                                eprintln!("Failed to extract boolean result: {}", e);
                                false
                            }
                        }
                    }),
                    None => false,
                },
                _ => return None,
            };
            if valid {
                match field_type {
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
                }
            } else {
                None
            }
        })
        .collect();
    Ok(results)
}

#[pyfunction]
fn process_json_stream_with_errors(lines: Vec<String>, field: &str, field_type: &str) -> PyResult<(Vec<String>, Vec<(usize, String, String)>)> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let results: Vec<String> = lines.par_iter() // Changed to par_iter()
        .filter_map(|line| {
            eprintln!("Processing line: {}", line);
            let json: Value = match serde_json::from_str(line) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("Invalid JSON in process_json_stream_with_errors: {} for line: {}", e, line);
                    return None;
                }
            };
            let value = json.get(field).and_then(|v| v.as_str());
            eprintln!("Field value for {}: {:?}", field, value);
            let valid = match field_type {
                "email" => value.map(is_valid_email).unwrap_or(false),
                "number" => value.map(is_valid_number).unwrap_or(false),
                "url" | "boolean" => match value {
                    Some(val) => Python::with_gil(|py| {
                        eprintln!("Validating {}: {}", field_type, val);
                        let module = match PyModule::import_bound(py, "pyrustpipe_rules") {
                            Ok(module) => module,
                            Err(e) => {
                                eprintln!("Failed to import pyrustpipe_rules: {}", e);
                                return false;
                            }
                        };
                        let rule_class = match module.getattr("Rule") {
                            Ok(rule_class) => rule_class,
                            Err(e) => {
                                eprintln!("Failed to get Rule class: {}", e);
                                return false;
                            }
                        };
                        let rule = match rule_class.call1((field_type,)) {
                            Ok(rule) => rule,
                            Err(e) => {
                                eprintln!("Failed to create Rule instance: {}", e);
                                return false;
                            }
                        };
                        let result = match rule.call_method1("validate", (val,)) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Failed to call validate method: {}", e);
                                return false;
                            }
                        };
                        match result.extract::<bool>() {
                            Ok(result) => {
                                eprintln!("Validation result for {}: {}", val, result);
                                result
                            }
                            Err(e) => {
                                eprintln!("Failed to extract boolean result: {}", e);
                                false
                            }
                        }
                    }),
                    None => false,
                },
                _ => return None,
            };
            if valid {
                match field_type {
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
                }
            } else {
                None
            }
        })
        .collect();
    let errors: Vec<(usize, String, String)> = lines.par_iter() // Changed to par_iter()
        .enumerate()
        .filter_map(|(index, line)| {
            eprintln!("Checking errors for line: {}", line);
            let json: Value = match serde_json::from_str(line) {
                Ok(json) => json,
                Err(e) => return Some((index, line.clone(), format!("Invalid JSON: {}", e))),
            };
            let value = json.get(field).and_then(|v| v.as_str());
            eprintln!("Error check field value for {}: {:?}", field, value);
            match field_type {
                "email" => match value {
                    Some(email) => if !is_valid_email(email) {
                        Some((index, line.clone(), format!("Invalid email: {}", email)))
                    } else {
                        None
                    },
                    None => Some((index, line.clone(), format!("No '{}' field", field))),
                },
                "number" => match value {
                    Some(number) => if !is_valid_number(number) {
                        Some((index, line.clone(), format!("Invalid number: {}", number)))
                    } else {
                        None
                    },
                    None => Some((index, line.clone(), format!("No '{}' field", field))),
                },
                "url" | "boolean" => match value {
                    Some(val) => Python::with_gil(|py| {
                        eprintln!("Validating for error {}: {}", field_type, val);
                        let module = match PyModule::import_bound(py, "pyrustpipe_rules") {
                            Ok(module) => module,
                            Err(e) => {
                                eprintln!("Failed to import pyrustpipe_rules: {}", e);
                                return Some((index, line.clone(), format!("Validation error: {}", e)));
                            }
                        };
                        let rule_class = match module.getattr("Rule") {
                            Ok(rule_class) => rule_class,
                            Err(e) => {
                                eprintln!("Failed to get Rule class: {}", e);
                                return Some((index, line.clone(), format!("Validation error: {}", e)));
                            }
                        };
                        let rule = match rule_class.call1((field_type,)) {
                            Ok(rule) => rule,
                            Err(e) => {
                                eprintln!("Failed to create Rule instance: {}", e);
                                return Some((index, line.clone(), format!("Validation error: {}", e)));
                            }
                        };
                        let result = match rule.call_method1("validate", (val,)) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Failed to call validate method: {}", e);
                                return Some((index, line.clone(), format!("Validation error: {}", e)));
                            }
                        };
                        match result.extract::<bool>() {
                            Ok(true) => None,
                            Ok(false) => Some((index, line.clone(), format!("Invalid {}: {}", field_type, val))),
                            Err(e) => {
                                eprintln!("Failed to extract boolean result: {}", e);
                                Some((index, line.clone(), format!("Validation error: {}", e)))
                            }
                        }
                    }),
                    None => Some((index, line.clone(), format!("No '{}' field", field))),
                },
                _ => Some((index, line.clone(), format!("Unsupported field type: {}", field_type))),
            }
        })
        .collect();
    Ok((results, errors))
}

#[pyfunction]
fn process_json_stream_batch(lines: Vec<String>, field_types: Vec<(String, String)>) -> PyResult<HashMap<String, Vec<String>>> {
    let url_re = Regex::new(r"^(https?)(://)([^/]+)(.*)$").unwrap();
    let results: HashMap<String, Vec<String>> = field_types.iter().map(|(_, ft)| (ft.clone(), Vec::new())).collect();
    let results = std::sync::Mutex::new(results);
    field_types.par_iter() // Changed to par_iter()
        .for_each(|(field, field_type)| {
            let mut field_results = Vec::new();
            for line in &lines {
                eprintln!("Batch processing line: {}", line);
                let json: Value = match serde_json::from_str(line) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Invalid JSON in process_json_stream_batch: {} for line: {}", e, line);
                        continue;
                    }
                };
                let value = json.get(field).and_then(|v| v.as_str());
                eprintln!("Batch field value for {}: {:?}", field, value);
                let valid = match field_type.as_str() {
                    "email" => value.map(is_valid_email).unwrap_or(false),
                    "number" => value.map(is_valid_number).unwrap_or(false),
                    "url" | "boolean" => match value {
                        Some(val) => Python::with_gil(|py| {
                            eprintln!("Batch validating {}: {}", field_type, val);
                            let module = match PyModule::import_bound(py, "pyrustpipe_rules") {
                                Ok(module) => module,
                                Err(e) => {
                                    eprintln!("Failed to import pyrustpipe_rules: {}", e);
                                    return false;
                                }
                            };
                            let rule_class = match module.getattr("Rule") {
                                Ok(rule_class) => rule_class,
                                Err(e) => {
                                    eprintln!("Failed to get Rule class: {}", e);
                                    return false;
                                }
                            };
                            let rule = match rule_class.call1((field_type.as_str(),)) {
                                Ok(rule) => rule,
                                Err(e) => {
                                    eprintln!("Failed to create Rule instance: {}", e);
                                    return false;
                                }
                            };
                            let result = match rule.call_method1("validate", (val,)) {
                                Ok(result) => result,
                                Err(e) => {
                                    eprintln!("Failed to call validate method: {}", e);
                                    return false;
                                }
                            };
                            match result.extract::<bool>() {
                                Ok(result) => {
                                    eprintln!("Validation result for {}: {}", val, result);
                                    result
                                }
                                Err(e) => {
                                    eprintln!("Failed to extract boolean result: {}", e);
                                    false
                                }
                            }
                        }),
                        None => false,
                    },
                    _ => return,
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
                        field_results.push(t);
                    }
                }
            }
            results.lock().unwrap().get_mut(field_type).unwrap().extend(field_results);
        });
    Ok(results.into_inner().unwrap())
}

#[pyfunction]
fn process_json_file(path: &str, field: &str, field_type: &str) -> PyResult<Vec<String>> {
    let file = File::open(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to open file: {}", e)))?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines()
        .filter_map(|l| l.ok())
        .collect();
    process_json_stream(lines, field, field_type)
}

#[pyfunction]
fn process_json_stream_iter(_py: Python, iter: &Bound<PyIterator>, field: &str, field_type: &str) -> PyResult<Vec<String>> {
    let lines: Vec<String> = iter.iter()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Iterator error: {}", e)))?
        .filter_map(|item| item.ok())
        .map(|item| item.to_string())
        .collect();
    process_json_stream(lines, field, field_type)
}

#[pymodule]
fn pyrustpipe(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(say_hello, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_email, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_number, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_url, m)?)?;
    m.add_function(wrap_pyfunction!(validate_json_field, m)?)?;
    m.add_function(wrap_pyfunction!(validate_json_stream, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream_with_errors, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream_batch, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_file, m)?)?;
    m.add_function(wrap_pyfunction!(process_json_stream_iter, m)?)?;
    Ok(())
}