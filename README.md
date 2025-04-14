# PyRustPipe

PyRustPipe is a Python-Rust library for fast and reliable data validation and transformation pipelines. It combines Python’s ease of use for defining rules with Rust’s performance for processing data, ideal for engineers and data scientists handling large datasets.

## Status
This project is in early development. Current features:
- **Module 1**: Basic Python-Rust integration, allowing Python to call Rust functions.
- **Module 2**: Email validation using a regex in Rust, callable from Python.

Future plans include a Python `Rule` class, JSON processing, and parallel pipelines.

## Installation
PyRustPipe requires Python 3.12+ and Rust 1.86.0+. It’s tested on Ubuntu and designed for Windows compatibility.

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/PyRustPipe.git
   cd PyRustPipe