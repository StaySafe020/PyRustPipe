#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_valid_email() {
        assert!(is_valid_email("test@example.com"));
        assert!(!is_valid_email("invalid.email"));
    }
    #[test]
    fn test_normalize_url() {
        assert_eq!(normalize_url("HTTPS://Example.COM/path"), "https://example.com/path");
    }
}