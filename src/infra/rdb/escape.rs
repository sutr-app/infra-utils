/// Escape special characters in LIKE pattern for SQL queries
///
/// Escapes the following characters:
/// - `\` -> `\\`
/// - `%` -> `\%`
/// - `_` -> `\_`
///
/// # Examples
///
/// ```
/// use infra_utils::infra::rdb::escape::escape_like_pattern;
///
/// assert_eq!(escape_like_pattern("test%"), "test\\%");
/// assert_eq!(escape_like_pattern("test_name"), "test\\_name");
/// assert_eq!(escape_like_pattern("test\\"), "test\\\\");
/// assert_eq!(escape_like_pattern("100%_off\\sale"), "100\\%\\_off\\\\sale");
/// ```
pub fn escape_like_pattern(input: &str) -> String {
    input
        .replace('\\', "\\\\") // Backslash must be escaped first
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_percent() {
        assert_eq!(escape_like_pattern("test%"), "test\\%");
        assert_eq!(escape_like_pattern("%test"), "\\%test");
        assert_eq!(escape_like_pattern("te%st"), "te\\%st");
    }

    #[test]
    fn test_escape_underscore() {
        assert_eq!(escape_like_pattern("test_name"), "test\\_name");
        assert_eq!(escape_like_pattern("_test"), "\\_test");
        assert_eq!(escape_like_pattern("te_st"), "te\\_st");
    }

    #[test]
    fn test_escape_backslash() {
        assert_eq!(escape_like_pattern("test\\"), "test\\\\");
        assert_eq!(escape_like_pattern("\\test"), "\\\\test");
        assert_eq!(escape_like_pattern("te\\st"), "te\\\\st");
    }

    #[test]
    fn test_escape_combined() {
        assert_eq!(
            escape_like_pattern("100%_off\\sale"),
            "100\\%\\_off\\\\sale"
        );
        assert_eq!(escape_like_pattern("a%b_c\\d"), "a\\%b\\_c\\\\d");
    }

    #[test]
    fn test_escape_empty_string() {
        assert_eq!(escape_like_pattern(""), "");
    }

    #[test]
    fn test_escape_no_special_chars() {
        assert_eq!(escape_like_pattern("normal_text"), "normal\\_text");
        assert_eq!(escape_like_pattern("test123"), "test123");
    }

    #[test]
    fn test_escape_order_matters() {
        // Backslash must be escaped first to avoid double-escaping
        let input = "\\%";
        let expected = "\\\\\\%"; // \\ and \%
        assert_eq!(escape_like_pattern(input), expected);
    }
}
