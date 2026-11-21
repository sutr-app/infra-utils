/// Escape character used in LIKE patterns built by [`escape_like_pattern`].
///
/// A backslash is avoided on purpose: SQLite has no default escape character and
/// MySQL treats `'\'` as an unterminated literal in its default SQL mode, so a
/// non-backslash character is the only one accepted by both databases.
pub const LIKE_ESCAPE_CHAR: char = '!';

/// `ESCAPE` clause matching [`LIKE_ESCAPE_CHAR`].
pub const LIKE_ESCAPE_CLAUSE: &str = "ESCAPE '!'";

/// Build a `<column> LIKE ? ESCAPE '!'` predicate for patterns built by [`escape_like_pattern`].
///
/// `column` is embedded verbatim, so it must be a fixed identifier, never user input.
///
/// # Examples
///
/// ```
/// use infra_utils::infra::rdb::escape::like_clause;
///
/// assert_eq!(like_clause("name"), "name LIKE ? ESCAPE '!'");
/// ```
pub fn like_clause(column: &str) -> String {
    format!("{column} LIKE ? {LIKE_ESCAPE_CLAUSE}")
}

/// Escape LIKE wildcards (`%`, `_`) and the escape character itself with [`LIKE_ESCAPE_CHAR`].
///
/// The query must declare the escape character with [`like_clause`] or [`LIKE_ESCAPE_CLAUSE`].
///
/// # Examples
///
/// ```
/// use infra_utils::infra::rdb::escape::escape_like_pattern;
///
/// assert_eq!(escape_like_pattern("test%"), "test!%");
/// assert_eq!(escape_like_pattern("test_name"), "test!_name");
/// assert_eq!(escape_like_pattern("test!"), "test!!");
/// assert_eq!(escape_like_pattern("100%_off!sale"), "100!%!_off!!sale");
/// ```
pub fn escape_like_pattern(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for c in input.chars() {
        if matches!(c, LIKE_ESCAPE_CHAR | '%' | '_') {
            escaped.push(LIKE_ESCAPE_CHAR);
        }
        escaped.push(c);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_percent() {
        assert_eq!(escape_like_pattern("test%"), "test!%");
        assert_eq!(escape_like_pattern("%test"), "!%test");
        assert_eq!(escape_like_pattern("te%st"), "te!%st");
    }

    #[test]
    fn test_escape_underscore() {
        assert_eq!(escape_like_pattern("test_name"), "test!_name");
        assert_eq!(escape_like_pattern("_test"), "!_test");
        assert_eq!(escape_like_pattern("te_st"), "te!_st");
    }

    #[test]
    fn test_escape_escape_char() {
        assert_eq!(escape_like_pattern("test!"), "test!!");
        assert_eq!(escape_like_pattern("!test"), "!!test");
        assert_eq!(escape_like_pattern("!%"), "!!!%");
    }

    #[test]
    fn test_backslash_is_not_special() {
        assert_eq!(escape_like_pattern("te\\st"), "te\\st");
    }

    #[test]
    fn test_escape_combined() {
        assert_eq!(escape_like_pattern("100%_off!sale"), "100!%!_off!!sale");
        assert_eq!(escape_like_pattern("a%b_c!d"), "a!%b!_c!!d");
    }

    #[test]
    fn test_escape_empty_and_plain() {
        assert_eq!(escape_like_pattern(""), "");
        assert_eq!(escape_like_pattern("test123"), "test123");
        assert_eq!(escape_like_pattern("日本語"), "日本語");
    }

    #[test]
    fn test_clause_uses_escape_char() {
        assert_eq!(LIKE_ESCAPE_CLAUSE, format!("ESCAPE '{LIKE_ESCAPE_CHAR}'"));
        assert_eq!(like_clause("name"), "name LIKE ? ESCAPE '!'");
    }
}
