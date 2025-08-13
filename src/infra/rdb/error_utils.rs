use sqlx::error::DatabaseError;

/// A trait for checking specific database error types
pub trait DatabaseErrorExt {
    /// Check if the database error is a unique constraint violation
    /// Returns true for SQLite UNIQUE constraint violations and MySQL duplicate entry errors
    fn is_unique_constraint_error(&self) -> bool;

    /// Check if the database error is a foreign key constraint violation
    fn is_foreign_key_constraint_error(&self) -> bool;

    /// Check if the database error is a not null constraint violation  
    fn is_not_null_constraint_error(&self) -> bool;
}

impl DatabaseErrorExt for &dyn DatabaseError {
    fn is_unique_constraint_error(&self) -> bool {
        // SQLite UNIQUE constraint violation
        #[cfg(not(any(feature = "mysql", feature = "postgres")))]
        {
            if let Some(sqlite_err) = self.try_downcast_ref::<sqlx::sqlite::SqliteError>() {
                return sqlite_err.code().as_deref() == Some("2067"); // SQLITE_CONSTRAINT_UNIQUE
            }
        }

        // MySQL duplicate entry error
        #[cfg(feature = "mysql")]
        {
            if let Some(mysql_err) = self.try_downcast_ref::<sqlx::mysql::MySqlError>() {
                return mysql_err.number() == 1062; // ER_DUP_ENTRY
            }
        }

        // PostgreSQL unique violation
        #[cfg(all(feature = "postgres", not(feature = "mysql")))]
        {
            if let Some(pg_err) = self.try_downcast_ref::<sqlx::postgres::PgError>() {
                return pg_err.code().code() == "23505"; // unique_violation
            }
        }

        false
    }

    fn is_foreign_key_constraint_error(&self) -> bool {
        // SQLite FOREIGN KEY constraint violation
        #[cfg(not(any(feature = "mysql", feature = "postgres")))]
        {
            if let Some(sqlite_err) = self.try_downcast_ref::<sqlx::sqlite::SqliteError>() {
                return sqlite_err.code().as_deref() == Some("2067") // SQLITE_CONSTRAINT_FOREIGNKEY
                    && sqlite_err.message().contains("FOREIGN KEY");
            }
        }

        // MySQL foreign key constraint error
        #[cfg(feature = "mysql")]
        {
            if let Some(mysql_err) = self.try_downcast_ref::<sqlx::mysql::MySqlError>() {
                return mysql_err.number() == 1452; // ER_NO_REFERENCED_ROW_2
            }
        }

        // PostgreSQL foreign key violation
        #[cfg(all(feature = "postgres", not(feature = "mysql")))]
        {
            if let Some(pg_err) = self.try_downcast_ref::<sqlx::postgres::PgError>() {
                return pg_err.code().code() == "23503"; // foreign_key_violation
            }
        }

        false
    }

    fn is_not_null_constraint_error(&self) -> bool {
        // SQLite NOT NULL constraint violation
        #[cfg(not(any(feature = "mysql", feature = "postgres")))]
        {
            if let Some(sqlite_err) = self.try_downcast_ref::<sqlx::sqlite::SqliteError>() {
                return sqlite_err.code().as_deref() == Some("1299"); // SQLITE_CONSTRAINT_NOTNULL
            }
        }

        // MySQL not null constraint error
        #[cfg(feature = "mysql")]
        {
            if let Some(mysql_err) = self.try_downcast_ref::<sqlx::mysql::MySqlError>() {
                return mysql_err.number() == 1048; // ER_BAD_NULL_ERROR
            }
        }

        // PostgreSQL not null violation
        #[cfg(all(feature = "postgres", not(feature = "mysql")))]
        {
            if let Some(pg_err) = self.try_downcast_ref::<sqlx::postgres::PgError>() {
                return pg_err.code().code() == "23502"; // not_null_violation
            }
        }

        false
    }
}

/// Convenience function for checking unique constraint errors
/// This function is provided for backward compatibility and easy usage
pub fn is_unique_constraint_error(db_err: &dyn DatabaseError) -> bool {
    db_err.is_unique_constraint_error()
}

/// Convenience function for checking foreign key constraint errors
pub fn is_foreign_key_constraint_error(db_err: &dyn DatabaseError) -> bool {
    db_err.is_foreign_key_constraint_error()
}

/// Convenience function for checking not null constraint errors
pub fn is_not_null_constraint_error(db_err: &dyn DatabaseError) -> bool {
    db_err.is_not_null_constraint_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_utils_trait_available() {
        // This test just ensures the trait is available and compiles
        // Real testing would require actual database errors, which is complex to set up
        let test_fn = |db_err: &dyn DatabaseError| {
            let _ = db_err.is_unique_constraint_error();
            let _ = db_err.is_foreign_key_constraint_error();
            let _ = db_err.is_not_null_constraint_error();
        };
        // Just ensure the function compiles
        let _ = test_fn;
    }
}
