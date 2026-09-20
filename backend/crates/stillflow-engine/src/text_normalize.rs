//! Deterministic text normalization for `Rule::NormalizeText` (NX-N2 / #366).
//!
//! Every operation is a pure function over `&str`. They are engine-owned
//! deterministic code: no user script, closure, pattern language or external
//! input ever reaches them. NULL is preserved by the caller (a NULL value never
//! enters this module and never becomes a value), and non-`Utf8` columns are
//! rejected before execution by the shared semantic analyzer.

use stillflow_plan::TextOperation;
use unicode_normalization::UnicodeNormalization;

/// Applies one normalization operation to one value.
pub(crate) fn normalize(value: &str, operation: TextOperation) -> String {
    match operation {
        TextOperation::Trim => value.trim().to_owned(),
        TextOperation::CollapseWhitespace => collapse_whitespace(value),
        TextOperation::Lowercase => value.to_lowercase(),
        TextOperation::Uppercase => value.to_uppercase(),
        TextOperation::UnicodeNfc => value.nfc().collect(),
    }
}

/// Replaces every maximal run of Unicode whitespace with one ASCII space.
///
/// Leading and trailing runs also become a single space; callers that want the
/// ends removed chain a `Trim` operation as well. The definition is
/// reproducible: `char::is_whitespace` decides what counts as whitespace, and
/// the replacement is always exactly one `U+0020`.
fn collapse_whitespace(value: &str) -> String {
    let mut collapsed = String::with_capacity(value.len());
    let mut in_run = false;
    for character in value.chars() {
        if character.is_whitespace() {
            if !in_run {
                collapsed.push(' ');
                in_run = true;
            }
        } else {
            collapsed.push(character);
            in_run = false;
        }
    }
    collapsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_removes_leading_and_trailing_whitespace_only() {
        assert_eq!(normalize("  a b  ", TextOperation::Trim), "a b");
        assert_eq!(normalize("\t\nx\r\n", TextOperation::Trim), "x");
        assert_eq!(normalize("", TextOperation::Trim), "");
        assert_eq!(normalize("中文 空格", TextOperation::Trim), "中文 空格");
    }

    #[test]
    fn collapse_whitespace_is_reproducible_and_unicode_aware() {
        assert_eq!(
            normalize("a   b\t\tc", TextOperation::CollapseWhitespace),
            "a b c"
        );
        // Leading and trailing runs collapse to one space, not zero.
        assert_eq!(normalize("  a  ", TextOperation::CollapseWhitespace), " a ");
        // A full-width ideographic space is whitespace too.
        assert_eq!(
            normalize("x\u{3000}\u{3000}y", TextOperation::CollapseWhitespace),
            "x y"
        );
        // Already collapsed input is a fixpoint.
        let once = normalize("a b", TextOperation::CollapseWhitespace);
        assert_eq!(normalize(&once, TextOperation::CollapseWhitespace), once);
    }

    #[test]
    fn case_operations_are_unicode_aware() {
        assert_eq!(normalize("AbC", TextOperation::Lowercase), "abc");
        assert_eq!(normalize("AbC", TextOperation::Uppercase), "ABC");
        // Non-ASCII letters participate.
        assert_eq!(normalize("ÄÖÜ", TextOperation::Lowercase), "äöü");
        assert_eq!(normalize("straße", TextOperation::Uppercase), "STRASSE");
        // Digits and punctuation are untouched.
        assert_eq!(normalize("a-1_2", TextOperation::Uppercase), "A-1_2");
    }

    #[test]
    fn unicode_nfc_composes_decomposed_sequences() {
        // "e" + combining acute accent -> single precomposed character.
        let decomposed = "e\u{0301}";
        let normalized = normalize(decomposed, TextOperation::UnicodeNfc);
        assert_eq!(normalized, "\u{00e9}");
        assert_eq!(normalized.chars().count(), 1);

        // Already-composed input is unchanged, including CJK.
        assert_eq!(normalize("\u{00e9}", TextOperation::UnicodeNfc), "\u{00e9}");
        assert_eq!(normalize("中文", TextOperation::UnicodeNfc), "中文");

        // Normalization is idempotent.
        assert_eq!(
            normalize(&normalized, TextOperation::UnicodeNfc),
            normalized
        );
    }

    #[test]
    fn empty_string_is_preserved_by_every_operation() {
        for operation in [
            TextOperation::Trim,
            TextOperation::CollapseWhitespace,
            TextOperation::Lowercase,
            TextOperation::Uppercase,
            TextOperation::UnicodeNfc,
        ] {
            assert_eq!(normalize("", operation), "");
        }
    }

    #[test]
    fn whitespace_only_input_rules_are_defined() {
        // Trim removes it entirely; collapse turns it into a single space.
        assert_eq!(normalize("   ", TextOperation::Trim), "");
        assert_eq!(normalize("   ", TextOperation::CollapseWhitespace), " ");
    }
}
