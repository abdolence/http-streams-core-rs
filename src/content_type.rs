//! A deliberately tiny media-type parser.
//!
//! Only what content-type negotiation for streamed bodies actually needs: the essence
//! (`type/subtype`) with parameters stripped, compared case-insensitively, plus awareness of
//! the structured-suffix convention (`application/cloudevents+json`). Not the `mime` crate:
//! neither dependent crate pulls it in today, and this is twenty lines.

/// A parsed `Content-Type` header value, borrowed from the header it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentType<'a> {
    essence: &'a str,
}

impl<'a> ContentType<'a> {
    /// Parse a raw header value, discarding any parameters (`; charset=utf-8` and friends).
    ///
    /// Never fails: a value this cannot make sense of simply matches nothing.
    pub fn parse(raw: &'a str) -> Self {
        let essence = raw.split(';').next().unwrap_or("").trim();
        Self { essence }
    }

    /// The `type/subtype`, with parameters and surrounding whitespace removed.
    pub fn essence(&self) -> &'a str {
        self.essence
    }

    /// Whether the essence equals `other`, ignoring ASCII case.
    ///
    /// RFC 9110 makes type and subtype case-insensitive, so `APPLICATION/JSON` matches.
    pub fn matches(&self, other: &str) -> bool {
        self.essence.eq_ignore_ascii_case(other)
    }

    /// Whether the essence equals any of `candidates`, ignoring ASCII case.
    pub fn matches_any(&self, candidates: &[&str]) -> bool {
        candidates.iter().any(|c| self.matches(c))
    }

    /// The part after `/`, e.g. `cloudevents+json` for `application/cloudevents+json`.
    pub fn subtype(&self) -> &'a str {
        match self.essence.split_once('/') {
            Some((_, sub)) => sub,
            None => "",
        }
    }

    /// Whether the subtype carries the structured suffix `+<suffix>`.
    ///
    /// `application/cloudevents+json` has the `json` suffix; `application/json` does not.
    pub fn has_suffix(&self, suffix: &str) -> bool {
        match self.subtype().rsplit_once('+') {
            Some((_, s)) => s.eq_ignore_ascii_case(suffix),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_parameters_and_whitespace() {
        assert_eq!(
            ContentType::parse("application/json").essence(),
            "application/json"
        );
        assert_eq!(
            ContentType::parse("application/json; charset=utf-8").essence(),
            "application/json"
        );
        assert_eq!(
            ContentType::parse("application/json;charset=utf-8").essence(),
            "application/json"
        );
        assert_eq!(ContentType::parse("  text/csv  ").essence(), "text/csv");
    }

    #[test]
    fn matching_ignores_case() {
        assert!(ContentType::parse("APPLICATION/JSON").matches("application/json"));
        assert!(ContentType::parse("application/json").matches("Application/Json"));
        assert!(!ContentType::parse("text/json").matches("application/json"));
    }

    #[test]
    fn matches_any_of_a_set() {
        let ct = ContentType::parse("application/x-ndjson");
        assert!(ct.matches_any(&["application/jsonstream", "application/x-ndjson"]));
        assert!(!ct.matches_any(&["application/json", "text/csv"]));
    }

    #[test]
    fn structured_suffix() {
        assert!(ContentType::parse("application/cloudevents+json").has_suffix("json"));
        assert!(ContentType::parse("application/vnd.foo+JSON").has_suffix("json"));
        assert!(!ContentType::parse("application/json").has_suffix("json"));
        assert!(!ContentType::parse("text/csv").has_suffix("json"));
    }

    #[test]
    fn nonsense_matches_nothing() {
        let ct = ContentType::parse("");
        assert_eq!(ct.essence(), "");
        assert_eq!(ct.subtype(), "");
        assert!(!ct.matches("application/json"));
        assert!(!ct.has_suffix("json"));
    }
}
