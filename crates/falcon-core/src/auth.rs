//! Shared secret comparison, used identically by both protocol front-ends.
//!
//! This lives in `falcon-core` rather than in each front-end because a
//! security primitive with two copies is a primitive with two chances to be
//! fixed in only one place — the REST and wire servers previously carried
//! byte-identical private implementations of the function below.

/// Compare a presented token against the configured one without leaking, via
/// timing, *which byte* first differed.
///
/// The loop always visits every byte of the pair rather than returning at the
/// first mismatch, so the comparison's duration does not reveal how much of a
/// guessed prefix was correct — the signal that makes a token brute-forceable
/// one byte at a time.
///
/// The early length check does leak the *length* of the configured token. That
/// is standard for this construction and not worth avoiding: length alone does
/// not narrow the search space meaningfully, and hiding it would require
/// hashing both sides first.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Percent-decode a URL query-parameter value.
///
/// Needed because a key containing `+`, `/`, or `=` — all of which appear in
/// base64-ish secrets — arrives percent-encoded, and comparing the still-encoded
/// form against the raw configured token rejects a correct key. Invalid escapes
/// are passed through verbatim rather than erroring: this feeds a constant-time
/// comparison that will reject them anyway.
pub fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // `+` means space in a query string (application/x-www-form-urlencoded).
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                match (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    (Some(hi), Some(lo)) => {
                        out.push(hi << 4 | lo);
                        i += 3;
                    }
                    // Not a valid escape — keep the '%' as a literal.
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    // A token that is not valid UTF-8 cannot match a `String` config value, so
    // lossy conversion here cannot turn a rejection into an acceptance.
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_tokens_match() {
        assert!(constant_time_eq(b"s3cret", b"s3cret"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn different_tokens_do_not_match() {
        assert!(!constant_time_eq(b"s3cret", b"s3creT"));
        assert!(!constant_time_eq(b"s3cret", b"wrong!"));
        // A correct prefix must not be accepted.
        assert!(!constant_time_eq(b"s3cret", b"s3c"));
        assert!(!constant_time_eq(b"s3c", b"s3cret"));
    }

    #[test]
    fn empty_presented_token_never_matches_a_configured_one() {
        // The "auth enabled but client sent nothing" path.
        assert!(!constant_time_eq(b"", b"s3cret"));
    }

    #[test]
    fn percent_decode_restores_special_characters() {
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("a%2bb"), "a+b");
        assert_eq!(percent_decode("a%3D"), "a=");
        assert_eq!(percent_decode("a+b"), "a b");
    }

    #[test]
    fn percent_decode_passes_through_malformed_escapes() {
        // Truncated or non-hex escapes must not panic or silently drop bytes.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%a"), "%a");
    }

    /// The bug this pair guards: a base64-ish key sent through a browser
    /// arrives percent-encoded, and comparing the encoded form to the raw
    /// configured token rejected a correct key.
    #[test]
    fn encoded_key_matches_configured_token_after_decoding() {
        let configured = "abc+def/ghi=";
        let as_sent_by_browser = "abc%2Bdef%2Fghi%3D";
        assert!(!constant_time_eq(
            as_sent_by_browser.as_bytes(),
            configured.as_bytes()
        ));
        assert!(constant_time_eq(
            percent_decode(as_sent_by_browser).as_bytes(),
            configured.as_bytes()
        ));
    }
}
