//! What the machine actually gives this process — memory and cores.
//!
//! A cache whose size is a fixed number in a config file is wrong on almost
//! every machine it runs on: 256 MB wastes a 64 GB host and gets OOM-killed in
//! a 512 MB container. So when the operator has not named a capacity, Falcon
//! derives one from the ceiling it is actually running under.
//!
//! Detection is deliberately *policy* and lives here, not in the cache engine.
//! `falcon-storage` receives resolved numbers and reads nothing from the
//! environment, which keeps it a pure, deterministic, testable data structure.
//!
//! Everything here is plain file reads — cgroup limits are files — so no
//! dependency is needed to ask the kernel how much memory this process may use.

/// Where a detected memory ceiling came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySource {
    /// A cgroup v2 container limit (`/sys/fs/cgroup/memory.max`).
    CgroupV2,
    /// A cgroup v1 container limit.
    CgroupV1,
    /// Total host RAM — no container limit applies.
    HostTotal,
}

impl MemorySource {
    pub fn as_str(self) -> &'static str {
        match self {
            MemorySource::CgroupV2 => "cgroup-v2",
            MemorySource::CgroupV1 => "cgroup-v1",
            MemorySource::HostTotal => "host-total",
        }
    }
}

/// A detected memory ceiling and where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimit {
    pub bytes: u64,
    pub source: MemorySource,
}

/// Where the capacity actually in force came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacitySource {
    /// The operator set it; detection was not consulted.
    Explicit,
    /// Derived from a detected memory ceiling.
    Auto(MemorySource),
    /// Nothing could be detected, so the built-in default applies.
    Fallback,
}

impl CapacitySource {
    pub fn as_str(self) -> &'static str {
        match self {
            CapacitySource::Explicit => "explicit",
            CapacitySource::Auto(s) => s.as_str(),
            CapacitySource::Fallback => "fallback-default",
        }
    }
}

/// The capacity a cache will actually be built with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedCapacity {
    pub bytes: u64,
    pub source: CapacitySource,
}

/// Used when nothing can be detected and the operator said nothing.
pub const FALLBACK_CAPACITY_MB: usize = 256;

/// Never auto-size below this — under it a cache is not worth running.
const MIN_AUTO_CAPACITY: u64 = 64 * 1024 * 1024;

/// Memory left to everything that is not cached entries: the async runtime,
/// TLS and connection buffers, and allocator slack. The engine charges an
/// *estimated* per-entry overhead rather than measuring RSS, so this headroom
/// also absorbs that estimate being low.
const MIN_HEADROOM: u64 = 256 * 1024 * 1024;

/// Share of the detected ceiling a cache may use, when headroom allows.
const AUTO_FRACTION_PERCENT: u64 = 70;

/// Logical CPUs available to this process. Falls back to 1.
pub fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The memory ceiling this process lives under.
///
/// A container limit wins over host RAM: inside a 512 MB container the host's
/// 64 GB is irrelevant, and sizing to it is exactly how a cache gets
/// OOM-killed. Returns `None` when nothing can be determined — notably on
/// macOS, which has no `/proc`; a dev box then gets the predictable default.
pub fn detect_memory_limit() -> Option<MemoryLimit> {
    let host = host_total_bytes();

    // cgroup v2, then v1. Both may report "unlimited", in which case the host
    // total is the real ceiling.
    let cgroup = read_first(&[
        ("/sys/fs/cgroup/memory.max", MemorySource::CgroupV2),
        (
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            MemorySource::CgroupV1,
        ),
    ]);

    if let Some((bytes, source)) = cgroup {
        // An "unlimited" cgroup reports a sentinel near u64::MAX. Rejecting
        // anything above host RAM catches that and every other absurd reading
        // in one rule — without it, an unconstrained host would auto-size the
        // cache to exabytes.
        let plausible = host.map(|h| bytes <= h).unwrap_or(true);
        if plausible {
            return Some(MemoryLimit { bytes, source });
        }
    }

    host.map(|bytes| MemoryLimit {
        bytes,
        source: MemorySource::HostTotal,
    })
}

fn read_first(candidates: &[(&str, MemorySource)]) -> Option<(u64, MemorySource)> {
    for (path, source) in candidates {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Some(bytes) = parse_limit(&text) {
                return Some((bytes, *source));
            }
        }
    }
    None
}

/// Parse a cgroup limit file: a decimal byte count, or `max` for unlimited.
fn parse_limit(text: &str) -> Option<u64> {
    let t = text.trim();
    if t == "max" {
        return None;
    }
    t.parse::<u64>().ok().filter(|&b| b > 0)
}

/// Total host RAM from `/proc/meminfo`. `None` where that file doesn't exist.
fn host_total_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    parse_meminfo_total(&text)
}

/// Pull `MemTotal` (reported in kB) out of `/proc/meminfo`.
fn parse_meminfo_total(text: &str) -> Option<u64> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Turn a detected ceiling into a cache budget.
///
/// Two constraints, whichever binds harder: a share of the ceiling, and a fixed
/// headroom reserve. The share alone is wrong on small containers (70% of
/// 512 MB leaves only 154 MB for everything else); the reserve alone is wrong
/// on large ones (leaving 256 MB of a 64 GB box would hand the cache 99.6%).
/// Returns `None` when the ceiling is too small to carve a useful cache out of.
pub fn auto_capacity_bytes(limit: Option<MemoryLimit>) -> Option<u64> {
    let limit = limit?.bytes;
    let by_fraction = limit / 100 * AUTO_FRACTION_PERCENT;
    let by_headroom = limit.checked_sub(MIN_HEADROOM)?;
    let capacity = by_fraction.min(by_headroom);
    (capacity >= MIN_AUTO_CAPACITY).then_some(capacity)
}

/// Resolve the capacity a cache should be built with.
///
/// An explicit setting always wins — auto-sizing is what happens in its
/// absence, never something that overrides it.
pub fn resolve_capacity(explicit_mb: Option<usize>) -> ResolvedCapacity {
    if let Some(mb) = explicit_mb {
        return ResolvedCapacity {
            bytes: (mb as u64) * 1024 * 1024,
            source: CapacitySource::Explicit,
        };
    }
    let limit = detect_memory_limit();
    match auto_capacity_bytes(limit) {
        // `limit` is Some whenever auto_capacity_bytes returned Some.
        Some(bytes) => ResolvedCapacity {
            bytes,
            source: CapacitySource::Auto(limit.expect("detected").source),
        },
        None => ResolvedCapacity {
            bytes: (FALLBACK_CAPACITY_MB as u64) * 1024 * 1024,
            source: CapacitySource::Fallback,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * MB;

    fn limit(bytes: u64) -> Option<MemoryLimit> {
        Some(MemoryLimit {
            bytes,
            source: MemorySource::CgroupV2,
        })
    }

    #[test]
    fn parses_cgroup_limits() {
        assert_eq!(parse_limit("536870912\n"), Some(512 * MB));
        assert_eq!(parse_limit("  1073741824  "), Some(GB));
        // "max" means unlimited, not a number.
        assert_eq!(parse_limit("max\n"), None);
        assert_eq!(parse_limit("garbage"), None);
        assert_eq!(parse_limit(""), None);
        assert_eq!(parse_limit("0"), None);
    }

    #[test]
    fn parses_meminfo() {
        let text = "MemTotal:       16384000 kB\nMemFree:         100 kB\n";
        assert_eq!(parse_meminfo_total(text), Some(16_384_000 * 1024));
        assert_eq!(parse_meminfo_total("MemFree: 100 kB\n"), None);
    }

    #[test]
    fn auto_capacity_leaves_headroom_on_small_limits() {
        // 70% of 512 MB would be 358 MB, leaving too little for the runtime;
        // the headroom reserve binds instead.
        assert_eq!(auto_capacity_bytes(limit(512 * MB)), Some(256 * MB));
    }

    #[test]
    fn auto_capacity_takes_the_fraction_on_large_limits() {
        // With plenty spare, the 70% share is what binds.
        assert_eq!(auto_capacity_bytes(limit(4 * GB)), Some(4 * GB / 100 * 70));
        assert_eq!(
            auto_capacity_bytes(limit(64 * GB)),
            Some(64 * GB / 100 * 70)
        );
    }

    #[test]
    fn auto_capacity_declines_when_there_is_too_little_memory() {
        // Nothing useful can be carved out of these.
        assert_eq!(auto_capacity_bytes(limit(128 * MB)), None);
        assert_eq!(auto_capacity_bytes(limit(64 * MB)), None);
        assert_eq!(auto_capacity_bytes(None), None);
    }

    #[test]
    fn auto_capacity_never_exceeds_its_limit() {
        for l in [300 * MB, 512 * MB, GB, 8 * GB, 512 * GB] {
            if let Some(c) = auto_capacity_bytes(limit(l)) {
                assert!(c < l, "auto capacity {c} must stay under the {l} ceiling");
            }
        }
    }

    #[test]
    fn explicit_capacity_always_wins() {
        // Whatever the machine looks like, a set value is used verbatim.
        let r = resolve_capacity(Some(64));
        assert_eq!(r.bytes, 64 * MB);
        assert_eq!(r.source, CapacitySource::Explicit);

        // Including a value that happens to equal the fallback default.
        let r = resolve_capacity(Some(FALLBACK_CAPACITY_MB));
        assert_eq!(r.source, CapacitySource::Explicit);
    }

    #[test]
    fn unset_capacity_resolves_to_something_usable() {
        // Auto on Linux, fallback on a platform with no /proc — either way a
        // sane, non-zero budget.
        let r = resolve_capacity(None);
        assert!(r.bytes >= MIN_AUTO_CAPACITY);
        assert_ne!(r.source, CapacitySource::Explicit);
    }
}
