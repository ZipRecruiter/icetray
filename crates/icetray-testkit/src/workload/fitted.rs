//! Synthetic workloads fitted to measured ones, for a benchmark run without
//! the traces: each key read an exact number of times, from a measured
//! curve of cumulative lookups against the share of keys read, the reads
//! dealt into requests whose key counts come from a measured histogram,
//! and a request's keys made alike by a Gaussian copula.
//!
//! The draw: key `k` of `K` is read `c_k = round(L·S(k/K)) −
//! round(L·S((k−1)/K))` times, `S` the curve and `L` the lookups, so the
//! counts sum to `L` and a key whose count rounds to zero is never read.
//! Those `L` reads form a pool ordered by rank. Each request draws `z`
//! from a standard normal, and each of its slots draws `e` and takes the
//! read at quantile `Φ(√ρ·z + √(1−ρ)·e)` of what remains of the pool,
//! removing it, until the pool is empty. `√ρ·z + √(1−ρ)·e` is a standard
//! normal whatever `z`, so every key is read exactly its count, a key read
//! once is never read again, as in a trace, and `ρ` alone determines how
//! alike one request's keys are: at zero they are independent draws, and
//! toward one a request's keys are all hot or all cold together, as
//! measured requests are.
//!
//! The measured workloads, from the busiest hour of one day of production
//! traffic, 2026-10-28, are described by shape and numbers alone.

use rand::RngExt as _;
use rand::SeedableRng as _;
use rand::rngs::StdRng;

use super::{Requests, WorkloadFault};
use crate::error::Result;
use icetray_core::approx;

/// A measured curve of a workload's keys: points of (share of the keys
/// read, hottest first; share of the lookups those keys take), rising to
/// (1, 1). Between points the share is interpolated linearly in log-log
/// space, so a segment is a power law and the curve rises monotonically;
/// below the first point the keys take equal shares.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RankCurve {
    points: &'static [(f64, f64)],
}

impl RankCurve {
    /// The share of the lookups the hottest `keys` share of the keys read
    /// takes, `keys` in [0, 1].
    pub fn share(self, keys: f64) -> f64 {
        let (first_keys, first_share) = self.points[0];
        if keys <= first_keys {
            return first_share * keys / first_keys;
        }
        for pair in self.points.windows(2) {
            let [(x0, y0), (x1, y1)] = [pair[0], pair[1]];
            if keys <= x1 {
                let slope = (y1 / y0).ln() / (x1 / x0).ln();
                return y0 * (keys / x0).powf(slope);
            }
        }
        1.0
    }
}

/// Job embeddings of about 4.2 KB read in batches by a ranking service
/// for the candidates it scores: the busiest hour's curve over 4,770,350
/// keys and 111,821,327 lookups, thinned from 113 log-spaced points to 28,
/// with the shares at 0.01, 0.1, 1, 10, and 50 percent of the keys within
/// 0.3 points of the hour's (2.98, 13.4, 27.8, 72.3, and 97.1 percent). A
/// flat head of about 1,000 keys, one list returned to many requests, then
/// Zipf of exponent about 0.52 over ranks 5,000 to 500,000, then a steep
/// tail to the 34 percent of keys read once, which take 1.5 percent of the
/// lookups.
pub const EMBEDDING_CURVE: RankCurve = RankCurve {
    points: &[
        (2.096_28e-07, 0.000_110_417),
        (1.048_14e-06, 0.000_506_549),
        (5.450_33e-06, 0.002_032_81),
        (9.433_27e-06, 0.003_276_88),
        (1.656_06e-05, 0.005_433_84),
        (4.465_08e-05, 0.013_737_6),
        (0.000_210_676, 0.061_099_2),
        (0.000_279_434, 0.076_512_7),
        (0.000_370_623, 0.092_189_9),
        (0.000_491_369, 0.106_895),
        (0.000_651_525, 0.119_307),
        (0.002_670_66, 0.175_096),
        (0.004_077_9, 0.200_057),
        (0.006_226_38, 0.231_842),
        (0.012_606_6, 0.305_708),
        (0.059_508_4, 0.595_342),
        (0.104_632, 0.733_687),
        (0.159_763, 0.828_896),
        (0.243_943, 0.902_646),
        (0.372_479, 0.949_927),
        (0.658_111, 0.985_415),
        (0.675_206, 0.986_144),
        (0.709_395, 0.987_603),
        (0.760_678, 0.989_79),
        (0.829_056, 0.992_707),
        (0.897_433, 0.995_624),
        (0.948_717, 0.997_812),
        (1.0, 1.0),
    ],
};

/// Per-user profile embeddings of about 4.2 KB, one Get per ranking
/// request for the user it ranks for: the busiest hour's curve over
/// 3,310,542 users and 5,123,466 requests, thinned to 30 points with the
/// same five shares within 0.1 points of the hour's (1.50, 3.82, 10.8,
/// 32.4, and 67.7 percent). Zipf of exponent about 0.55 to the 75 percent
/// of users read once, who take 49 percent of the lookups.
pub const PER_USER_CURVE: RankCurve = RankCurve {
    points: &[
        (3.020_65e-07, 0.000_435_252),
        (1.208_26e-06, 0.001_194_5),
        (1.812_39e-06, 0.001_513_82),
        (3.020_65e-06, 0.002_099_95),
        (5.437_18e-06, 0.002_988_41),
        (1.631_15e-05, 0.005_485_74),
        (4.923_67e-05, 0.010_213_4),
        (8.548_45e-05, 0.013_819_4),
        (0.000_112_67, 0.015_849_8),
        (0.000_148_616, 0.017_934),
        (0.000_588_725, 0.030_677_7),
        (0.001_021_58, 0.038_554_8),
        (0.001_772_22, 0.049_120_5),
        (0.012_187_1, 0.117_544),
        (0.036_679_8, 0.195_601),
        (0.048_312_3, 0.225_288),
        (0.063_634, 0.254_988),
        (0.073_030_3, 0.273_203),
        (0.096_191_2, 0.318_099),
        (0.126_697, 0.358_297),
        (0.166_877, 0.410_222),
        (0.191_519, 0.442_067),
        (0.246_507, 0.513_129),
        (0.284_182, 0.537_472),
        (0.359_531, 0.586_159),
        (0.472_555, 0.659_19),
        (0.623_254, 0.756_564),
        (0.773_952, 0.853_939),
        (0.886_976, 0.926_969),
        (1.0, 1.0),
    ],
};

/// Job display documents of about 10.8 KB of JSON read in small batches,
/// one per screen of cards: the busiest hour's curve over 959,536
/// documents and 2,408,398 lookups, thinned to 25 points with the same
/// five shares within 0.1 points of the hour's (1.76, 5.50, 17.0, 47.4,
/// and 80.1 percent). Zipf of exponent about 0.57 to the 63 percent of
/// documents read once, which take 25 percent of the lookups.
pub const DOCUMENT_CURVE: RankCurve = RankCurve {
    points: &[
        (1.042_17e-06, 0.000_866_966),
        (2.084_34e-06, 0.001_427_92),
        (4.168_68e-06, 0.002_272_88),
        (6.253_02e-06, 0.003_022_76),
        (3.126_51e-05, 0.008_737_76),
        (4.585_55e-05, 0.011_173_8),
        (6.774_11e-05, 0.014_129_3),
        (9.900_62e-05, 0.017_602_6),
        (0.000_143_82, 0.021_547_5),
        (0.000_238_657, 0.027_888_2),
        (0.001_588_27, 0.068_724_5),
        (0.013_616, 0.197_782),
        (0.032_976_4, 0.298_58),
        (0.062_031, 0.392_355),
        (0.102_834, 0.479_261),
        (0.193_438, 0.601_611),
        (0.282_61, 0.678_344),
        (0.372_563, 0.750_021),
        (0.403_935, 0.762_52),
        (0.466_679, 0.787_518),
        (0.560_794, 0.825_015),
        (0.686_282, 0.875_011),
        (0.811_768, 0.925_006),
        (0.905_884, 0.962_503),
        (1.0, 1.0),
    ],
};

/// The cumulative share of lookups over a workload's keys: for each rank
/// from the hottest, the share it and every hotter rank take.
#[derive(Clone, Debug, PartialEq)]
pub struct RankCdf {
    cumulative: Vec<f64>,
}

impl RankCdf {
    /// `curve` over `keys` keys, rank `r` taking the curve's share of
    /// the first `r + 1` keys less that of the first `r`. Fewer than one
    /// key, or more than a `u32` numbers, is
    /// `WorkloadFault::WorkloadKeys`.
    pub fn from_curve(curve: RankCurve, keys: u64) -> Result<Self> {
        check_keys(keys)?;
        Ok(Self::normalized(
            (1..=keys).map(|rank| curve.share(approx::ratio(rank, keys))),
        ))
    }

    /// Zipf of `exponent` over `keys` keys, rank `r` from zero drawn in
    /// proportion to `1 / (r + 1)^exponent`, computed exactly. A key count
    /// refused as `from_curve` refuses it, or an exponent that is not a
    /// finite number at or above zero, is a `WorkloadFault`.
    pub fn zipf(exponent: f64, keys: u64) -> Result<Self> {
        check_keys(keys)?;
        if !(exponent.is_finite() && exponent >= 0.0) {
            return Err(WorkloadFault::ZipfExponent { exponent }.into());
        }
        let mut total = 0.0;
        Ok(Self::normalized((1..=keys).map(|rank| {
            total += approx::ratio(1, rank).powf(exponent);
            total
        })))
    }

    /// `cumulative` as shares of its last.
    fn normalized(cumulative: impl Iterator<Item = f64>) -> Self {
        let mut cumulative: Vec<f64> = cumulative.collect();
        let last = cumulative.last().copied().unwrap_or(1.0);
        for share in &mut cumulative {
            *share /= last;
        }
        Self { cumulative }
    }

    /// The keys ranked.
    pub fn keys(&self) -> usize {
        self.cumulative.len()
    }

    /// Each rank's reads among `lookups`: `round(L·S(k/K)) −
    /// round(L·S((k−1)/K))` for rank `k` from one, `S` the cumulative
    /// share, rounded half up, so the counts sum to `lookups` (the module
    /// doc).
    pub fn counts(&self, lookups: u64) -> Vec<u64> {
        let total = approx::gauge(lookups);
        let mut rounded = 0;
        let mut previous = 0;
        let last = self.cumulative.len() - 1;
        let mut counts = Vec::with_capacity(self.cumulative.len());
        for (rank, &share) in self.cumulative.iter().enumerate() {
            let target = total * share;
            while rounded < lookups && approx::gauge(rounded) + HALF <= target {
                rounded += 1;
            }
            if rank == last {
                rounded = lookups;
            }
            counts.push(rounded - previous);
            previous = rounded;
        }
        counts
    }
}

/// What rounds a cumulative count half up (`RankCdf::counts`).
const HALF: f64 = 0.5;

/// Refuses a key count a `RankCdf` cannot rank: none, or more than a `u32`
/// numbers.
fn check_keys(keys: u64) -> Result<()> {
    if keys == 0 || u32::try_from(keys).is_err() {
        return Err(WorkloadFault::WorkloadKeys { keys }.into());
    }
    Ok(())
}

/// One bucket of a histogram of keys per request: the least and most keys
/// of a request in it, and the requests it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SizeBucket {
    /// The fewest keys of a request in the bucket.
    pub least: u32,
    /// The most keys of a request in the bucket.
    pub most: u32,
    /// The requests measured in it.
    pub requests: u64,
}

/// Keys per batch of the job embedding workload, the day's 3.4 million
/// batches: p10 2, p25 22, p50 228, p75 382, p90 784, p99 1,813, mean
/// 340, at most 5,925, which closes the last bucket.
pub const EMBEDDING_SIZES: &[SizeBucket] = &[
    SizeBucket {
        least: 1,
        most: 1,
        requests: 235_117,
    },
    SizeBucket {
        least: 2,
        most: 4,
        requests: 331_512,
    },
    SizeBucket {
        least: 5,
        most: 9,
        requests: 142_647,
    },
    SizeBucket {
        least: 10,
        most: 19,
        requests: 115_735,
    },
    SizeBucket {
        least: 20,
        most: 49,
        requests: 207_089,
    },
    SizeBucket {
        least: 50,
        most: 99,
        requests: 152_450,
    },
    SizeBucket {
        least: 100,
        most: 199,
        requests: 313_075,
    },
    SizeBucket {
        least: 200,
        most: 499,
        requests: 1_260_049,
    },
    SizeBucket {
        least: 500,
        most: 999,
        requests: 378_251,
    },
    SizeBucket {
        least: 1_000,
        most: 1_999,
        requests: 253_153,
    },
    SizeBucket {
        least: 2_000,
        most: 4_999,
        requests: 6_019,
    },
    SizeBucket {
        least: 5_000,
        most: 5_925,
        requests: 418,
    },
];

/// Keys per batch of the document workload, the day's 2.7 million
/// batches of documents shown together: p10 1, p25 2, p50 4, p75 13, p90
/// 37, p99 107, mean 13.1, at most 1,544, which closes the last bucket.
/// Each of the day's buckets from 5 keys up to 999 is split as the
/// busiest hour's batches split it, since a log-uniform draw over the
/// day's wider buckets overshot the mean by a quarter; the draw's mean is
/// 13.3.
pub const DOCUMENT_SIZES: &[SizeBucket] = &[
    SizeBucket {
        least: 1,
        most: 1,
        requests: 356_400,
    },
    SizeBucket {
        least: 2,
        most: 2,
        requests: 572_501,
    },
    SizeBucket {
        least: 3,
        most: 4,
        requests: 503_463,
    },
    SizeBucket {
        least: 5,
        most: 6,
        requests: 205_794,
    },
    SizeBucket {
        least: 7,
        most: 9,
        requests: 225_816,
    },
    SizeBucket {
        least: 10,
        most: 13,
        requests: 191_942,
    },
    SizeBucket {
        least: 14,
        most: 19,
        requests: 167_772,
    },
    SizeBucket {
        least: 20,
        most: 29,
        requests: 165_481,
    },
    SizeBucket {
        least: 30,
        most: 49,
        requests: 144_157,
    },
    SizeBucket {
        least: 50,
        most: 69,
        requests: 131_596,
    },
    SizeBucket {
        least: 70,
        most: 99,
        requests: 29_090,
    },
    SizeBucket {
        least: 100,
        most: 149,
        requests: 19_348,
    },
    SizeBucket {
        least: 150,
        most: 249,
        requests: 9_009,
    },
    SizeBucket {
        least: 250,
        most: 499,
        requests: 2_969,
    },
    SizeBucket {
        least: 500,
        most: 999,
        requests: 256,
    },
    SizeBucket {
        least: 1_000,
        most: 1_544,
        requests: 14,
    },
];

/// Keys per request drawn from a histogram: a bucket in proportion to its
/// requests, and within it a count log-uniform over `[least, most + 1)`,
/// floored, so a count `n` takes the bucket's share times
/// `ln((n + 1) / n) / ln((most + 1) / least)`.
#[derive(Clone, Debug, PartialEq)]
pub struct SizeDist {
    /// For each count from one, the share of requests of that many keys or
    /// fewer.
    cumulative: Vec<f64>,
}

impl SizeDist {
    /// The distribution of `buckets`, which hold disjoint counts from one.
    pub fn new(buckets: &[SizeBucket]) -> Self {
        let most = buckets.iter().map(|bucket| bucket.most).max().unwrap_or(1);
        let mut mass = vec![0.0; approx::length(most)];
        let requests: u64 = buckets.iter().map(|bucket| bucket.requests).sum();
        for bucket in buckets {
            let share = approx::ratio(bucket.requests, requests);
            let span = (f64::from(bucket.most) + 1.0).ln() - f64::from(bucket.least).ln();
            for count in bucket.least..=bucket.most {
                let width = (f64::from(count) + 1.0).ln() - f64::from(count).ln();
                mass[approx::length(count - 1)] += share * width / span;
            }
        }
        let mut total = 0.0;
        let cumulative = mass
            .into_iter()
            .map(|share| {
                total += share;
                total
            })
            .collect();
        Self { cumulative }
    }

    /// The count of the uniform draw `uniform`, in [0, 1).
    pub fn count(&self, uniform: f64) -> usize {
        1 + self
            .cumulative
            .partition_point(|&share| share <= uniform)
            .min(self.cumulative.len() - 1)
    }

    /// The mean count.
    pub fn mean(&self) -> f64 {
        let mut previous = 0.0;
        let mut mean = 0.0;
        for (count, &share) in (1_u32..).zip(&self.cumulative) {
            mean += f64::from(count) * (share - previous);
            previous = share;
        }
        mean
    }
}

/// A measured workload as the benchmark draws it: its rank curve, its
/// requests' key counts (`None` for one key a request), the copula's `ρ`
/// (the module doc), and its busiest hour's lookups and distinct keys,
/// whose ratio the benchmark keeps over however many keys it draws from.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fitted {
    /// The keys' rank curve.
    pub curve: RankCurve,
    /// The requests' key counts.
    pub sizes: Option<&'static [SizeBucket]>,
    /// The copula's correlation between one request's keys.
    pub rho: f64,
    /// The busiest hour's lookups.
    pub hour_lookups: u64,
    /// The busiest hour's distinct keys.
    pub hour_keys: u64,
}

/// The embedding workload's batches. `ρ` is 0.7175, fitted so that over
/// batches of 50 keys or more the standard deviation of a batch's share of
/// keys among the hottest one percent is 0.315, as measured over the
/// busiest hour, against 0.028 for independent draws: a model of the
/// share's variance across batches, from the curve's share at one percent
/// and the sizes above, solved by bisection, and checked against the draw.
/// The hour read 111,821,327 keys, 4,770,350 of them distinct, in 336,290
/// batches.
pub const EMBEDDING_BATCHES: Fitted = Fitted {
    curve: EMBEDDING_CURVE,
    sizes: Some(EMBEDDING_SIZES),
    rho: 0.7175,
    hour_lookups: 111_821_327,
    hour_keys: 4_770_350,
};

/// The per-user workload, one Get a request, so `ρ` has no effect. The
/// hour read 5,123,466 keys, 3,310,542 of them distinct. The draw spreads
/// a key's reads evenly through the run, while the hour read 8.6 percent
/// of its keys again within 0.1 s, so at small budgets the trace's row
/// cache hits about 6 points more often than this draw's.
pub const PER_USER_GET: Fitted = Fitted {
    curve: PER_USER_CURVE,
    sizes: None,
    rho: 0.0,
    hour_lookups: 5_123_466,
    hour_keys: 3_310_542,
};

/// The document workload's batches. `ρ` is 0.8912, fitted as the
/// embedding workload's is, over batches of 10 keys or more, to a
/// standard deviation of 0.31, against 0.08 for independent draws. The
/// hour read 2,408,398 keys, 959,536 of them distinct, in 196,347 batches.
pub const DOCUMENT_BATCHES: Fitted = Fitted {
    curve: DOCUMENT_CURVE,
    sizes: Some(DOCUMENT_SIZES),
    rho: 0.8912,
    hour_lookups: 2_408_398,
    hour_keys: 959_536,
};

impl Fitted {
    /// The lookups the hour would make over `keys` distinct keys at its
    /// own lookups per key.
    pub fn lookups_over(self, keys: u64) -> u64 {
        let lookups = u128::from(keys) * u128::from(self.hour_lookups) / u128::from(self.hour_keys);
        u64::try_from(lookups).expect("a workload's lookups fit a u64")
    }
}

/// The `lookups` reads of `cdf`'s keys (`RankCdf::counts`) dealt into
/// requests until none remain, seeded by `seed`: each request's key count
/// from `sizes` (one when `None`), the last request shorter when the reads
/// run out, and its keys taken from what remains through the copula of
/// correlation `rho` (the module doc), each a rank in `0..cdf.keys()`. A
/// `rho` outside [0, 1] is `WorkloadFault::Rho`.
pub fn draw_requests(
    cdf: &RankCdf,
    sizes: Option<&SizeDist>,
    rho: f64,
    lookups: u64,
    seed: u64,
) -> Result<Requests<u32>> {
    if !(0.0..=1.0).contains(&rho) {
        return Err(WorkloadFault::Rho { rho }.into());
    }
    let shared = rho.sqrt();
    let own = (1.0 - rho).sqrt();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut pool = Pool::new(&cdf.counts(lookups));
    let mut requests = Requests::new();
    let mut keys = Vec::new();
    while !pool.is_empty() {
        let count = sizes.map_or(1, |sizes| sizes.count(rng.random()));
        let z = standard_normal(&mut rng);
        keys.clear();
        for _ in 0..count {
            if pool.is_empty() {
                break;
            }
            let uniform = phi(shared * z + own * standard_normal(&mut rng));
            let rank = pool.take(uniform);
            keys.push(u32::try_from(rank).expect("a RankCdf ranks fewer keys than a u32 numbers"));
        }
        requests.push(&keys);
    }
    Ok(requests)
}

/// The reads left of each rank, in a Fenwick tree over the ranks, so
/// taking the read at a quantile of what remains costs one walk down the
/// tree of `log K` steps.
struct Pool {
    /// One-based: `tree[i]` holds the reads left of the ranks in
    /// `(i − i.isolate_lowest_one(), i]`.
    tree: Vec<u64>,
    remaining: u64,
}

impl Pool {
    /// A pool of `counts[k]` reads of each rank `k`.
    fn new(counts: &[u64]) -> Self {
        let mut tree = vec![0; counts.len() + 1];
        for (at, &count) in counts.iter().enumerate() {
            let node = at + 1;
            tree[node] += count;
            let parent = node + node.isolate_lowest_one();
            if parent < tree.len() {
                tree[parent] += tree[node];
            }
        }
        Self {
            tree,
            remaining: counts.iter().sum(),
        }
    }

    fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    /// Removes and returns the rank of the read at quantile `uniform`, in
    /// [0, 1), of the reads left in rank order: the rank whose reads span
    /// that quantile. Only called on a pool that is not empty.
    fn take(&mut self, uniform: f64) -> usize {
        let ranks = self.tree.len() - 1;
        let mut at = 0;
        let mut below = 0;
        let mut step = if ranks == 0 { 0 } else { 1 << ranks.ilog2() };
        while step > 0 {
            let next = at + step;
            if next <= ranks && approx::ratio(below + self.tree[next], self.remaining) <= uniform {
                at = next;
                below += self.tree[next];
            }
            step >>= 1;
        }
        let mut node = at + 1;
        while node <= ranks {
            self.tree[node] -= 1;
            node += node.isolate_lowest_one();
        }
        self.remaining -= 1;
        at
    }
}

/// A standard normal draw by Box and Muller's transform of two uniforms,
/// the first in (0, 1] so its logarithm is finite.
fn standard_normal(rng: &mut StdRng) -> f64 {
    let radius = 1.0 - rng.random::<f64>();
    let angle = rng.random::<f64>();
    (-2.0 * radius.ln()).sqrt() * (std::f64::consts::TAU * angle).cos()
}

/// The coefficients of the Chebyshev fit of `erfc` (Press et al.,
/// Numerical Recipes, `erfcc`), lowest power first, whose fractional error
/// is below 1.2e-7 everywhere; a uniform that far off moves a draw by less
/// than a millionth of the hottest key's share.
const ERFC_COEFFICIENTS: [f64; 10] = [
    -1.265_512_23,
    1.000_023_68,
    0.374_091_96,
    0.096_784_18,
    -0.186_288_06,
    0.278_868_07,
    -1.135_203_98,
    1.488_515_87,
    -0.822_152_23,
    0.170_872_77,
];

/// The complementary error function, by `ERFC_COEFFICIENTS`.
fn erfc(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.5 * x.abs());
    let series = ERFC_COEFFICIENTS
        .iter()
        .rev()
        .fold(0.0, |sum, &coefficient| sum * t + coefficient);
    let value = t * (-x * x + series).exp();
    if x >= 0.0 { value } else { 2.0 - value }
}

/// The standard normal CDF `Φ`, through `erfc`, held below one so a rank
/// drawn from it exists.
fn phi(x: f64) -> f64 {
    (0.5 * erfc(-x / std::f64::consts::SQRT_2)).min(1.0 - f64::EPSILON)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// The shares of keys, in parts per million, at which each curve's
    /// share of lookups was measured and at which the tests check the
    /// counts against the curve: 0.01, 0.1, 1, 10, and 50 percent.
    const PARTS_PER_MILLION: [u64; 5] = [100, 1_000, 10_000, 100_000, 500_000];

    /// Keys the curve tests count over: a million, so each share of
    /// `PARTS_PER_MILLION` is a whole number of keys.
    const KEYS: u64 = 1_000_000;

    /// Keys the draw tests deal from: enough that the hottest one percent
    /// is a thousand keys, few enough that a draw of the embedding batches'
    /// 2.3 million reads stays quick.
    const DRAW_KEYS: u64 = 100_000;

    /// The seed of the tests' draws.
    const SEED: u64 = 11;

    /// `parts` per million as a share.
    fn share_of(parts: u64) -> f64 {
        approx::ratio(parts, KEYS)
    }

    /// Each curve's busiest-hour shares at `PARTS_PER_MILLION`, and the
    /// curve itself within half a point of them, as the thinning kept it.
    #[test]
    fn test_share_is_within_half_a_point_of_the_measured_shares() {
        let measured = [
            (EMBEDDING_CURVE, [0.0298, 0.134, 0.278, 0.723, 0.971]),
            (PER_USER_CURVE, [0.0150, 0.0382, 0.108, 0.324, 0.677]),
            (DOCUMENT_CURVE, [0.0176, 0.0550, 0.170, 0.474, 0.801]),
        ];
        for (curve, shares) in measured {
            for (parts, share) in PARTS_PER_MILLION.into_iter().zip(shares) {
                let drawn = curve.share(share_of(parts));
                assert!(
                    (drawn - share).abs() < 0.005,
                    "{parts} ppm: {drawn} against {share}"
                );
            }
            assert!((curve.share(1.0) - 1.0).abs() < f64::EPSILON);
        }
    }

    /// The counts sum to the lookups exactly, and the hottest keys' share
    /// of them is the curve's within a thousandth, the rounding of one
    /// read per key's boundary over a million keys and more lookups.
    #[test]
    fn test_counts_sum_to_the_lookups_and_follow_the_curve() {
        for fitted in [EMBEDDING_BATCHES, PER_USER_GET, DOCUMENT_BATCHES] {
            let lookups = fitted.lookups_over(KEYS);
            let counts = RankCdf::from_curve(fitted.curve, KEYS)
                .unwrap()
                .counts(lookups);
            assert_eq!(counts.len(), approx::length(KEYS));
            assert_eq!(counts.iter().sum::<u64>(), lookups);
            for parts in PARTS_PER_MILLION {
                let hot: u64 = counts[..approx::length(parts)].iter().sum();
                let share = approx::ratio(hot, lookups);
                let want = fitted.curve.share(share_of(parts));
                assert!(
                    (share - want).abs() < 0.001,
                    "{parts} ppm: {share} against {want}"
                );
            }
        }
    }

    /// The per-user curve's flat tail of keys read once survives the
    /// thinning: three keys in four are read exactly once, as in the hour.
    #[test]
    fn test_counts_keep_the_keys_read_once() {
        let keys = PER_USER_GET.hour_keys;
        let counts = RankCdf::from_curve(PER_USER_CURVE, keys)
            .unwrap()
            .counts(PER_USER_GET.hour_lookups);
        let once = counts.iter().filter(|&&count| count == 1).count();
        let share = approx::ratio(approx::count(once), keys);
        assert!((share - 0.753).abs() < 0.02, "read once: {share}");
    }

    #[test]
    fn test_zipf_is_the_exact_harmonic_shares() {
        let cdf = RankCdf::zipf(1.0, 4).unwrap();
        let total = 1.0 + 1.0 / 2.0 + 1.0 / 3.0 + 1.0 / 4.0;
        let shares = [1.0 / total, 1.5 / total, (1.5 + 1.0 / 3.0) / total, 1.0];
        for (got, want) in cdf.cumulative.iter().zip(shares) {
            assert!((got - want).abs() < 1e-12, "{got} against {want}");
        }
        assert_eq!(cdf.counts(25), vec![12, 6, 4, 3]);
        let flat = RankCdf::zipf(0.0, 4).unwrap();
        assert_eq!(
            flat.counts(8),
            vec![2, 2, 2, 2],
            "an exponent of zero is uniform"
        );
    }

    #[test]
    fn test_zipf_refuses_a_bad_exponent_or_no_keys() {
        for exponent in [-0.5, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                RankCdf::zipf(exponent, KEYS),
                Err(crate::TestkitError::Workload(
                    WorkloadFault::ZipfExponent { .. }
                ))
            ));
        }
        assert!(matches!(
            RankCdf::from_curve(EMBEDDING_CURVE, 0),
            Err(crate::TestkitError::Workload(WorkloadFault::WorkloadKeys {
                keys: 0
            }))
        ));
    }

    #[test]
    fn test_phi_is_the_normal_cdf() {
        let known = [
            (0.0, 0.5),
            (1.0, 0.841_344_746),
            (-1.0, 0.158_655_254),
            (1.96, 0.975_002_105),
            (-3.0, 0.001_349_898),
        ];
        for (x, want) in known {
            assert!((phi(x) - want).abs() < 1e-7, "phi({x}) = {}", phi(x));
        }
        assert!(phi(40.0) < 1.0, "phi stays below one");
    }

    /// 200,000 draws: each quantile of the drawn counts lies in the bucket
    /// the measured one lies in, and log-uniform spreading inside a bucket
    /// moves it by at most 12 percent for these histograms (the p90 of the
    /// embedding batches, at 863 against 784, moves most).
    /// Each histogram's draw keeps the measured mean of keys per batch
    /// within 2 percent: 340 for the embedding batches, 13.1 for the
    /// documents.
    #[test]
    fn test_size_dist_keeps_the_measured_mean() {
        for (buckets, measured) in [(EMBEDDING_SIZES, 340.0), (DOCUMENT_SIZES, 13.1)] {
            let mean = SizeDist::new(buckets).mean();
            assert!(
                (mean - measured).abs() <= 0.02 * measured,
                "drew a mean of {mean} against {measured}"
            );
        }
    }

    #[test]
    fn test_size_dist_draws_the_measured_median_and_p90() {
        let draws = 200_000;
        let mut rng = StdRng::seed_from_u64(SEED);
        for (buckets, median, p90) in [(EMBEDDING_SIZES, 228, 784), (DOCUMENT_SIZES, 4, 37)] {
            let sizes = SizeDist::new(buckets);
            let mut counts: Vec<usize> = (0..draws).map(|_| sizes.count(rng.random())).collect();
            counts.sort_unstable();
            for (quantile, measured) in [(draws / 2, median), (draws * 9 / 10, p90)] {
                let count = counts[quantile];
                let off = approx::ratio(
                    approx::count(count.abs_diff(measured)),
                    approx::count(measured),
                );
                assert!(off <= 0.12, "drew {count} against {measured}");
                let bucket = |keys: usize| {
                    buckets.iter().position(|bucket| {
                        approx::length(bucket.least) <= keys && keys <= approx::length(bucket.most)
                    })
                };
                assert_eq!(
                    bucket(count),
                    bucket(measured),
                    "drew {count} against {measured}"
                );
            }
            assert!(counts.iter().all(|&count| count >= 1));
        }
    }

    /// `fitted`'s requests over `DRAW_KEYS` keys at `rho`, and the counts
    /// they were dealt from.
    fn drawn(fitted: Fitted, rho: f64) -> (Requests<u32>, Vec<u64>) {
        let cdf = RankCdf::from_curve(fitted.curve, DRAW_KEYS).unwrap();
        let lookups = fitted.lookups_over(DRAW_KEYS);
        let sizes = fitted.sizes.map(SizeDist::new);
        let requests = draw_requests(&cdf, sizes.as_ref(), rho, lookups, SEED).unwrap();
        (requests, cdf.counts(lookups))
    }

    /// The mean and standard deviation of the share of a request's keys
    /// among the hottest one percent, over the requests of at least
    /// `least` keys, and how many there were.
    fn hot_share_spread(requests: &Requests<u32>, least: usize) -> (f64, f64, usize) {
        let hot = approx::length(DRAW_KEYS) / 100;
        let shares: Vec<f64> = requests
            .iter()
            .filter(|keys| keys.len() >= least)
            .map(|keys| {
                let hot_keys = keys
                    .iter()
                    .filter(|&&rank| approx::length(rank) < hot)
                    .count();
                approx::ratio(approx::count(hot_keys), approx::count(keys.len()))
            })
            .collect();
        let count = approx::gauge_len(shares.len());
        let mean = shares.iter().sum::<f64>() / count;
        let variance = shares
            .iter()
            .map(|share| (share - mean).powi(2))
            .sum::<f64>()
            / count;
        (mean, variance.sqrt(), shares.len())
    }

    /// Every key is read exactly its count, and a seed draws the same
    /// requests again.
    #[test]
    fn test_draw_requests_reads_every_key_its_count() {
        for fitted in [EMBEDDING_BATCHES, PER_USER_GET, DOCUMENT_BATCHES] {
            let (requests, counts) = drawn(fitted, fitted.rho);
            let mut read: HashMap<u32, u64> = HashMap::new();
            for &rank in requests.iter().flatten() {
                *read.entry(rank).or_default() += 1;
            }
            for (rank, &count) in counts.iter().enumerate() {
                let rank = u32::try_from(rank).unwrap();
                assert_eq!(read.get(&rank).copied().unwrap_or(0), count, "rank {rank}");
            }
            assert_eq!(
                approx::count(requests.lookups()),
                counts.iter().sum::<u64>()
            );
            assert_eq!(requests, drawn(fitted, fitted.rho).0, "a seed draws alike");
        }
        let (one_key, _) = drawn(PER_USER_GET, PER_USER_GET.rho);
        assert!(one_key.iter().all(|keys| keys.len() == 1));
    }

    /// Thousands of batches of at least 50 or 10 keys: the hot share's mean
    /// is the curve's share at one percent and its standard deviation the
    /// measured one, within 0.02 each, five or more standard errors at that
    /// many batches.
    #[test]
    fn test_draw_requests_spreads_hot_shares_as_measured() {
        for (fitted, least, measured) in
            [(EMBEDDING_BATCHES, 50, 0.315), (DOCUMENT_BATCHES, 10, 0.31)]
        {
            let (requests, _) = drawn(fitted, fitted.rho);
            let (mean, deviation, batches) = hot_share_spread(&requests, least);
            let expected = fitted.curve.share(0.01);
            assert!(batches > 2_000, "{batches} batches");
            assert!(
                (mean - expected).abs() < 0.02,
                "mean {mean} against {expected}"
            );
            assert!(
                (deviation - measured).abs() < 0.02,
                "deviation {deviation} against {measured}"
            );
        }
    }

    #[test]
    fn test_draw_requests_without_correlation_spreads_as_independent_draws() {
        let (requests, _) = drawn(EMBEDDING_BATCHES, 0.0);
        let (_, deviation, _) = hot_share_spread(&requests, 50);
        assert!(deviation < 0.05, "independent draws spread {deviation}");
    }

    #[test]
    fn test_draw_requests_refuses_a_rho_outside_zero_to_one() {
        let cdf = RankCdf::from_curve(PER_USER_CURVE, DRAW_KEYS).unwrap();
        assert!(matches!(
            draw_requests(&cdf, None, 1.5, 1, SEED),
            Err(crate::TestkitError::Workload(WorkloadFault::Rho { .. }))
        ));
    }

    #[test]
    fn test_pool_takes_each_read_once_in_quantile_order() {
        let mut pool = Pool::new(&[2, 0, 1]);
        assert_eq!(pool.take(0.9), 2, "the top third is rank 2's read");
        assert_eq!(pool.take(0.0), 0);
        assert_eq!(pool.take(0.99), 0, "rank 1 has no read and rank 2 is spent");
        assert!(pool.is_empty());
    }

    #[test]
    fn test_lookups_over_keeps_the_hour_lookups_per_key() {
        assert_eq!(EMBEDDING_BATCHES.lookups_over(4_770_350), 111_821_327);
        assert_eq!(DOCUMENT_BATCHES.lookups_over(959_536 / 2), 2_408_398 / 2);
    }
}
