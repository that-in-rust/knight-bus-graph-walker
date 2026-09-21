// Standalone A06 research executable; intentionally outside the Cargo workspace.
// rustc --edition=2021 -O -Dwarnings -C overflow-checks=yes FILE -o PROGRAM
// All accounting is logical requested I/O, Vec capacity, and owned live file
// lengths. It excludes filesystem allocation, source retention, RSS and cache.
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

type ResearchResult<T> = Result<T, String>;

const C1: u64 = 0x9e3779b97f4a7c15;
const C2: u64 = 0xbf58476d1ce4e5b9;
const IO_BYTES: usize = 65536;
const PACKED_CAP: u64 = 2_000_000_000;
const DISK_CAP: u64 = 50_000_000_000;
const ERROR_SCALE: i128 = 1_i128 << 32;
const BUDGET_SCALE: i128 = 1_i128 << 35;

fn convert_io_error_result<T>(value: std::io::Result<T>) -> ResearchResult<T> {
    value.map_err(|error| error.to_string())
}

fn add_checked_integer_values(a: i128, b: i128) -> ResearchResult<i128> {
    a.checked_add(b)
        .ok_or_else(|| "i128 addition admission".into())
}

fn multiply_checked_integer_values(a: i128, b: i128) -> ResearchResult<i128> {
    a.checked_mul(b)
        .ok_or_else(|| "i128 multiplication admission".into())
}

fn write_packed_signed_cell(bytes: &mut [u8], value: i128) -> ResearchResult<()> {
    let width = bytes.len();
    if !(1..=16).contains(&width) {
        return Err("packed width admission".into());
    }
    if width < 16 {
        let limit = 1_i128 << (width * 8 - 1);
        if value < -limit || value >= limit {
            return Err("packed signed overflow".into());
        }
    }
    bytes.copy_from_slice(&value.to_le_bytes()[..width]);
    Ok(())
}

fn read_packed_signed_cell(bytes: &[u8]) -> ResearchResult<i128> {
    if !(1..=16).contains(&bytes.len()) {
        return Err("packed width admission".into());
    }
    let mut full = [if bytes[bytes.len() - 1] & 128 != 0 {
        255
    } else {
        0
    }; 16];
    full[..bytes.len()].copy_from_slice(bytes);
    Ok(i128::from_le_bytes(full))
}

fn round_exact_integer_even(num: i128, den: i128) -> ResearchResult<i128> {
    if den <= 0 {
        return Err("nonpositive division denominator".into());
    }
    let quotient = num.div_euclid(den);
    let remainder = num.rem_euclid(den);
    // Comparing r with den-r avoids doubling a possibly large remainder.
    if remainder > den - remainder || (remainder == den - remainder && quotient % 2 != 0) {
        add_checked_integer_values(quotient, 1)
    } else {
        Ok(quotient)
    }
}

fn ceil_positive_integer_ratio(num: i128, den: i128) -> ResearchResult<i128> {
    if num < 0 || den <= 0 {
        return Err("positive ceiling admission".into());
    }
    add_checked_integer_values(num / den, i128::from(num % den != 0))
}

fn calculate_integer_square_root(value: u128) -> u128 {
    let (mut low, mut high) = (0_u128, 1_u128 << 64);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if middle <= value / middle {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

type RowEnclosure = (Vec<(i64, i64)>, u128);

fn enclose_normalized_integer_row(row: &[i128]) -> ResearchResult<RowEnclosure> {
    let mut square = 0;
    for &x in row {
        square = add_checked_integer_values(square, multiply_checked_integer_values(x, x)?)?;
    }
    if square == 0 {
        return Ok((vec![(0, 0); row.len()], 0));
    }
    let h = calculate_integer_square_root(square as u128);
    let lower = calculate_integer_square_root(
        multiply_checked_integer_values(square, 1_i128 << 56)? as u128,
    ) as i128;
    let upper = lower + 1;
    let mut result = Vec::with_capacity(row.len());
    for &x in row {
        let num = multiply_checked_integer_values(x, 1_i128 << 56)?;
        let (dl, du) = if x >= 0 {
            (upper, lower)
        } else {
            (lower, upper)
        };
        let lo = num.div_euclid(dl);
        let hi =
            add_checked_integer_values(num.div_euclid(du), i128::from(num.rem_euclid(du) != 0))?;
        result.push((
            i64::try_from(lo).map_err(|_| "normalizer endpoint overflow")?,
            i64::try_from(hi).map_err(|_| "normalizer endpoint overflow")?,
        ));
    }
    Ok((result, h))
}

fn parse_exact_decimal_tolerance(text: &str) -> ResearchResult<i128> {
    let parts: Vec<_> = text.split('.').collect();
    if parts.len() > 2 || parts[0].is_empty() || !parts[0].bytes().all(|x| x.is_ascii_digit()) {
        return Err("TAU must be a nonnegative finite decimal, without exponent".into());
    }
    let fraction = if parts.len() == 2 { parts[1] } else { "" };
    if fraction.len() > 12
        || !fraction.bytes().all(|x| x.is_ascii_digit())
        || (parts.len() == 2 && fraction.is_empty())
    {
        return Err("TAU permits at most 12 fractional digits".into());
    }
    let denominator = 10_i128.pow(fraction.len() as u32);
    let whole = parts[0].parse::<i128>().map_err(|_| "TAU integer range")?;
    let tail = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| "TAU fractional range")?
    };
    let numerator =
        add_checked_integer_values(multiply_checked_integer_values(whole, denominator)?, tail)?;
    Ok(multiply_checked_integer_values(numerator, BUDGET_SCALE)? / denominator)
}

#[derive(Default)]
struct Meter {
    read: u64,
    written: u64,
    disk_live: u64,
    disk_peak: u64,
    packed_live: u64,
    packed_peak: u64,
    conversion_read: u64,
    conversion_written: u64,
}

type SharedMeter = Rc<RefCell<Meter>>;

struct TrackedReader {
    reader: BufReader<File>,
    meter: SharedMeter,
}

impl TrackedReader {
    fn open_metered_input_file(path: &Path, meter: &SharedMeter) -> ResearchResult<Self> {
        Ok(Self {
            reader: BufReader::with_capacity(IO_BYTES, convert_io_error_result(File::open(path))?),
            meter: Rc::clone(meter),
        })
    }

    fn read_metered_exact_bytes(&mut self, bytes: &mut [u8]) -> ResearchResult<()> {
        convert_io_error_result(self.reader.read_exact(bytes))?;
        self.meter.borrow_mut().read += bytes.len() as u64;
        Ok(())
    }
}

struct TrackedWriter {
    writer: BufWriter<File>,
    position: u64,
    length: u64,
    meter: SharedMeter,
}

impl TrackedWriter {
    fn open_metered_output_file(
        path: &Path,
        meter: &SharedMeter,
        update: bool,
    ) -> ResearchResult<Self> {
        let file = if update {
            convert_io_error_result(OpenOptions::new().write(true).open(path))?
        } else {
            convert_io_error_result(OpenOptions::new().write(true).create_new(true).open(path))?
        };
        let length = convert_io_error_result(file.metadata())?.len();
        Ok(Self {
            writer: BufWriter::with_capacity(IO_BYTES, file),
            position: 0,
            length,
            meter: Rc::clone(meter),
        })
    }

    fn write_metered_exact_bytes(&mut self, bytes: &[u8]) -> ResearchResult<()> {
        let next = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or("file length overflow")?;
        let growth = next.saturating_sub(self.length);
        if self
            .meter
            .borrow()
            .disk_live
            .checked_add(growth)
            .ok_or("disk counter overflow")?
            > DISK_CAP
        {
            return Err("owned disk limit exceeded".into());
        }
        convert_io_error_result(self.writer.write_all(bytes))?;
        self.position = next;
        self.length = self.length.max(next);
        let mut meter = self.meter.borrow_mut();
        meter.written += bytes.len() as u64;
        meter.disk_live += growth;
        meter.disk_peak = meter.disk_peak.max(meter.disk_live);
        Ok(())
    }

    fn flush_metered_output_file(&mut self) -> ResearchResult<()> {
        convert_io_error_result(self.writer.flush())
    }
}

fn remove_metered_owned_file(path: &Path, meter: &SharedMeter) -> ResearchResult<()> {
    let length = convert_io_error_result(fs::metadata(path))?.len();
    convert_io_error_result(fs::remove_file(path))?;
    let mut stats = meter.borrow_mut();
    stats.disk_live = stats
        .disk_live
        .checked_sub(length)
        .ok_or("owned file accounting underflow")?;
    Ok(())
}

fn mix_seeded_integer_value(mut value: u64) -> u64 {
    value = value.wrapping_add(C1);
    value = (value ^ (value >> 30)).wrapping_mul(C2);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn validate_source_header_values(n: u64, features: u64) -> ResearchResult<()> {
    if !(2..=5_000_000).contains(&features) || n < 2 * features || n > 100_000_000 {
        return Err("source admission: 2<=F<=5000000 and 2F<=N<=100000000".into());
    }
    Ok(())
}

fn generate_canonical_source_file(
    path: &Path,
    n: u64,
    features: u64,
    seed: u64,
) -> ResearchResult<()> {
    validate_source_header_values(n, features)?;
    let file = convert_io_error_result(OpenOptions::new().write(true).create_new(true).open(path))?;
    let mut writer = BufWriter::with_capacity(IO_BYTES, file);
    convert_io_error_result(writer.write_all(b"KBMEAN01"))?;
    for value in [n, features, seed] {
        convert_io_error_result(writer.write_all(&value.to_le_bytes()))?;
    }
    // Generator detail left open by the plan: hot prefix max(1,F/8).
    for i in 0..n {
        let first = i % features;
        let mixed = mix_seeded_integer_value(seed ^ i.wrapping_mul(C1));
        let mut second = mixed
            % if i % 5 == 0 {
                (features / 8).max(1)
            } else {
                features
            };
        if second == first {
            second = (second + 1) % features;
        }
        convert_io_error_result(writer.write_all(&i.to_le_bytes()))?;
        convert_io_error_result(writer.write_all(&(first as u32).to_le_bytes()))?;
        convert_io_error_result(writer.write_all(&(second as u32).to_le_bytes()))?;
    }
    convert_io_error_result(writer.flush())
}

struct Source {
    n: u64,
    features: usize,
    seed: u64,
    counts: Vec<u64>,
    prepared: PathBuf,
}

fn decode_source_record_bytes(
    bytes: &[u8; 16],
    expected: u64,
    features: usize,
) -> ResearchResult<[usize; 2]> {
    let mut id = [0; 8];
    id.copy_from_slice(&bytes[..8]);
    let mut a = [0; 4];
    a.copy_from_slice(&bytes[8..12]);
    let mut b = [0; 4];
    b.copy_from_slice(&bytes[12..]);
    let first = u32::from_le_bytes(a) as usize;
    let second = u32::from_le_bytes(b) as usize;
    if u64::from_le_bytes(id) != expected
        || first >= features
        || second >= features
        || first == second
    {
        return Err("source record ID, feature range, or duplicate membership".into());
    }
    Ok([first, second])
}

fn prepare_validated_source_copy(
    path: &Path,
    out: &Path,
    meter: &SharedMeter,
) -> ResearchResult<Source> {
    let mut reader = TrackedReader::open_metered_input_file(path, meter)?;
    let length = convert_io_error_result(reader.reader.get_ref().metadata())?.len();
    let mut header = [0_u8; 32];
    reader.read_metered_exact_bytes(&mut header)?;
    if &header[..8] != b"KBMEAN01" {
        return Err("source magic mismatch".into());
    }
    let mut values = [0_u64; 3];
    for (j, value) in values.iter_mut().enumerate() {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&header[8 + 8 * j..16 + 8 * j]);
        *value = u64::from_le_bytes(bytes);
    }
    let [n, f, seed] = values;
    validate_source_header_values(n, f)?;
    if length != 32 + 16 * n {
        return Err("source exact length mismatch".into());
    }
    let prepared = out.join("prepared.bin");
    let mut writer = TrackedWriter::open_metered_output_file(&prepared, meter, false)?;
    writer.write_metered_exact_bytes(&header)?;
    let mut counts = vec![0_u64; f as usize];
    for i in 0..n {
        let mut bytes = [0; 16];
        reader.read_metered_exact_bytes(&mut bytes)?;
        for feature in decode_source_record_bytes(&bytes, i, f as usize)? {
            counts[feature] += 1;
        }
        writer.write_metered_exact_bytes(&bytes)?;
    }
    writer.flush_metered_output_file()?;
    if counts.iter().any(|&count| count == 1) {
        return Err("active feature cardinality must be at least two".into());
    }
    Ok(Source {
        n,
        features: f as usize,
        seed,
        counts,
        prepared,
    })
}

fn open_prepared_record_reader(
    source: &Source,
    meter: &SharedMeter,
) -> ResearchResult<TrackedReader> {
    let mut reader = TrackedReader::open_metered_input_file(&source.prepared, meter)?;
    let mut header = [0; 32];
    reader.read_metered_exact_bytes(&mut header)?;
    Ok(reader)
}

fn read_prepared_record_features(
    reader: &mut TrackedReader,
    i: u64,
    source: &Source,
) -> ResearchResult<[usize; 2]> {
    let mut bytes = [0; 16];
    reader.read_metered_exact_bytes(&mut bytes)?;
    decode_source_record_bytes(&bytes, i, source.features)
}

struct Layout {
    offsets: Vec<usize>,
    widths: Vec<usize>,
    dim: usize,
    length: usize,
}

fn build_shared_feature_layout(
    source: &Source,
    dim: usize,
    maximum: i128,
    means: bool,
) -> ResearchResult<Rc<Layout>> {
    let mut offsets = Vec::with_capacity(source.features);
    let mut widths = Vec::with_capacity(source.features);
    let mut length = 0_usize;
    for &count in &source.counts {
        let bound = if means {
            maximum
        } else {
            multiply_checked_integer_values(maximum, i128::from(count))?
        };
        let bits = 2 + (128 - bound.max(1).leading_zeros()) as usize;
        let width = bits.div_ceil(8);
        offsets.push(length);
        widths.push(width);
        length = length
            .checked_add(dim.checked_mul(width).ok_or("layout overflow")?)
            .ok_or("layout overflow")?;
    }
    Ok(Rc::new(Layout {
        offsets,
        widths,
        dim,
        length,
    }))
}

struct PackedPlane {
    bytes: Vec<u8>,
    layout: Rc<Layout>,
    meter: SharedMeter,
}

impl PackedPlane {
    fn allocate_packed_zero_plane(
        layout: &Rc<Layout>,
        meter: &SharedMeter,
    ) -> ResearchResult<Self> {
        if meter.borrow().packed_live + layout.length as u64 > PACKED_CAP {
            return Err("packed plane cap exceeded".into());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(layout.length)
            .map_err(|_| "packed allocation failed")?;
        bytes.resize(layout.length, 0);
        let capacity = bytes.capacity() as u64;
        let mut stats = meter.borrow_mut();
        if stats.packed_live + capacity > PACKED_CAP {
            return Err("packed capacity cap exceeded".into());
        }
        stats.packed_live += capacity;
        stats.packed_peak = stats.packed_peak.max(stats.packed_live);
        drop(stats);
        Ok(Self {
            bytes,
            layout: Rc::clone(layout),
            meter: Rc::clone(meter),
        })
    }

    fn read_packed_feature_value(&self, feature: usize, j: usize) -> ResearchResult<i128> {
        let width = self.layout.widths[feature];
        let offset = self.layout.offsets[feature] + j * width;
        read_packed_signed_cell(&self.bytes[offset..offset + width])
    }

    fn write_packed_feature_value(
        &mut self,
        feature: usize,
        j: usize,
        value: i128,
    ) -> ResearchResult<()> {
        let width = self.layout.widths[feature];
        let offset = self.layout.offsets[feature] + j * width;
        write_packed_signed_cell(&mut self.bytes[offset..offset + width], value)
    }

    fn add_packed_feature_row(&mut self, feature: usize, row: &[i128]) -> ResearchResult<()> {
        for (j, &value) in row.iter().enumerate() {
            self.write_packed_feature_value(
                feature,
                j,
                add_checked_integer_values(self.read_packed_feature_value(feature, j)?, value)?,
            )?;
        }
        Ok(())
    }
}

impl Drop for PackedPlane {
    fn drop(&mut self) {
        self.meter.borrow_mut().packed_live -= self.bytes.capacity() as u64;
    }
}

trait PackedFeatureValues {
    fn read_packed_feature_value(&self, feature: usize, j: usize) -> ResearchResult<i128>;
}

impl PackedFeatureValues for PackedPlane {
    fn read_packed_feature_value(&self, feature: usize, j: usize) -> ResearchResult<i128> {
        PackedPlane::read_packed_feature_value(self, feature, j)
    }
}

struct PackedSlice<'a> {
    bytes: &'a [u8],
    layout: &'a Layout,
}

impl PackedFeatureValues for PackedSlice<'_> {
    fn read_packed_feature_value(&self, feature: usize, j: usize) -> ResearchResult<i128> {
        let width = self.layout.widths[feature];
        let offset = self.layout.offsets[feature] + j * width;
        read_packed_signed_cell(&self.bytes[offset..offset + width])
    }
}

fn promote_arena_workspace_layout(
    workspace: &mut Rc<Layout>,
    means: &Layout,
) -> ResearchResult<()> {
    let sums = Rc::get_mut(workspace).ok_or("workspace layout already shared")?;
    if sums.dim != means.dim || sums.widths.len() != means.widths.len() {
        return Err("arena layout mismatch".into());
    }
    let mut length = 0_usize;
    for f in 0..sums.widths.len() {
        sums.widths[f] = sums.widths[f].max(means.widths[f]);
        sums.offsets[f] = length;
        length = length
            .checked_add(
                sums.dim
                    .checked_mul(sums.widths[f])
                    .ok_or("workspace layout overflow")?,
            )
            .ok_or("workspace layout overflow")?;
    }
    sums.length = length;
    Ok(())
}

fn compact_arena_mean_workspace(
    bytes: &mut [u8],
    sums: &Layout,
    means: &Layout,
    counts: &[u64],
) -> ResearchResult<i128> {
    if bytes.len() != sums.length
        || sums.dim != means.dim
        || counts.len() != sums.widths.len()
        || sums.widths.len() != means.widths.len()
        || sums.widths.iter().zip(&means.widths).any(|(h, c)| h < c)
    {
        return Err("arena compaction width or length admission".into());
    }
    let mut defect = 0;
    // Read each full input cell before writing its shorter output, in forward order.
    for (f, &count) in counts.iter().enumerate() {
        for j in 0..means.dim {
            let input = sums.offsets[f] + j * sums.widths[f];
            let value = read_packed_signed_cell(&bytes[input..input + sums.widths[f]])?;
            let count = i128::from(count);
            let mean = if count == 0 {
                0
            } else {
                round_exact_integer_even(value, count)?
            };
            if count > 0 {
                defect = defect.max(calculate_upward_division_defect(value, count, mean)?);
            }
            let output = means.offsets[f] + j * means.widths[f];
            write_packed_signed_cell(&mut bytes[output..output + means.widths[f]], mean)?;
        }
    }
    Ok(defect)
}

struct HistoryArena {
    bytes: Vec<u8>,
    meter: SharedMeter,
}

impl HistoryArena {
    fn allocate_paid_history_arena(
        depth: usize,
        sums: &Layout,
        means: &Layout,
        meter: &SharedMeter,
    ) -> ResearchResult<Self> {
        let length = if depth == 0 {
            0
        } else {
            (depth - 1)
                .checked_mul(means.length)
                .and_then(|prefix| prefix.checked_add(sums.length))
                .ok_or("arena length overflow")?
                .max(
                    depth
                        .checked_mul(means.length)
                        .ok_or("arena length overflow")?,
                )
        };
        let admitted = meter
            .borrow()
            .packed_live
            .checked_add(length as u64)
            .ok_or("arena counter overflow")?;
        if admitted > PACKED_CAP {
            return Err("packed arena cap exceeded".into());
        }
        let mut bytes = Vec::new();
        if length > 0 {
            bytes
                .try_reserve_exact(length)
                .map_err(|_| "arena allocation failed")?;
            bytes.resize(length, 0);
        }
        let capacity = bytes.capacity() as u64;
        let mut stats = meter.borrow_mut();
        let next = stats
            .packed_live
            .checked_add(capacity)
            .ok_or("arena capacity overflow")?;
        if next > PACKED_CAP {
            return Err("packed arena capacity cap exceeded".into());
        }
        stats.packed_live = next;
        stats.packed_peak = stats.packed_peak.max(next);
        drop(stats);
        Ok(Self {
            bytes,
            meter: Rc::clone(meter),
        })
    }

    fn clear_next_arena_workspace(&mut self, start: usize, length: usize) -> ResearchResult<()> {
        let end = start
            .checked_add(length)
            .ok_or("workspace range overflow")?;
        let workspace = self
            .bytes
            .get_mut(start..end)
            .ok_or("workspace outside arena")?;
        workspace.fill(0);
        Ok(())
    }
}

impl Drop for HistoryArena {
    fn drop(&mut self) {
        self.meter.borrow_mut().packed_live -= self.bytes.capacity() as u64;
    }
}

fn add_arena_workspace_row(
    bytes: &mut [u8],
    sums: &Layout,
    feature: usize,
    row: &[i128],
) -> ResearchResult<()> {
    let width = sums.widths[feature];
    for (j, &value) in row.iter().enumerate() {
        let offset = sums.offsets[feature] + j * width;
        let previous = read_packed_signed_cell(&bytes[offset..offset + width])?;
        write_packed_signed_cell(
            &mut bytes[offset..offset + width],
            add_checked_integer_values(previous, value)?,
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct Receipt {
    admitted: bool,
    rows: u64,
    reason: String,
    mode: String,
    n: u64,
    features: usize,
    dim: usize,
    depth: usize,
    b: u32,
    prep_seconds: f64,
    propagation_seconds: f64,
    output_seconds: f64,
    conversion_seconds: f64,
    total_seconds: f64,
    output_bytes: u64,
    staged_rows: u64,
    bound_units: Option<i128>,
    tolerance_units: Option<i128>,
    metadata_bytes: u64,
    local_scratch_bytes: u64,
    endpoint_record_bytes: usize,
    endpoint_codec: &'static str,
    meter: SharedMeter,
}

fn initialize_seeded_integer_row(source: &Source, i: u64, dim: usize, maximum: i128) -> Vec<i128> {
    (0..dim)
        .map(|j| {
            if j == 0 {
                maximum
            } else {
                let value = mix_seeded_integer_value(
                    source.seed ^ i.wrapping_mul(C1) ^ (j as u64).wrapping_mul(C2),
                );
                (i128::from(value % 3) - 1) * maximum
            }
        })
        .collect()
}

fn calculate_upward_division_defect(num: i128, den: i128, rounded: i128) -> ResearchResult<i128> {
    let residual = add_checked_integer_values(
        multiply_checked_integer_values(den, rounded)?,
        num.checked_neg().ok_or("defect sign overflow")?,
    )?;
    let absolute = residual.checked_abs().ok_or("defect magnitude overflow")?;
    ceil_positive_integer_ratio(multiply_checked_integer_values(absolute, ERROR_SCALE)?, den)
}

fn update_frozen_integer_row(
    source: &Source,
    features: [usize; 2],
    old: &[i128],
    plane: &impl PackedFeatureValues,
    means: bool,
    maximum: i128,
    eta: &mut i128,
) -> ResearchResult<Vec<i128>> {
    let mut q = 0_i128;
    let mut degree = 0_i128;
    for f in features {
        let weight = (1 + f % 17) as i128;
        q = add_checked_integer_values(q, weight)?;
        degree = add_checked_integer_values(
            degree,
            multiply_checked_integer_values(weight, i128::from(source.counts[f] - 1))?,
        )?;
    }
    if degree <= 0 {
        return Err("nonpositive prepared degree".into());
    }
    let mut next = Vec::with_capacity(old.len());
    for (j, &previous) in old.iter().enumerate() {
        let mut numerator = 0;
        for f in features {
            let scale = if means {
                i128::from(source.counts[f])
            } else {
                1
            };
            let contribution = multiply_checked_integer_values(
                plane.read_packed_feature_value(f, j)?,
                multiply_checked_integer_values((1 + f % 17) as i128, scale)?,
            )?;
            numerator = add_checked_integer_values(numerator, contribution)?;
        }
        numerator =
            add_checked_integer_values(numerator, -multiply_checked_integer_values(q, previous)?)?;
        let raw = round_exact_integer_even(numerator, degree)?;
        *eta = (*eta).max(calculate_upward_division_defect(numerator, degree, raw)?);
        let value = if means {
            raw.clamp(-maximum, maximum)
        } else {
            raw
        };
        if value < -maximum || value > maximum {
            return Err("node invariant violated".into());
        }
        next.push(value);
    }
    Ok(next)
}

fn replay_frozen_history_row(
    source: &Source,
    i: u64,
    features: [usize; 2],
    dim: usize,
    maximum: i128,
    history: &[PackedPlane],
    means: bool,
    mut eta: Option<&mut [i128]>,
) -> ResearchResult<Vec<Vec<i128>>> {
    let mut layers = Vec::with_capacity(history.len() + 1);
    layers.push(initialize_seeded_integer_row(source, i, dim, maximum));
    for (t, plane) in history.iter().enumerate() {
        let mut defect = 0;
        let previous = layers.last().ok_or("missing initial layer")?;
        let next = update_frozen_integer_row(
            source,
            features,
            previous,
            plane,
            means,
            maximum,
            &mut defect,
        )?;
        if let Some(errors) = eta.as_deref_mut() {
            errors[t] = errors[t].max(defect);
        }
        layers.push(next);
    }
    Ok(layers)
}

fn convert_completed_mean_plane(
    sums: PackedPlane,
    source: &Source,
    layout: &Rc<Layout>,
    out: &Path,
    receipt: &mut Receipt,
) -> ResearchResult<(PackedPlane, i128)> {
    let start = Instant::now();
    let path = out.join("conversion.bin");
    let mut defect = 0;
    {
        let mut writer = TrackedWriter::open_metered_output_file(&path, &receipt.meter, false)?;
        for f in 0..source.features {
            for j in 0..layout.dim {
                let value = sums.read_packed_feature_value(f, j)?;
                let count = i128::from(source.counts[f]);
                let mean = if count == 0 {
                    0
                } else {
                    round_exact_integer_even(value, count)?
                };
                if count > 0 {
                    defect = defect.max(calculate_upward_division_defect(value, count, mean)?);
                }
                let mut bytes = [0; 16];
                write_packed_signed_cell(&mut bytes[..layout.widths[f]], mean)?;
                writer.write_metered_exact_bytes(&bytes[..layout.widths[f]])?;
            }
        }
        writer.flush_metered_output_file()?;
    }
    receipt.meter.borrow_mut().conversion_written += layout.length as u64;
    // This drop precedes allocation of the replacement plane, not just a length reset.
    drop(sums);
    let mut means = PackedPlane::allocate_packed_zero_plane(layout, &receipt.meter)?;
    {
        let mut reader = TrackedReader::open_metered_input_file(&path, &receipt.meter)?;
        reader.read_metered_exact_bytes(&mut means.bytes)?;
    }
    receipt.meter.borrow_mut().conversion_read += layout.length as u64;
    remove_metered_owned_file(&path, &receipt.meter)?;
    receipt.conversion_seconds += start.elapsed().as_secs_f64();
    Ok((means, defect))
}

fn accumulate_normalized_layer_bounds(
    row: &[i128],
    t: usize,
    endpoints: &mut [(i64, i64)],
    minima: &mut [u128],
) -> ResearchResult<()> {
    let (bounds, h) = enclose_normalized_integer_row(row)?;
    minima[t] = minima[t].min(h);
    let coefficient = if t % 2 == 0 { 2_i128 } else { -1 };
    for ((total_lo, total_hi), (mut lo, mut hi)) in endpoints.iter_mut().zip(bounds) {
        if coefficient < 0 {
            std::mem::swap(&mut lo, &mut hi);
        }
        let lower =
            add_checked_integer_values(i128::from(*total_lo), coefficient * i128::from(lo))?;
        let upper =
            add_checked_integer_values(i128::from(*total_hi), coefficient * i128::from(hi))?;
        *total_lo = i64::try_from(lower).map_err(|_| "output endpoint overflow")?;
        *total_hi = i64::try_from(upper).map_err(|_| "output endpoint overflow")?;
    }
    Ok(())
}

fn write_canonical_output_header(
    writer: &mut TrackedWriter,
    receipt: &Receipt,
) -> ResearchResult<()> {
    writer.write_metered_exact_bytes(b"KBOUT001")?;
    for value in [
        receipt.n,
        receipt.dim as u64,
        receipt.depth as u64,
        u64::from(receipt.b),
    ] {
        writer.write_metered_exact_bytes(&value.to_le_bytes())?;
    }
    Ok(())
}

fn stage_normalized_output_row(
    writer: &mut TrackedWriter,
    i: u64,
    endpoints: &[(i64, i64)],
    numeric_width: &mut i128,
) -> ResearchResult<()> {
    writer.write_metered_exact_bytes(&i.to_le_bytes())?;
    for &(lo, hi) in endpoints {
        let midpoint = add_checked_integer_values(i128::from(lo), i128::from(hi))?;
        if lo > hi || midpoint.abs() >= (1_i128 << 53) {
            return Err("binary64 midpoint exactness admission".into());
        }
        let value = (midpoint as f64) * (1.0 / 4294967296.0);
        if !value.is_finite() {
            return Err("nonfinite output".into());
        }
        *numeric_width = (*numeric_width).max(i128::from(hi) - i128::from(lo));
        writer.write_metered_exact_bytes(&value.to_le_bytes())?;
    }
    Ok(())
}

fn certify_observed_output_budget(
    delta: &[i128],
    eta: &[i128],
    minima: &[u128],
    dim: usize,
    numeric_width: i128,
) -> ResearchResult<i128> {
    if delta.len() != eta.len() || minima.len() != delta.len() + 1 {
        return Err("certificate length mismatch".into());
    }
    let root = calculate_integer_square_root(dim as u128);
    let c = (root + u128::from(root * root < dim as u128)) as i128;
    let mut total = multiply_checked_integer_values(8, numeric_width)?;
    let mut epsilon = 0;
    for (t, &h) in minima.iter().enumerate() {
        if t > 0 {
            epsilon = add_checked_integer_values(
                epsilon,
                add_checked_integer_values(2 * delta[t - 1], eta[t - 1])?,
            )?;
        }
        if epsilon == 0 {
            continue;
        }
        if h == u128::MAX {
            return Err("incomplete observed norm minimum".into());
        }
        let radius = multiply_checked_integer_values(c, epsilon)?;
        let scaled_h = multiply_checked_integer_values(
            i128::try_from(h).map_err(|_| "norm range")?,
            ERROR_SCALE,
        )?;
        if scaled_h <= radius {
            return Err("uncertified zero or insufficient global norm floor".into());
        }
        let denominator = scaled_h - radius;
        let numerator = multiply_checked_integer_values(
            multiply_checked_integer_values(2, radius)?,
            ERROR_SCALE,
        )?;
        let beta = ceil_positive_integer_ratio(numerator, denominator)?;
        let coefficient = if t % 2 == 0 { 2 } else { 1 };
        total =
            add_checked_integer_values(total, multiply_checked_integer_values(coefficient, beta)?)?;
    }
    if total >= (1_i128 << 53) {
        return Err("reported dyadic error range".into());
    }
    Ok(total)
}

fn execute_history_replay_mode(
    source: &Source,
    out: &Path,
    sum_layout: &Rc<Layout>,
    mean_layout: &Rc<Layout>,
    receipt: &mut Receipt,
) -> ResearchResult<i128> {
    let maximum = 1_i128 << receipt.b;
    let means = receipt.mode == "mean";
    let mut history = Vec::with_capacity(receipt.depth);
    let mut deltas = vec![0; receipt.depth];
    let start = Instant::now();
    for delta in &mut deltas {
        let mut sums = PackedPlane::allocate_packed_zero_plane(sum_layout, &receipt.meter)?;
        let mut reader = open_prepared_record_reader(source, &receipt.meter)?;
        for i in 0..source.n {
            let features = read_prepared_record_features(&mut reader, i, source)?;
            let layers = replay_frozen_history_row(
                source,
                i,
                features,
                receipt.dim,
                maximum,
                &history,
                means,
                None,
            )?;
            let row = layers.last().ok_or("missing reconstructed row")?;
            for f in features {
                sums.add_packed_feature_row(f, row)?;
            }
        }
        drop(reader);
        if means {
            let (plane, error) =
                convert_completed_mean_plane(sums, source, mean_layout, out, receipt)?;
            *delta = error;
            history.push(plane);
        } else {
            history.push(sums);
        }
    }
    receipt.propagation_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let mut eta = vec![0; receipt.depth];
    let mut minima = vec![u128::MAX; receipt.depth + 1];
    let mut numeric_width = 0;
    let mut writer =
        TrackedWriter::open_metered_output_file(&out.join("result.stage"), &receipt.meter, false)?;
    write_canonical_output_header(&mut writer, receipt)?;
    let mut reader = open_prepared_record_reader(source, &receipt.meter)?;
    for i in 0..source.n {
        let features = read_prepared_record_features(&mut reader, i, source)?;
        let layers = replay_frozen_history_row(
            source,
            i,
            features,
            receipt.dim,
            maximum,
            &history,
            means,
            Some(&mut eta),
        )?;
        let mut endpoints = vec![(0, 0); receipt.dim];
        for (t, row) in layers.iter().enumerate() {
            accumulate_normalized_layer_bounds(row, t, &mut endpoints, &mut minima)?;
        }
        stage_normalized_output_row(&mut writer, i, &endpoints, &mut numeric_width)?;
        receipt.staged_rows += 1;
    }
    writer.flush_metered_output_file()?;
    let result = certify_observed_output_budget(&deltas, &eta, &minima, receipt.dim, numeric_width);
    receipt.output_seconds = start.elapsed().as_secs_f64();
    result
}

fn replay_arena_history_row(
    source: &Source,
    i: u64,
    features: [usize; 2],
    dim: usize,
    maximum: i128,
    bytes: &[u8],
    layout: &Layout,
    depth: usize,
    mut eta: Option<&mut [i128]>,
) -> ResearchResult<Vec<Vec<i128>>> {
    let mut layers = Vec::with_capacity(depth + 1);
    layers.push(initialize_seeded_integer_row(source, i, dim, maximum));
    for t in 0..depth {
        let start = t * layout.length;
        let plane = PackedSlice {
            bytes: &bytes[start..start + layout.length],
            layout,
        };
        let mut defect = 0;
        let previous = layers.last().ok_or("missing initial arena layer")?;
        let next = update_frozen_integer_row(
            source,
            features,
            previous,
            &plane,
            true,
            maximum,
            &mut defect,
        )?;
        if let Some(errors) = eta.as_deref_mut() {
            errors[t] = errors[t].max(defect);
        }
        layers.push(next);
    }
    Ok(layers)
}

fn execute_arena_replay_mode(
    source: &Source,
    out: &Path,
    sum_layout: &Rc<Layout>,
    mean_layout: &Rc<Layout>,
    receipt: &mut Receipt,
) -> ResearchResult<i128> {
    let maximum = 1_i128 << receipt.b;
    let start = Instant::now();
    let mut arena = HistoryArena::allocate_paid_history_arena(
        receipt.depth,
        sum_layout,
        mean_layout,
        &receipt.meter,
    )?;
    let mut deltas = vec![0; receipt.depth];
    for (t, delta) in deltas.iter_mut().enumerate() {
        let prefix_length = t * mean_layout.length;
        arena.clear_next_arena_workspace(prefix_length, sum_layout.length)?;
        let (prefix, tail) = arena.bytes.split_at_mut(prefix_length);
        let workspace = &mut tail[..sum_layout.length];
        {
            let mut reader = open_prepared_record_reader(source, &receipt.meter)?;
            for i in 0..source.n {
                let features = read_prepared_record_features(&mut reader, i, source)?;
                let layers = replay_arena_history_row(
                    source,
                    i,
                    features,
                    receipt.dim,
                    maximum,
                    prefix,
                    mean_layout,
                    t,
                    None,
                )?;
                let row = layers.last().ok_or("missing arena reconstruction")?;
                for f in features {
                    add_arena_workspace_row(workspace, sum_layout, f, row)?;
                }
            }
        }
        let conversion = Instant::now();
        *delta = compact_arena_mean_workspace(workspace, sum_layout, mean_layout, &source.counts)?;
        receipt.conversion_seconds += conversion.elapsed().as_secs_f64();
    }
    receipt.propagation_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let mut eta = vec![0; receipt.depth];
    let mut minima = vec![u128::MAX; receipt.depth + 1];
    let mut numeric_width = 0;
    let mut writer =
        TrackedWriter::open_metered_output_file(&out.join("result.stage"), &receipt.meter, false)?;
    write_canonical_output_header(&mut writer, receipt)?;
    let mut reader = open_prepared_record_reader(source, &receipt.meter)?;
    for i in 0..source.n {
        let features = read_prepared_record_features(&mut reader, i, source)?;
        let layers = replay_arena_history_row(
            source,
            i,
            features,
            receipt.dim,
            maximum,
            &arena.bytes,
            mean_layout,
            receipt.depth,
            Some(&mut eta),
        )?;
        let mut endpoints = vec![(0, 0); receipt.dim];
        for (t, row) in layers.iter().enumerate() {
            accumulate_normalized_layer_bounds(row, t, &mut endpoints, &mut minima)?;
        }
        stage_normalized_output_row(&mut writer, i, &endpoints, &mut numeric_width)?;
        receipt.staged_rows += 1;
    }
    writer.flush_metered_output_file()?;
    let result = certify_observed_output_budget(&deltas, &eta, &minima, receipt.dim, numeric_width);
    receipt.output_seconds = start.elapsed().as_secs_f64();
    result
}

fn write_raw_integer_row(writer: &mut TrackedWriter, row: &[i128]) -> ResearchResult<()> {
    for &value in row {
        let raw = i32::try_from(value).map_err(|_| "raw i32 admission")?;
        writer.write_metered_exact_bytes(&raw.to_le_bytes())?;
    }
    Ok(())
}

fn read_raw_integer_row(reader: &mut TrackedReader, dim: usize) -> ResearchResult<Vec<i128>> {
    let mut row = Vec::with_capacity(dim);
    for _ in 0..dim {
        let mut bytes = [0; 4];
        reader.read_metered_exact_bytes(&mut bytes)?;
        row.push(i128::from(i32::from_le_bytes(bytes)));
    }
    Ok(row)
}

#[derive(Clone, Copy)]
struct EndpointCodec {
    center_bytes: usize,
    center_bound: i128,
    width_bound: i128,
}

impl EndpointCodec {
    fn calculate_endpoint_record_bytes(self) -> usize {
        if self.center_bytes == 0 {
            16
        } else {
            self.center_bytes + 1
        }
    }

    fn describe_endpoint_storage_codec(self) -> &'static str {
        match self.center_bytes {
            0 => "two_signed_i64_le",
            4 => "lossless_C6_signed_i32_le_center_plus_u8_width",
            5 => "lossless_C6_signed_i40_le_center_plus_u8_width",
            _ => "invalid_codec",
        }
    }
}

fn select_endpoint_storage_codec(depth: usize, compact: bool) -> ResearchResult<EndpointCodec> {
    if depth > 64 {
        return Err("endpoint depth admission".into());
    }
    if !compact {
        return Ok(EndpointCodec {
            center_bytes: 0,
            center_bound: 0,
            width_bound: 0,
        });
    }
    // Each normalized endpoint has magnitude <=2^28 and spread <=2.
    // Thus the full-depth center is bounded by 2^29*A and width by 2*A<=196.
    let absolute = (2 * (depth / 2 + 1) + (depth + 1) / 2) as i128;
    Ok(EndpointCodec {
        center_bytes: if depth <= 1 { 4 } else { 5 },
        center_bound: multiply_checked_integer_values(1_i128 << 29, absolute)?,
        width_bound: 2 * absolute,
    })
}

fn validate_center_endpoint_values(
    center: i128,
    width: i128,
    codec: EndpointCodec,
) -> ResearchResult<()> {
    if !matches!(codec.center_bytes, 4 | 5)
        || width < 0
        || width > codec.width_bound
        || center
            .checked_abs()
            .ok_or("endpoint center magnitude overflow")?
            > codec.center_bound
    {
        return Err("endpoint center or width outside depth bound".into());
    }
    if center.rem_euclid(2) != width % 2 {
        return Err("endpoint center/width parity mismatch".into());
    }
    Ok(())
}

fn encode_center_endpoint_cell(
    bytes: &mut [u8],
    pair: (i64, i64),
    codec: EndpointCodec,
) -> ResearchResult<()> {
    if bytes.len() != codec.center_bytes + 1 {
        return Err("endpoint compact record length".into());
    }
    let center = add_checked_integer_values(i128::from(pair.0), i128::from(pair.1))?;
    let width = i128::from(pair.1) - i128::from(pair.0);
    validate_center_endpoint_values(center, width, codec)?;
    write_packed_signed_cell(&mut bytes[..codec.center_bytes], center)?;
    bytes[codec.center_bytes] = u8::try_from(width).map_err(|_| "endpoint width byte overflow")?;
    Ok(())
}

fn decode_center_endpoint_cell(bytes: &[u8], codec: EndpointCodec) -> ResearchResult<(i64, i64)> {
    if bytes.len() != codec.center_bytes + 1 {
        return Err("endpoint compact record length".into());
    }
    let center = read_packed_signed_cell(&bytes[..codec.center_bytes])?;
    let width = i128::from(bytes[codec.center_bytes]);
    validate_center_endpoint_values(center, width, codec)?;
    let lo = add_checked_integer_values(center, -width)? / 2;
    let hi = add_checked_integer_values(center, width)? / 2;
    Ok((
        i64::try_from(lo).map_err(|_| "endpoint lower overflow")?,
        i64::try_from(hi).map_err(|_| "endpoint upper overflow")?,
    ))
}

fn calculate_declared_file_lengths(
    n: u64,
    dim: usize,
    endpoint_bytes: usize,
) -> ResearchResult<(u64, u64)> {
    if !matches!(endpoint_bytes, 0 | 5 | 6 | 16) {
        return Err("endpoint record size admission".into());
    }
    let dim = u64::try_from(dim).map_err(|_| "file dimension overflow")?;
    let record = dim
        .checked_mul(8)
        .and_then(|x| x.checked_add(8))
        .ok_or("output row length overflow")?;
    let output = n
        .checked_mul(record)
        .and_then(|x| x.checked_add(40))
        .ok_or("output length overflow")?;
    let source = n
        .checked_mul(16)
        .and_then(|x| x.checked_add(32))
        .ok_or("source length overflow")?;
    let state = if endpoint_bytes == 0 {
        0
    } else {
        n.checked_mul(dim)
            .and_then(|x| x.checked_mul(4 + endpoint_bytes as u64))
            .ok_or("disk state length overflow")?
    };
    let total = output
        .checked_add(source)
        .and_then(|x| x.checked_add(state))
        .ok_or("declared file length overflow")?;
    Ok((output, total))
}

fn write_output_endpoint_row(
    writer: &mut TrackedWriter,
    endpoints: &[(i64, i64)],
    codec: EndpointCodec,
) -> ResearchResult<()> {
    for &(lo, hi) in endpoints {
        if codec.center_bytes == 0 {
            writer.write_metered_exact_bytes(&lo.to_le_bytes())?;
            writer.write_metered_exact_bytes(&hi.to_le_bytes())?;
        } else {
            let mut bytes = [0; 6];
            let record = &mut bytes[..codec.calculate_endpoint_record_bytes()];
            encode_center_endpoint_cell(record, (lo, hi), codec)?;
            writer.write_metered_exact_bytes(record)?;
        }
    }
    Ok(())
}

fn read_output_endpoint_row(
    reader: &mut TrackedReader,
    dim: usize,
    codec: EndpointCodec,
) -> ResearchResult<Vec<(i64, i64)>> {
    let mut row = Vec::with_capacity(dim);
    for _ in 0..dim {
        if codec.center_bytes == 0 {
            let mut a = [0; 8];
            let mut b = [0; 8];
            reader.read_metered_exact_bytes(&mut a)?;
            reader.read_metered_exact_bytes(&mut b)?;
            row.push((i64::from_le_bytes(a), i64::from_le_bytes(b)));
        } else {
            let mut bytes = [0; 6];
            let record = &mut bytes[..codec.calculate_endpoint_record_bytes()];
            reader.read_metered_exact_bytes(record)?;
            row.push(decode_center_endpoint_cell(record, codec)?);
        }
    }
    Ok(row)
}

fn execute_fused_disk_mode(
    source: &Source,
    out: &Path,
    sum_layout: &Rc<Layout>,
    mean_layout: &Rc<Layout>,
    receipt: &mut Receipt,
) -> ResearchResult<i128> {
    let start = Instant::now();
    let maximum = 1_i128 << receipt.b;
    let codec = select_endpoint_storage_codec(receipt.depth, receipt.mode == "disk6")?;
    let raw_path = out.join("raw.bin");
    let endpoints_path = out.join("endpoints.bin");
    let mut deltas = vec![0; receipt.depth];
    let mut eta = vec![0; receipt.depth];
    let mut minima = vec![u128::MAX; receipt.depth + 1];
    let mut sums = if receipt.depth > 0 {
        Some(PackedPlane::allocate_packed_zero_plane(
            sum_layout,
            &receipt.meter,
        )?)
    } else {
        None
    };
    {
        let mut reader = open_prepared_record_reader(source, &receipt.meter)?;
        let mut raw = TrackedWriter::open_metered_output_file(&raw_path, &receipt.meter, false)?;
        let mut output =
            TrackedWriter::open_metered_output_file(&endpoints_path, &receipt.meter, false)?;
        for i in 0..source.n {
            let features = read_prepared_record_features(&mut reader, i, source)?;
            let row = initialize_seeded_integer_row(source, i, receipt.dim, maximum);
            if let Some(plane) = &mut sums {
                for f in features {
                    plane.add_packed_feature_row(f, &row)?;
                }
            }
            let mut endpoints = vec![(0, 0); receipt.dim];
            accumulate_normalized_layer_bounds(&row, 0, &mut endpoints, &mut minima)?;
            write_raw_integer_row(&mut raw, &row)?;
            write_output_endpoint_row(&mut output, &endpoints, codec)?;
        }
        raw.flush_metered_output_file()?;
        output.flush_metered_output_file()?;
    }
    for t in 0..receipt.depth {
        let current = sums.take().ok_or("missing fused accumulator")?;
        let (means, delta) =
            convert_completed_mean_plane(current, source, mean_layout, out, receipt)?;
        deltas[t] = delta;
        // The next exact accumulator overlaps only the current frozen mean plane.
        let mut next = if t + 1 < receipt.depth {
            Some(PackedPlane::allocate_packed_zero_plane(
                sum_layout,
                &receipt.meter,
            )?)
        } else {
            None
        };
        {
            let mut source_reader = open_prepared_record_reader(source, &receipt.meter)?;
            let mut raw_reader = TrackedReader::open_metered_input_file(&raw_path, &receipt.meter)?;
            let mut raw_writer =
                TrackedWriter::open_metered_output_file(&raw_path, &receipt.meter, true)?;
            let mut endpoint_reader =
                TrackedReader::open_metered_input_file(&endpoints_path, &receipt.meter)?;
            let mut endpoint_writer =
                TrackedWriter::open_metered_output_file(&endpoints_path, &receipt.meter, true)?;
            for i in 0..source.n {
                let features = read_prepared_record_features(&mut source_reader, i, source)?;
                let old = read_raw_integer_row(&mut raw_reader, receipt.dim)?;
                let row = update_frozen_integer_row(
                    source,
                    features,
                    &old,
                    &means,
                    true,
                    maximum,
                    &mut eta[t],
                )?;
                let mut endpoints =
                    read_output_endpoint_row(&mut endpoint_reader, receipt.dim, codec)?;
                accumulate_normalized_layer_bounds(&row, t + 1, &mut endpoints, &mut minima)?;
                if let Some(plane) = &mut next {
                    for f in features {
                        plane.add_packed_feature_row(f, &row)?;
                    }
                }
                // Both complete old rows have been consumed before either write.
                write_raw_integer_row(&mut raw_writer, &row)?;
                write_output_endpoint_row(&mut endpoint_writer, &endpoints, codec)?;
            }
            raw_writer.flush_metered_output_file()?;
            endpoint_writer.flush_metered_output_file()?;
        }
        drop(means);
        sums = next;
    }
    receipt.propagation_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let mut numeric_width = 0;
    {
        let mut reader = TrackedReader::open_metered_input_file(&endpoints_path, &receipt.meter)?;
        let mut writer = TrackedWriter::open_metered_output_file(
            &out.join("result.stage"),
            &receipt.meter,
            false,
        )?;
        write_canonical_output_header(&mut writer, receipt)?;
        for i in 0..source.n {
            let endpoints = read_output_endpoint_row(&mut reader, receipt.dim, codec)?;
            stage_normalized_output_row(&mut writer, i, &endpoints, &mut numeric_width)?;
            receipt.staged_rows += 1;
        }
        writer.flush_metered_output_file()?;
    }
    remove_metered_owned_file(&raw_path, &receipt.meter)?;
    remove_metered_owned_file(&endpoints_path, &receipt.meter)?;
    let result = certify_observed_output_budget(&deltas, &eta, &minima, receipt.dim, numeric_width);
    receipt.output_seconds = start.elapsed().as_secs_f64();
    result
}

fn execute_admitted_workflow_inner(
    source_path: &Path,
    out: &Path,
    tolerance: &str,
    receipt: &mut Receipt,
) -> ResearchResult<()> {
    if !["sum", "mean", "disk", "arena", "disk6"].contains(&receipt.mode.as_str()) {
        return Err("MODE must be sum, mean, disk, arena, or disk6".into());
    }
    if !(1..=256).contains(&receipt.dim) || receipt.depth > 64 || receipt.b > 30 {
        return Err("arithmetic admission: 1<=D<=256, 0<=L<=64, 0<=B<=30".into());
    }
    receipt.tolerance_units = Some(parse_exact_decimal_tolerance(tolerance)?);
    let uses_disk = matches!(receipt.mode.as_str(), "disk" | "disk6");
    if uses_disk {
        let codec = select_endpoint_storage_codec(receipt.depth, receipt.mode == "disk6")?;
        receipt.endpoint_record_bytes = codec.calculate_endpoint_record_bytes();
        receipt.endpoint_codec = codec.describe_endpoint_storage_codec();
    } else {
        receipt.endpoint_codec = "none";
    }
    let start = Instant::now();
    let prepared = prepare_validated_source_copy(source_path, out, &receipt.meter);
    receipt.prep_seconds = start.elapsed().as_secs_f64();
    let source = prepared?;
    receipt.n = source.n;
    receipt.features = source.features;
    let (output_length, declared_file_bytes) =
        calculate_declared_file_lengths(source.n, receipt.dim, receipt.endpoint_record_bytes)?;
    if declared_file_bytes > DISK_CAP {
        return Err("declared file lengths exceed owned disk cap".into());
    }
    let maximum = 1_i128 << receipt.b;
    // D*M^2*2^56 <= 2^124 under the admitted envelope, below i128::MAX.
    let mut sum_layout = build_shared_feature_layout(&source, receipt.dim, maximum, false)?;
    let mean_layout = build_shared_feature_layout(&source, receipt.dim, maximum, true)?;
    if receipt.mode == "arena" {
        promote_arena_workspace_layout(&mut sum_layout, &mean_layout)?;
    }
    receipt.metadata_bytes = (source.counts.capacity() * 8
        + (sum_layout.offsets.capacity()
            + sum_layout.widths.capacity()
            + mean_layout.offsets.capacity()
            + mean_layout.widths.capacity())
            * std::mem::size_of::<usize>()) as u64;
    receipt.local_scratch_bytes = ((receipt.depth + 1) * (receipt.dim * 16 + 24)
        + receipt.dim * 96
        + (receipt.depth + 1) * 64
        + 6 * IO_BYTES) as u64;
    let bound = if receipt.mode == "arena" {
        execute_arena_replay_mode(&source, out, &sum_layout, &mean_layout, receipt)?
    } else if uses_disk {
        execute_fused_disk_mode(&source, out, &sum_layout, &mean_layout, receipt)?
    } else {
        execute_history_replay_mode(&source, out, &sum_layout, &mean_layout, receipt)?
    };
    receipt.bound_units = Some(bound);
    if bound
        > receipt
            .tolerance_units
            .ok_or("missing tolerance admission")?
    {
        return Err("observed complete-output bound exceeds TAU".into());
    }
    let staged = out.join("result.stage");
    if receipt.staged_rows != source.n
        || convert_io_error_result(fs::metadata(&staged))?.len() != output_length
    {
        return Err("incomplete staged output".into());
    }
    convert_io_error_result(File::open(&staged))?
        .sync_all()
        .map_err(|error| error.to_string())?;
    convert_io_error_result(fs::rename(staged, out.join("result.bin")))?;
    receipt.rows = source.n;
    receipt.output_bytes = output_length;
    Ok(())
}

fn run_packed_workflow_job(
    mode: &str,
    source: &Path,
    out: &Path,
    dim: usize,
    depth: usize,
    b: u32,
    tolerance: &str,
) -> ResearchResult<Receipt> {
    let start = Instant::now();
    let mut receipt = Receipt {
        mode: mode.into(),
        dim,
        depth,
        b,
        ..Receipt::default()
    };
    if let Err(error) = fs::create_dir(out) {
        receipt.reason = format!("OUTDIR must be new: {error}");
        receipt.total_seconds = start.elapsed().as_secs_f64();
        return Ok(receipt);
    }
    match execute_admitted_workflow_inner(source, out, tolerance, &mut receipt) {
        Ok(()) => {
            receipt.admitted = true;
            receipt.reason = "complete".into();
        }
        Err(error) => {
            receipt.reason = error;
            for name in [
                "result.bin",
                "result.stage",
                "raw.bin",
                "endpoints.bin",
                "conversion.bin",
            ] {
                let path = out.join(name);
                if path.exists() {
                    if let Err(cleanup) = remove_metered_owned_file(&path, &receipt.meter) {
                        receipt.reason.push_str(&format!("; cleanup: {cleanup}"));
                    }
                }
            }
        }
    }
    receipt.total_seconds = start.elapsed().as_secs_f64();
    Ok(receipt)
}

fn encode_json_string_value(text: &str) -> String {
    let mut value = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => value.push_str("\\\""),
            '\\' => value.push_str("\\\\"),
            ch if ch.is_control() => value.push_str(&format!("\\u{:04x}", ch as u32)),
            _ => value.push(ch),
        }
    }
    value.push('"');
    value
}

fn format_workflow_json_receipt(receipt: &Receipt) -> String {
    let meter = receipt.meter.borrow();
    let bound = receipt.bound_units.map_or("null".into(), |x| {
        format!("{}", x as f64 / BUDGET_SCALE as f64)
    });
    let exact = receipt.bound_units.map_or("null".into(), |x| x.to_string());
    let tolerance = receipt
        .tolerance_units
        .map_or("null".into(), |x| x.to_string());
    format!(concat!("{{\"admitted\":{},\"mode\":{},\"reason\":{},\"n\":{},\"f\":{},\"d\":{},\"l\":{},\"b\":{},",
        "\"rows\":{},\"staged_rows\":{},\"output_bytes\":{},\"error_bound\":{},\"bound_units_2neg35\":{},\"tau_floor_units_2neg35\":{},",
        "\"prepare_seconds\":{:.9},\"propagation_seconds\":{:.9},\"output_seconds\":{:.9},\"conversion_seconds\":{:.9},\"total_seconds\":{:.9},",
        "\"logical_bytes_read\":{},\"logical_bytes_written\":{},\"conversion_bytes_read\":{},\"conversion_bytes_written\":{},",
        "\"peak_packed_plane_payload\":{},\"peak_disk_bytes\":{},\"retained_disk_bytes\":{},\"metadata_bytes\":{},\"local_scratch_allowance_bytes\":{},",
        "\"endpoint_record_bytes\":{},\"endpoint_codec\":{},",
        "\"accounting_scope\":\"packed Vec capacities; owned OUTDIR logical file lengths including prepared copy, scratch, staging and result; excludes supplied source, filesystem allocation and RSS/page cache\",",
        "\"phase_scope\":\"conversion is included in propagation; disk propagation includes initialization and normalization; replay output includes replay and normalization; total includes admission, layout, sync, rename and cleanup; an interrupted phase can report zero while total still includes its elapsed time\",",
        "\"disk_schedule\":\"fused in-place raw/endpoints with current means plus next exact sums; restart from source on failure\",",
        "\"omitted_disk_optimizations\":\"initial raw/endpoint file elision and direct final-transition output staging are not implemented; not an optimal-baseline claim\",",
        "\"initialization\":\"constant first coordinate; splitmix64 ternary other coordinates\",\"packed_cap_bytes\":{},\"owned_disk_cap_bytes\":{}}}"),
        receipt.admitted,encode_json_string_value(&receipt.mode),encode_json_string_value(&receipt.reason),receipt.n,receipt.features,receipt.dim,receipt.depth,receipt.b,
        receipt.rows,receipt.staged_rows,receipt.output_bytes,bound,exact,tolerance,
        receipt.prep_seconds,receipt.propagation_seconds,receipt.output_seconds,receipt.conversion_seconds,receipt.total_seconds,
        meter.read,meter.written,meter.conversion_read,meter.conversion_written,meter.packed_peak,meter.disk_peak,meter.disk_live,
        receipt.metadata_bytes,receipt.local_scratch_bytes,receipt.endpoint_record_bytes,encode_json_string_value(receipt.endpoint_codec),PACKED_CAP,DISK_CAP)
}

fn dispatch_workflow_cli_command(args: &[String]) -> ResearchResult<bool> {
    match args.get(1).map(String::as_str) {
        Some("generate") if args.len() == 6 => {
            let start = Instant::now();
            let n = args[3].parse::<u64>().map_err(|_| "invalid N")?;
            let features = args[4].parse::<u64>().map_err(|_| "invalid F")?;
            let seed = args[5].parse::<u64>().map_err(|_| "invalid SEED")?;
            generate_canonical_source_file(Path::new(&args[2]), n, features, seed)?;
            println!("{{\"generated\":true,\"n\":{n},\"f\":{features},\"seed\":{seed},\"source_bytes\":{},\"generate_seconds\":{:.9},\"generator_hot_prefix\":\"max(1,F/8)\"}}",32+16*n,start.elapsed().as_secs_f64());
            Ok(true)
        }
        Some("run") if args.len() == 9 => {
            let dim = args[5].parse::<usize>().map_err(|_| "invalid D")?;
            let depth = args[6].parse::<usize>().map_err(|_| "invalid L")?;
            let b = args[7].parse::<u32>().map_err(|_| "invalid B")?;
            let receipt = run_packed_workflow_job(
                &args[2],
                Path::new(&args[3]),
                Path::new(&args[4]),
                dim,
                depth,
                b,
                &args[8],
            )?;
            println!("{}", format_workflow_json_receipt(&receipt));
            Ok(receipt.admitted)
        }
        _ => Err("usage: generate SOURCE N F SEED | run MODE SOURCE OUTDIR D L B TAU".into()),
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    match dispatch_workflow_cli_command(&args) {
        Ok(true) => {}
        Ok(false) => std::process::exit(2),
        Err(error) => {
            println!(
                "{{\"admitted\":false,\"reason\":{}}}",
                encode_json_string_value(&error)
            );
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn create_unique_test_directory() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "kb-packed-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn test_signed_packed_boundaries() {
        for width in 1..=16 {
            let values = if width == 16 {
                vec![i128::MIN, -1, 0, i128::MAX]
            } else {
                let limit = 1_i128 << (8 * width - 1);
                vec![-limit, -1, 0, limit - 1]
            };
            for value in values {
                let mut bytes = vec![0; width];
                write_packed_signed_cell(&mut bytes, value).unwrap();
                assert_eq!(read_packed_signed_cell(&bytes).unwrap(), value);
            }
        }
        assert!(write_packed_signed_cell(&mut [0], 128).is_err());
        assert!(write_packed_signed_cell(&mut [], 0).is_err());
    }

    #[test]
    fn test_signed_rounding_ties() {
        for (num, want) in [
            (-7, -4),
            (-5, -2),
            (-3, -2),
            (-1, 0),
            (1, 0),
            (3, 2),
            (5, 2),
            (7, 4),
        ] {
            assert_eq!(round_exact_integer_even(num, 2).unwrap(), want);
        }
        assert!(round_exact_integer_even(1, 0).is_err());
    }

    #[test]
    fn test_normalized_signed_enclosures() {
        let (bounds, h) = enclose_normalized_integer_row(&[3, -4]).unwrap();
        assert_eq!(h, 5);
        for ((lo, hi), numerator) in bounds.into_iter().zip([3_i128, -4]) {
            assert!(5 * i128::from(lo) <= numerator * (1 << 28));
            assert!(5 * i128::from(hi) >= numerator * (1 << 28));
        }
        assert_eq!(
            enclose_normalized_integer_row(&[0, 0]).unwrap(),
            (vec![(0, 0); 2], 0)
        );
    }

    #[test]
    fn test_malformed_source_refusal() {
        let dir = create_unique_test_directory();
        let source = dir.join("bad.bin");
        fs::write(&source, b"not canonical").unwrap();
        let result = run_packed_workflow_job("mean", &source, &dir.join("out"), 3, 2, 20, "0.0001");
        assert!(
            result.is_ok(),
            "a failed run still has a structured receipt"
        );
        assert!(!result.unwrap().admitted);
        assert!(!dir.join("out/result.bin").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_complete_modes_equality() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 24, 6, 81).unwrap();
        for mode in ["sum", "mean", "disk"] {
            let receipt =
                run_packed_workflow_job(mode, &source, &dir.join(mode), 4, 3, 24, "0.0001")
                    .unwrap();
            assert!(receipt.admitted);
            assert_eq!(receipt.rows, 24);
            assert_eq!(
                fs::metadata(dir.join(mode).join("result.bin"))
                    .unwrap()
                    .len(),
                40 + 24 * (8 + 4 * 8)
            );
        }
        assert_eq!(
            fs::read(dir.join("mean/result.bin")).unwrap(),
            fs::read(dir.join("disk/result.bin")).unwrap()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_failed_output_cleanup() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 18, 5, 7).unwrap();
        for mode in ["sum", "mean", "disk"] {
            let output = dir.join(mode);
            let receipt =
                run_packed_workflow_job(mode, &source, &output, 4, 3, 0, "0.000000000001").unwrap();
            assert!(!receipt.admitted);
            assert!(!output.join("result.bin").exists());
            assert!(!output.join("result.stage").exists());
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_decimal_budget_boundaries() {
        assert_eq!(parse_exact_decimal_tolerance("0").unwrap(), 0);
        assert_eq!(
            parse_exact_decimal_tolerance("0.125").unwrap(),
            BUDGET_SCALE / 8
        );
        assert_eq!(parse_exact_decimal_tolerance("0.000000000001").unwrap(), 0);
        for text in ["NaN", "inf", "-1", "1e-4", ".1", "1.", "0.0000000000001"] {
            assert!(parse_exact_decimal_tolerance(text).is_err());
        }
        assert!(parse_exact_decimal_tolerance("999999999999999999999999999999999999999").is_err());
    }

    #[test]
    fn test_feature_local_layouts() {
        let source = Source {
            n: 258,
            features: 3,
            seed: 0,
            counts: vec![2, 256, 0],
            prepared: PathBuf::new(),
        };
        let sum = build_shared_feature_layout(&source, 3, 1, false).unwrap();
        let mean = build_shared_feature_layout(&source, 3, 1, true).unwrap();
        assert_eq!(sum.widths, vec![1, 2, 1]);
        assert_eq!(sum.length, 12);
        assert_eq!(mean.length, 9);
        let meter = SharedMeter::default();
        let first = PackedPlane::allocate_packed_zero_plane(&sum, &meter).unwrap();
        let second = PackedPlane::allocate_packed_zero_plane(&sum, &meter).unwrap();
        assert!(Rc::ptr_eq(&first.layout, &second.layout));
        assert_eq!(meter.borrow().packed_peak, 24);
        drop(first);
        drop(second);
        assert_eq!(meter.borrow().packed_live, 0);
    }

    #[test]
    fn test_malformed_record_contracts() {
        let dir = create_unique_test_directory();
        let path = dir.join("good.bin");
        generate_canonical_source_file(&path, 12, 3, 6).unwrap();
        let good = fs::read(path).unwrap();
        for case in 0..5 {
            let mut data = good.clone();
            match case {
                0 => data.push(0),
                1 => {
                    data.pop();
                }
                2 => data[32] = 1,
                3 => data[40..44].copy_from_slice(&3_u32.to_le_bytes()),
                _ => {
                    let feature = data[40..44].to_vec();
                    data[44..48].copy_from_slice(&feature);
                }
            }
            let source = dir.join(format!("bad{case}.bin"));
            fs::write(&source, data).unwrap();
            let output = dir.join(format!("out{case}"));
            let receipt =
                run_packed_workflow_job("sum", &source, &output, 3, 2, 16, "0.001").unwrap();
            assert!(!receipt.admitted);
            assert!(!output.join("result.bin").exists());
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_disk_cursor_boundaries() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 600, 17, 23).unwrap();
        for mode in ["mean", "disk"] {
            let result =
                run_packed_workflow_job(mode, &source, &dir.join(mode), 32, 2, 26, "0.001")
                    .unwrap();
            assert!(result.admitted, "{}", result.reason);
            assert_eq!(result.meter.borrow().packed_live, 0);
            assert_eq!(
                result.meter.borrow().disk_live,
                32 + 600 * 16 + 40 + 600 * (8 + 32 * 8)
            );
            assert_eq!(
                result.meter.borrow().conversion_read,
                result.meter.borrow().conversion_written
            );
        }
        assert_eq!(
            fs::read(dir.join("mean/result.bin")).unwrap(),
            fs::read(dir.join("disk/result.bin")).unwrap()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_zero_depth_admission() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 8, 2, 0).unwrap();
        for mode in ["sum", "mean", "disk"] {
            let output = dir.join(mode);
            let result =
                run_packed_workflow_job(mode, &source, &output, 4, 0, 30, "0.001").unwrap();
            assert!(result.admitted, "{}", result.reason);
            let second =
                run_packed_workflow_job(mode, &source, &output, 4, 0, 30, "0.001").unwrap();
            assert!(!second.admitted);
            assert!(
                output.join("result.bin").exists(),
                "existing owned output must not be deleted"
            );
        }
        assert_eq!(
            fs::read(dir.join("sum/result.bin")).unwrap(),
            fs::read(dir.join("disk/result.bin")).unwrap()
        );
        let (endpoints, h) = enclose_normalized_integer_row(&vec![1_i128 << 30; 256]).unwrap();
        assert_eq!(h, 16 * (1_u128 << 30));
        assert_eq!(endpoints.len(), 256);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_arena_complete_equality() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 300, 9, 81).unwrap();
        let mut reference = None;
        for mode in ["mean", "disk", "arena"] {
            let output = dir.join(mode);
            let receipt =
                run_packed_workflow_job(mode, &source, &output, 8, 4, 26, "0.001").unwrap();
            assert!(receipt.admitted, "{}", receipt.reason);
            let bytes = fs::read(output.join("result.bin")).unwrap();
            if let Some(expected) = &reference {
                assert_eq!(&bytes, expected);
            } else {
                reference = Some(bytes);
            }
            if mode == "arena" {
                let meter = receipt.meter.borrow();
                assert_eq!(meter.conversion_read, 0);
                assert_eq!(meter.conversion_written, 0);
                assert_eq!(meter.packed_live, 0);
                assert!(meter.packed_peak >= 4 * 9 * 8 * 4);
                assert_eq!(meter.disk_live, 32 + 300 * 16 + 40 + 300 * (8 + 8 * 8));
                assert!(!output.join("conversion.bin").exists());
            }
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_arena_inactive_features() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 12, 4, 72).unwrap();
        let mut bytes = fs::read(&source).unwrap();
        for i in 0..12 {
            bytes[32 + i * 16 + 8..32 + i * 16 + 12].copy_from_slice(&0_u32.to_le_bytes());
            bytes[32 + i * 16 + 12..32 + i * 16 + 16].copy_from_slice(&2_u32.to_le_bytes());
        }
        fs::write(&source, bytes).unwrap();
        for mode in ["mean", "arena"] {
            let receipt =
                run_packed_workflow_job(mode, &source, &dir.join(mode), 5, 4, 26, "0.001").unwrap();
            assert!(receipt.admitted, "{}", receipt.reason);
            if mode == "arena" {
                assert!(receipt.meter.borrow().packed_peak >= 4 * 4 * 5 * 4);
            }
        }
        assert_eq!(
            fs::read(dir.join("mean/result.bin")).unwrap(),
            fs::read(dir.join("arena/result.bin")).unwrap()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_arena_refusal_zero_depth() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 18, 5, 7).unwrap();
        let zero = run_packed_workflow_job("arena", &source, &dir.join("zero"), 4, 0, 26, "0.001")
            .unwrap();
        assert!(zero.admitted, "{}", zero.reason);
        assert_eq!(zero.meter.borrow().packed_peak, 0);
        let refused = dir.join("refused");
        let result =
            run_packed_workflow_job("arena", &source, &refused, 4, 3, 0, "0.000000000001").unwrap();
        assert!(!result.admitted);
        assert_eq!(result.staged_rows, 18);
        assert_eq!(result.meter.borrow().packed_live, 0);
        for name in ["result.bin", "result.stage", "conversion.bin"] {
            assert!(!refused.join(name).exists());
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_arena_compaction_preserves_prefix() {
        let dir = create_unique_test_directory();
        let source = Source {
            n: 1030,
            features: 5,
            seed: 0,
            counts: vec![0, 2, 1024, 0, 3],
            prepared: PathBuf::new(),
        };
        let original = build_shared_feature_layout(&source, 3, 64, false).unwrap();
        let means = build_shared_feature_layout(&source, 3, 64, true).unwrap();
        let mut workspace = build_shared_feature_layout(&source, 3, 64, false).unwrap();
        assert!(workspace.widths[0] < means.widths[0]);
        promote_arena_workspace_layout(&mut workspace, &means).unwrap();
        assert!(workspace
            .widths
            .iter()
            .zip(&means.widths)
            .all(|(h, c)| h >= c));
        let prefix = 2 * means.length;
        let mut arena = vec![0xa5; prefix + workspace.length + 7];
        let values = [
            [0, 0, 0],
            [-128, 127, 3],
            [-65536, 65535, -1025],
            [0, 0, 0],
            [192, -192, -1],
        ];
        let mut receipt = Receipt::default();
        let mut separate =
            PackedPlane::allocate_packed_zero_plane(&original, &receipt.meter).unwrap();
        for (f, row) in values.iter().enumerate() {
            for (j, &value) in row.iter().enumerate() {
                separate.write_packed_feature_value(f, j, value).unwrap();
                let start = prefix + workspace.offsets[f] + j * workspace.widths[f];
                write_packed_signed_cell(&mut arena[start..start + workspace.widths[f]], value)
                    .unwrap();
            }
        }
        let (expected, defect) =
            convert_completed_mean_plane(separate, &source, &means, &dir, &mut receipt).unwrap();
        let actual = compact_arena_mean_workspace(
            &mut arena[prefix..prefix + workspace.length],
            &workspace,
            &means,
            &source.counts,
        )
        .unwrap();
        assert_eq!(actual, defect);
        assert_eq!(
            &arena[prefix..prefix + means.length],
            expected.bytes.as_slice()
        );
        assert!(arena[..prefix].iter().all(|&x| x == 0xa5));
        assert!(arena[prefix + workspace.length..]
            .iter()
            .all(|&x| x == 0xa5));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_arena_capacity_workspace_clear() {
        let source = Source {
            n: 1030,
            features: 5,
            seed: 0,
            counts: vec![0, 2, 1024, 0, 3],
            prepared: PathBuf::new(),
        };
        let means = build_shared_feature_layout(&source, 3, 64, true).unwrap();
        let mut workspace = build_shared_feature_layout(&source, 3, 64, false).unwrap();
        let mut invalid = vec![0x4a; workspace.length];
        let unchanged = invalid.clone();
        assert!(
            compact_arena_mean_workspace(&mut invalid, &workspace, &means, &source.counts).is_err()
        );
        assert_eq!(invalid, unchanged);
        promote_arena_workspace_layout(&mut workspace, &means).unwrap();
        let meter = SharedMeter::default();
        let mut arena =
            HistoryArena::allocate_paid_history_arena(3, &workspace, &means, &meter).unwrap();
        assert_eq!(
            arena.bytes.len(),
            (2 * means.length + workspace.length).max(3 * means.length)
        );
        assert_eq!(meter.borrow().packed_peak, arena.bytes.capacity() as u64);
        arena.bytes.fill(0x4a);
        arena
            .clear_next_arena_workspace(means.length, workspace.length)
            .unwrap();
        assert!(arena.bytes[..means.length].iter().all(|&x| x == 0x4a));
        assert!(arena.bytes[means.length..means.length + workspace.length]
            .iter()
            .all(|&x| x == 0));
        assert!(arena.bytes[means.length + workspace.length..]
            .iter()
            .all(|&x| x == 0x4a));
        drop(arena);
        assert_eq!(meter.borrow().packed_live, 0);
        assert!(
            HistoryArena::allocate_paid_history_arena(usize::MAX, &workspace, &means, &meter)
                .is_err()
        );
        assert_eq!(meter.borrow().packed_live, 0);
        let zero_meter = SharedMeter::default();
        let empty =
            HistoryArena::allocate_paid_history_arena(0, &workspace, &means, &zero_meter).unwrap();
        assert_eq!(empty.bytes.capacity(), 0);
        assert_eq!(zero_meter.borrow().packed_peak, 0);
    }

    #[test]
    fn test_disk6_paired_codec() {
        for depth in 0..=64 {
            let codec = select_endpoint_storage_codec(depth, true).unwrap();
            assert_eq!(codec.center_bytes, if depth <= 1 { 4 } else { 5 });
            let absolute: i128 = (0..=depth).map(|t| if t % 2 == 0 { 2 } else { 1 }).sum();
            assert_eq!(codec.center_bound, (1_i128 << 29) * absolute);
            assert_eq!(codec.width_bound, 2 * absolute);
            assert!(codec.width_bound <= 196);
            for center in [
                -codec.center_bound,
                -129,
                -128,
                -1,
                0,
                1,
                127,
                128,
                codec.center_bound,
            ] {
                for width in [0, 1, codec.width_bound] {
                    if (center - width) % 2 != 0 {
                        continue;
                    }
                    let pair = ((center - width) as i64 / 2, (center + width) as i64 / 2);
                    let mut bytes = vec![0; codec.center_bytes + 1];
                    encode_center_endpoint_cell(&mut bytes, pair, codec).unwrap();
                    assert_eq!(
                        read_packed_signed_cell(&bytes[..codec.center_bytes]).unwrap(),
                        center
                    );
                    assert_eq!(i128::from(bytes[codec.center_bytes]), width);
                    assert_eq!(decode_center_endpoint_cell(&bytes, codec).unwrap(), pair);
                }
            }
        }
        assert!(select_endpoint_storage_codec(65, true).is_err());
        let legacy = select_endpoint_storage_codec(64, false).unwrap();
        assert_eq!(legacy.center_bytes, 0);
        let codec = select_endpoint_storage_codec(64, true).unwrap();
        let mut bytes = [0; 6];
        assert!(encode_center_endpoint_cell(&mut bytes, (2, 1), codec).is_err());
        assert!(encode_center_endpoint_cell(&mut bytes, (0, 197), codec).is_err());
        let beyond = (codec.center_bound / 2 + 1) as i64;
        assert!(encode_center_endpoint_cell(&mut bytes, (beyond, beyond), codec).is_err());
        assert!(encode_center_endpoint_cell(&mut bytes, (i64::MIN, i64::MAX), codec).is_err());
        assert!(encode_center_endpoint_cell(&mut bytes[..5], (0, 0), codec).is_err());
        bytes.fill(0);
        bytes[5] = 1;
        assert!(
            decode_center_endpoint_cell(&bytes, codec).is_err(),
            "mismatched parity"
        );
        bytes[5] = 198;
        assert!(
            decode_center_endpoint_cell(&bytes, codec).is_err(),
            "excess width"
        );
        bytes[5] = 0;
        write_packed_signed_cell(&mut bytes[..5], codec.center_bound + 2).unwrap();
        assert!(
            decode_center_endpoint_cell(&bytes, codec).is_err(),
            "excess center"
        );
        assert!(decode_center_endpoint_cell(&bytes[..5], codec).is_err());
    }

    #[test]
    fn test_disk6_complete_traffic() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 12, 3, 81).unwrap();
        for depth in [0, 1, 2, 4, 64] {
            let old = dir.join(format!("disk-{depth}"));
            let compact = dir.join(format!("disk6-{depth}"));
            let a = run_packed_workflow_job("disk", &source, &old, 4, depth, 26, "0.001").unwrap();
            let b =
                run_packed_workflow_job("disk6", &source, &compact, 4, depth, 26, "0.001").unwrap();
            assert!(a.admitted, "{}", a.reason);
            assert!(b.admitted, "{}", b.reason);
            assert_eq!(a.bound_units, b.bound_units);
            assert_eq!(
                fs::read(old.join("result.bin")).unwrap(),
                fs::read(compact.join("result.bin")).unwrap()
            );
            let q = if depth <= 1 { 5 } else { 6 };
            assert_eq!(a.endpoint_record_bytes, 16);
            assert_eq!(b.endpoint_record_bytes, q as usize);
            assert!(b.endpoint_codec.contains("lossless_C6"));
            let cells = 12 * 4;
            let saved = (depth as u64 + 1) * (16 - q) * cells;
            let (ma, mb) = (a.meter.borrow(), b.meter.borrow());
            assert_eq!(ma.read - mb.read, saved);
            assert_eq!(ma.written - mb.written, saved);
            assert_eq!(ma.disk_peak - mb.disk_peak, (16 - q) * cells);
            assert_eq!(ma.disk_live, mb.disk_live);
            assert_eq!(ma.packed_peak, mb.packed_peak);
            assert_eq!(ma.conversion_read, mb.conversion_read);
            assert_eq!(ma.conversion_written, mb.conversion_written);
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_disk6_refusal_equivalence() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 18, 5, 7).unwrap();
        for b in [0, 26] {
            let mut reference = None;
            for mode in ["disk", "disk6"] {
                let output = dir.join(format!("{mode}-{b}"));
                let result = run_packed_workflow_job(mode, &source, &output, 4, 3, b, "0").unwrap();
                assert!(!result.admitted);
                assert_eq!(result.staged_rows, 18);
                if let Some((bound, reason)) = &reference {
                    assert_eq!(&result.bound_units, bound);
                    assert_eq!(&result.reason, reason);
                } else {
                    reference = Some((result.bound_units, result.reason.clone()));
                }
                assert_eq!(result.meter.borrow().packed_live, 0);
                for name in [
                    "result.bin",
                    "result.stage",
                    "endpoints.bin",
                    "raw.bin",
                    "conversion.bin",
                ] {
                    assert!(!output.join(name).exists());
                }
            }
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_disk6_reservation_width() {
        let (output, old) = calculate_declared_file_lengths(20_000_000, 128, 16).unwrap();
        let (same, compact) = calculate_declared_file_lengths(20_000_000, 128, 6).unwrap();
        assert_eq!(output, same);
        assert!(old > DISK_CAP && compact < DISK_CAP);
        assert_eq!(old - compact, 10 * 20_000_000 * 128);
        let (_, shallow) = calculate_declared_file_lengths(20_000_000, 128, 5).unwrap();
        assert_eq!(compact - shallow, 20_000_000 * 128);
        assert!(calculate_declared_file_lengths(u64::MAX, usize::MAX, 6).is_err());
    }

    #[test]
    fn test_disk6_cursor_record_boundaries() {
        let dir = create_unique_test_directory();
        let source = dir.join("source.bin");
        generate_canonical_source_file(&source, 128, 8, 93).unwrap();
        for depth in [1, 2] {
            let old = dir.join(format!("disk-{depth}"));
            let compact = dir.join(format!("disk6-{depth}"));
            let a =
                run_packed_workflow_job("disk", &source, &old, 128, depth, 26, "0.001").unwrap();
            let b = run_packed_workflow_job("disk6", &source, &compact, 128, depth, 26, "0.001")
                .unwrap();
            assert!(a.admitted && b.admitted, "{}; {}", a.reason, b.reason);
            assert_eq!(a.bound_units, b.bound_units);
            assert!(128 * 128 * b.endpoint_record_bytes > IO_BYTES);
            assert_eq!(
                fs::read(old.join("result.bin")).unwrap(),
                fs::read(compact.join("result.bin")).unwrap()
            );
            let saved = 2 * (depth as u64 + 1) * (16 - b.endpoint_record_bytes as u64) * 128 * 128;
            let (ma, mb) = (a.meter.borrow(), b.meter.borrow());
            assert_eq!(ma.read + ma.written - mb.read - mb.written, saved);
        }
        fs::remove_dir_all(dir).unwrap();
    }
}
