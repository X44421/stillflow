use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::de::{DeserializeSeed, Error as DeError, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::{Map, Value};

fn field_names(cols: usize) -> Vec<String> {
    (0..cols).map(|col| format!("c{col}")).collect()
}

fn cell_payload(row: usize, col: usize) -> String {
    let len = 32 + (row.wrapping_mul(31).wrapping_add(col.wrapping_mul(7))) % 65;
    let prefix = format!("v{row:08}_{col:02}_");
    let mut payload = prefix;
    while payload.len() < len {
        payload.push((b'a' + ((row + col + payload.len()) % 26) as u8) as char);
    }
    payload
}

fn fixture(cols: usize, rows: usize) -> Vec<u8> {
    let names = field_names(cols);
    let mut out = Vec::with_capacity(rows.saturating_mul(cols).saturating_mul(64));
    for row in 0..rows {
        out.push(b'{');
        for (col, name) in names.iter().enumerate() {
            if col > 0 {
                out.push(b',');
            }
            let payload = cell_payload(row, col);
            serde_json::to_writer(&mut out, name).expect("field name");
            out.push(b':');
            serde_json::to_writer(&mut out, &payload).expect("payload");
        }
        out.extend_from_slice(b"}\n");
    }
    out
}

fn legacy_like(bytes: &[u8], names: &[String]) -> usize {
    let mut reencoded = Vec::with_capacity(bytes.len());
    for raw in bytes.split(|byte| *byte == b'\n').filter(|raw| !raw.is_empty()) {
        let object: Map<String, Value> = serde_json::from_slice(raw).expect("legacy parse");
        assert_eq!(object.len(), names.len());
        for (name, value) in &object {
            assert!(names.iter().any(|candidate| candidate == name));
            assert!(value.is_string());
        }
        serde_json::to_writer(&mut reencoded, &Value::Object(object)).expect("legacy reencode");
        reencoded.push(b'\n');
    }

    let mut checksum = 0usize;
    for raw in reencoded
        .split(|byte| *byte == b'\n')
        .filter(|raw| !raw.is_empty())
    {
        let object: Map<String, Value> = serde_json::from_slice(raw).expect("second parse");
        for value in object.values() {
            checksum = checksum.saturating_add(value.as_str().expect("utf8").len());
        }
    }
    checksum
}

struct DirectRowSeed<'a> {
    field_index: &'a HashMap<String, usize>,
    columns: &'a mut [Vec<String>],
}

impl<'de> DeserializeSeed<'de> for DirectRowSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DirectRowVisitor {
            field_index: self.field_index,
            columns: self.columns,
        })
    }
}

struct DirectRowVisitor<'a> {
    field_index: &'a HashMap<String, usize>,
    columns: &'a mut [Vec<String>],
}

impl<'de> Visitor<'de> for DirectRowVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an object containing each established UTF-8 field exactly once")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen = vec![false; self.columns.len()];
        while let Some(name) = access.next_key::<String>()? {
            let Some(&index) = self.field_index.get(&name) else {
                return Err(A::Error::custom("unknown field"));
            };
            if seen[index] {
                return Err(A::Error::custom("duplicate field"));
            }
            seen[index] = true;
            let value = access.next_value::<String>()?;
            self.columns[index].push(value);
        }
        if seen.iter().any(|value| !*value) {
            return Err(A::Error::custom("missing required field"));
        }
        Ok(())
    }
}

fn direct_single_pass(bytes: &[u8], names: &[String], rows: usize) -> usize {
    let field_index = names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.clone(), index))
        .collect::<HashMap<_, _>>();
    let mut columns = (0..names.len())
        .map(|_| Vec::with_capacity(rows))
        .collect::<Vec<_>>();

    for raw in bytes.split(|byte| *byte == b'\n').filter(|raw| !raw.is_empty()) {
        let mut deserializer = serde_json::Deserializer::from_slice(raw);
        DirectRowSeed {
            field_index: &field_index,
            columns: &mut columns,
        }
        .deserialize(&mut deserializer)
        .expect("single-pass parse");
        deserializer.end().expect("row end");
    }

    columns
        .iter()
        .flat_map(|column| column.iter())
        .fold(0usize, |sum, value| sum.saturating_add(value.len()))
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn measure<F>(mut f: F, reps: usize) -> (Duration, usize)
where
    F: FnMut() -> usize,
{
    let mut elapsed = Vec::with_capacity(reps);
    let mut checksum = 0usize;
    for _ in 0..reps {
        let start = Instant::now();
        checksum = f();
        elapsed.push(start.elapsed());
    }
    (median(elapsed), checksum)
}

#[test]
fn compare_legacy_shape_to_indexed_single_pass() {
    // Keep the PR experiment bounded while preserving the accepted baseline's
    // width contrast. The full 100k/1M matrix remains in read_baseline.rs.
    let cases = [(10usize, 20_000usize), (100usize, 5_000usize)];
    let reps = 3usize;

    for (cols, rows) in cases {
        let names = field_names(cols);
        let bytes = fixture(cols, rows);

        // Warm both implementations once before collecting timings.
        let expected = legacy_like(&bytes, &names);
        assert_eq!(expected, direct_single_pass(&bytes, &names, rows));

        let (legacy, legacy_checksum) = measure(|| legacy_like(&bytes, &names), reps);
        let (direct, direct_checksum) = measure(|| direct_single_pass(&bytes, &names, rows), reps);
        assert_eq!(legacy_checksum, direct_checksum);

        let ratio = legacy.as_secs_f64() / direct.as_secs_f64();
        eprintln!(
            "JSON_SINGLE_PASS cols={cols} rows={rows} bytes={} legacy_ms={:.3} direct_ms={:.3} speedup={:.3}x checksum={direct_checksum}",
            bytes.len(),
            legacy.as_secs_f64() * 1000.0,
            direct.as_secs_f64() * 1000.0,
            ratio,
        );
    }
}
