//! End-to-end regression tests for the filterx CLI.
//!
//! Each test spawns the built binary so the CLI, parser and engine layers are
//! all exercised, matching how the tool is used in practice.

use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn write_temp_file(name: &str, content: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("filterx_it_{}_{}", std::process::id(), name));
    let mut file = File::create(&path).expect("create temp file");
    file.write_all(content.as_bytes()).expect("write temp file");
    path
}

fn run_filterx(args: &[&str]) -> (String, String, Option<i32>) {
    let output = Command::new(env!("CARGO_BIN_EXE_filterx"))
        .args(args)
        .output()
        .expect("failed to spawn filterx");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.code(),
    )
}

/// Same as `run_filterx` but fails (returns Err) when the child runs longer
/// than `timeout`, killing it first. Used to catch regressions that hang.
fn run_filterx_with_timeout(
    args: &[&str],
    stdout_path: &PathBuf,
    timeout: Duration,
) -> Result<(String, Option<i32>), ()> {
    let stdout = File::create(stdout_path).expect("create stdout file");
    let mut child = Command::new(env!("CARGO_BIN_EXE_filterx"))
        .args(args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn filterx");
    let started = Instant::now();
    loop {
        match child.try_wait().expect("wait on filterx") {
            Some(status) => {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                return Ok((stderr, status.code()));
            }
            None => {
                if started.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(());
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

const FASTQ_THREE_RECORDS: &str =
    "@r1 comm1\nACGT\n+\nIIII\n@r2 comm2\nTTTT\n+\nJJJJ\n@r3 comm3\nGGGG\n+\nIIII\n";

#[test]
fn fastq_without_expression_outputs_all_records() {
    // regression: the default --limit 0 used to break the fast path entirely
    let path = write_temp_file("fq_fast.fq", FASTQ_THREE_RECORDS);
    let (stdout, _, code) = run_filterx(&["fq", path.to_str().unwrap()]);
    assert_eq!(code, Some(0), "filterx fq failed");
    assert_eq!(stdout, FASTQ_THREE_RECORDS);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_with_expression_outputs_all_records() {
    // regression: the default --limit 0 used to set limit_rows to 0 and the
    // VM path produced no output at all
    let path = write_temp_file("fq_vm.fq", FASTQ_THREE_RECORDS);
    let (stdout, stderr, code) = run_filterx(&[
        "fq",
        path.to_str().unwrap(),
        "-e",
        "len(seq) > 0",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, FASTQ_THREE_RECORDS);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_qual_line_starting_with_at_sign_roundtrips() {
    // regression: a quality line starting with '@' (phred 31) was treated as
    // the next record's header, corrupting both records
    let input = "@r1 comm1\nACGT\n+\n@555\n@r2 comm2\nTTTT\n+\n5555\n";
    let path = write_temp_file("fq_at_qual.fq", input);
    let (stdout, stderr, code) = run_filterx(&["fq", path.to_str().unwrap()]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, input, "record with '@'-starting qual was corrupted");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_plus_line_with_id_roundtrips() {
    // regression: an optional id on the '+' line leaked into the sequence
    let input = "@r1 comm1\nACGT\n+r1\nIIII\n@r2 comm2\nTTTT\n+\nJJJJ\n";
    let path = write_temp_file("fq_plus_id.fq", input);
    let (stdout, stderr, code) = run_filterx(&["fq", path.to_str().unwrap()]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, input, "'+' line id was not preserved");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_no_qual_with_expression_matches_fast_path() {
    // regression: --no-qual plus any expression failed with "Lost 'qual'".
    // Both paths must agree and emit FASTA, like the no-expression fast path.
    let input = "@r1 comm1\nACGT\n+\nIIII\n@r2 comm2\nTTTT\n+\nJJJJ\n";
    let expected = ">r1 comm1\nACGT\n>r2 comm2\nTTTT\n";
    let path = write_temp_file("fq_no_qual.fq", input);

    let (fast, _, code) = run_filterx(&["fq", path.to_str().unwrap(), "--no-qual"]);
    assert_eq!(code, Some(0));
    assert_eq!(fast, expected);

    let (vm, stderr, code) = run_filterx(&[
        "fq",
        path.to_str().unwrap(),
        "--no-qual",
        "-e",
        "len(seq) > 0",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(vm, expected);
    assert!(!stderr.contains("Lost"), "stderr: {stderr}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn boolop_keeps_all_and_conditions() {
    // regression: `a > 0 and a > 1 and a > 2` silently dropped the third one
    let path = write_temp_file("and3.csv", "a\n1\n2\n3\n4\n");
    let (stdout, stderr, code) = run_filterx(&[
        "c",
        path.to_str().unwrap(),
        "-H",
        "-e",
        "a > 0 and a > 1 and a > 2",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, "a\n3\n4\n");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn boolop_keeps_all_or_conditions() {
    // regression: only the first two `or` values were evaluated
    let path = write_temp_file("or3.csv", "a\n1\n2\n3\n4\n");
    let (stdout, stderr, code) = run_filterx(&[
        "c",
        path.to_str().unwrap(),
        "-H",
        "-e",
        "a == 4 or a == 1 or a == 2",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, "a\n1\n2\n4\n");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unknown_column_name_is_a_friendly_error_not_a_panic() {
    // regression: `select('a(')` compiled the name as a regex and panicked
    let path = write_temp_file("cols.csv", "name,seq\nn1,AC\n");
    let (_, stderr, code) = run_filterx(&["c", path.to_str().unwrap(), "-H", "-e", "select('a(')"]);
    assert_eq!(code, Some(1));
    assert!(!stderr.contains("panicked"), "should not panic: {stderr}");
    assert!(stderr.contains("not found"), "stderr: {stderr}");
    assert!(stderr.contains("Valid columns"), "stderr: {stderr}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn typo_column_name_is_rejected_instead_of_matched() {
    // regression: `na` regex-matched the column `name`, deferring the error
    // to a confusing polars failure deep inside the plan
    let path = write_temp_file("typo.csv", "name,seq\nn1,AC\n");
    let (_, stderr, code) = run_filterx(&["c", path.to_str().unwrap(), "-H", "-e", "na == 'n1'"]);
    assert_eq!(code, Some(1));
    assert!(!stderr.contains("panicked"), "should not panic: {stderr}");
    assert!(stderr.contains("Column") && stderr.contains("not found"), "stderr: {stderr}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fasta_with_very_long_first_line_does_not_hang() {
    // regression: detect_breakline_len looped forever on fill_buf when the
    // first line exceeded the reader buffer (8 KiB)
    let mut content = String::from(">r1 ");
    content.push_str(&"C".repeat(64 * 1024));
    content.push_str("\nACGT\n");
    let path = write_temp_file("long_line.fa", &content);
    let mut stdout_path = std::env::temp_dir();
    stdout_path.push(format!("filterx_it_{}_long_line.out", std::process::id()));

    let result = run_filterx_with_timeout(
        &["fa", path.to_str().unwrap()],
        &stdout_path,
        Duration::from_secs(60),
    );
    assert!(result.is_ok(), "filterx fa hangs on a long first line");
    let (stderr, code) = result.unwrap();
    assert_eq!(code, Some(0), "stderr: {stderr}");
    let stdout = std::fs::read_to_string(&stdout_path).unwrap();
    assert!(stdout.starts_with(">r1 C"));
    assert!(stdout.ends_with("\nACGT\n"));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&stdout_path);
}

#[test]
fn fasta_default_limit_outputs_all_records() {
    // guard: the fasta path already normalized --limit 0, keep it that way
    let path = write_temp_file("two.fa", ">r1\nACGT\n>r2\nTTTT\n");
    let (stdout, _, code) = run_filterx(&["fa", path.to_str().unwrap()]);
    assert_eq!(code, Some(0));
    assert_eq!(stdout, ">r1\nACGT\n>r2\nTTTT\n");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn csv_basic_filter_still_works() {
    // guard for the shared csv path after the has_column change
    let path = write_temp_file("basic.csv", "name,seq\nn1,AC\nn2,GT\n");
    let (stdout, stderr, code) = run_filterx(&[
        "c",
        path.to_str().unwrap(),
        "-H",
        "-e",
        "seq == 'AC'",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, "name,seq\nn1,AC\n");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_phred_auto_prefers_phred33_for_common_illumina_quals() {
    // regression: 'I' (ASCII 73 = Q40) is valid in both encodings, but the old
    // heuristic classified a run of 'I' as phred64, yielding wrong qual() values
    let path = write_temp_file("phred33.fq", "@r1\nACGT\n+\nIIII\n");
    let (_, stderr, _) = run_filterx(&["fq", path.to_str().unwrap(), "-e", "phred()"]);
    assert!(stderr.contains("phred33"), "stderr: {stderr}");

    let (stdout, stderr, code) = run_filterx(&[
        "fq",
        path.to_str().unwrap(),
        "-e",
        "print('{qual(qual)}')",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    let q: f32 = stdout.trim().parse().expect("qual() should print a number");
    assert!(
        (q - 40.0).abs() < 0.01,
        "expected Q40 for 'IIII' under phred33, got {q}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_phred_auto_detects_phred64_for_illumina_13_quals() {
    // 'h' (ASCII 104 = Q41 phred64) cannot be phred33 (max is 'J' = 74)
    let path = write_temp_file("phred64.fq", "@r1\nACGT\n+\nhhhh\n");
    let (_, stderr, _) = run_filterx(&["fq", path.to_str().unwrap(), "-e", "phred()"]);
    assert!(stderr.contains("phred64"), "stderr: {stderr}");

    let (stdout, stderr, code) = run_filterx(&[
        "fq",
        path.to_str().unwrap(),
        "-e",
        "print('{qual(qual)}')",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    let q: f32 = stdout.trim().parse().expect("qual() should print a number");
    assert!(
        (q - 40.0).abs() < 0.01,
        "expected Q40 for 'hhhh' under phred64, got {q}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_phred_detect_rejects_inconsistent_types() {
    // '5' (53) can only be phred33, 'h' (104) can only be phred64
    let path = write_temp_file(
        "mixed.fq",
        "@r1\nACGT\n+\n5555\n@r2\nACGT\n+\nhhhh\n",
    );
    let (_, stderr, code) = run_filterx(&["fq", path.to_str().unwrap(), "--limit", "10"]);
    assert_eq!(code, Some(1));
    assert!(stderr.contains("not consistent"), "stderr: {stderr}");
    assert!(stderr.contains("--phred"), "stderr: {stderr}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_without_trailing_newline_keeps_last_quality_char() {
    // regression: the line break was stripped unconditionally, eating the
    // final quality character when the file does not end with '\n'
    let input = "@r1 comm1\nACGT\n+\nIIII";
    let path = write_temp_file("nonl.fq", input);
    let (stdout, stderr, code) = run_filterx(&["fq", path.to_str().unwrap()]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, format!("{input}\n"), "last qual char was eaten");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_truncated_qual_is_an_error() {
    // regression: a record whose quality is shorter than its sequence was
    // silently accepted at EOF
    let path = write_temp_file("truncated.fq", "@r1\nACGT\n+\nII");
    let (_, stderr, code) = run_filterx(&["fq", path.to_str().unwrap()]);
    assert_eq!(code, Some(1));
    assert!(
        stderr.contains("shorter than sequence"),
        "stderr: {stderr}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fastq_forced_phred64_does_not_panic_on_low_quality_chars() {
    // regression: the phred64 kernel subtracted 64 from chars below 64 and
    // indexed the map with the wrapped value
    let path = write_temp_file("low.fq", "@r1\nACGT\n+\n5555\n");
    let (stdout, stderr, code) = run_filterx(&[
        "fq",
        path.to_str().unwrap(),
        "--phred",
        "phred64",
        "-e",
        "print('{qual(qual)}')",
    ]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(!stderr.contains("panicked"), "stderr: {stderr}");
    let q: f32 = stdout.trim().parse().expect("qual() should print a number");
    assert_eq!(q, 0.0, "undecodable phred64 characters must yield 0.0");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fasta_without_trailing_newline_keeps_last_sequence_char() {
    // regression: same unconditional break-line stripping as fastq, in the
    // fasta sequence loop
    let path = write_temp_file("nonl.fa", ">r1\nACGT");
    let (stdout, stderr, code) = run_filterx(&["fa", path.to_str().unwrap()]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert_eq!(stdout, ">r1\nACGT\n", "last sequence char was eaten");
    let _ = std::fs::remove_file(&path);
}
