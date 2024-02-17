use assert_cmd::prelude::*;
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use smoldb::{Result, Storage};
use std::process::Command;
use tempfile::TempDir;

// `smoldb` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("smoldb").unwrap().assert().failure();
}

// `smoldb -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `smoldb get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `smoldb rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `smoldb set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = Storage::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `smoldb rm <KEY>` should print nothing and exit with zero.
#[test]
fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = Storage::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("smoldb")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}
