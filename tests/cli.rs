use assert_cmd::prelude::*;
use predicates::str::{contains, is_empty};
use std::fs::{self, File};
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// `smolcli` with no args should exit with a non-zero code.
#[test]
fn client_cli_no_args() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("smolcli").unwrap();
    cmd.current_dir(&temp_dir).assert().failure();
}

#[test]
fn client_cli_invalid_get() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["get"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["get", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", "invalid-addr", "get", "key"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--unknown-flag", "get", "key"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_set() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["set"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["set", "missing_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["set", "key", "value", "extra_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", "invalid-addr", "set", "key", "value"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--unknown-flag", "get", "key"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_rm() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["rm"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["rm", "key", "--addr", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["rm", "key", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_subcommand() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["unknown"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// `smolcli -V` should print the version
#[test]
fn client_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("smolcli").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `smoldb -V` should print the version
#[test]
fn server_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("smoldb").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn cli_log_configuration() {
    let temp_dir = TempDir::new().unwrap();
    let stderr_path = temp_dir.path().join("stderr");
    let mut cmd = Command::cargo_bin("smoldb").unwrap();
    let mut child = cmd
        .args(&["--storage", "bitcask", "--addr", "127.0.0.1:4001"])
        .current_dir(&temp_dir)
        .stderr(File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("server exited before killed");

    let content = fs::read_to_string(&stderr_path).expect("unable to read from stderr file");
    assert!(content.contains(env!("CARGO_PKG_VERSION")));
    assert!(content.contains("Bitcask"));
    assert!(content.contains("127.0.0.1:4001"));
}

fn cli_access_server(storage: &str, thread_pool: &str, addr: &str) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("smoldb").unwrap();
    let mut child = server
        .args(&["--storage", storage, "--pool", thread_pool, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout("value1\n");

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "set", "key1", "value2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout("value2\n");

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("Key not found"));

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "rm", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stderr(contains("Key not found"));

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "set", "key2", "value3"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    sender.send(()).unwrap();
    handle.join().unwrap();

    // Wait for server to exit
    thread::sleep(Duration::from_secs(1));

    // Reopen and check value
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("smoldb").unwrap();
    let mut child = server
        .args(&["--storage", storage, "--pool", thread_pool, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("value3"));
    Command::cargo_bin("smolcli")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("Key not found"));
    sender.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn cli_access_server_bitcask_naive() {
    cli_access_server("bitcask", "naive", "127.0.0.1:4002");
}

#[test]
fn cli_access_server_sled_naive() {
    cli_access_server("sled", "naive", "127.0.0.1:4003");
}

#[test]
fn cli_access_server_bitcask_rayon() {
    cli_access_server("bitcask", "rayon", "127.0.0.1:4004");
}

#[test]
fn cli_access_server_sled_rayon() {
    cli_access_server("sled", "rayon", "127.0.0.1:4005");
}

#[test]
fn cli_access_server_bitcask_shared_queue() {
    cli_access_server("bitcask", "shared-queue", "127.0.0.1:4006");
}

#[test]
fn cli_access_server_sled_shared_queue() {
    cli_access_server("sled", "shared-queue", "127.0.0.1:4007");
}
