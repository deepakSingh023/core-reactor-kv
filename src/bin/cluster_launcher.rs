// src/bin/cluster_launcher.rs

use std::process::Command;
use std::thread;
use std::time::Duration;

fn main() {
    println!(" [Launcher] Compiling your main Key-Value Reactor in Release Mode...");
    
    // 1. Force a clean Cargo release build first so the binary is fully ready
    let build_status = Command::new("cargo")
        .args(&["build", "--release"])
        .status()
        .expect("Failed to execute cargo build command");

    if !build_status.success() {
        eprintln!("❌ [Launcher] Compilation failed. Aborting cluster deployment.");
        std::process::exit(1);
    }

    println!(" [Launcher] Launching 4 Isolated Cluster Nodes concurrently...");

    // 2. Loop through Node IDs 0 to 3 to spawn them in separate hardware streams
    for node_id in 0..4 {
        let node_str = node_id.to_string();
        
        // Detect the operating system to open an actual visual terminal window
        if cfg!(target_os = "macos") {
            // macOS Track: Triggers AppleScript to pop a native Terminal window
            Command::new("osascript")
                .args(&[
                    "-e",
                    &format!(
                        "tell application \"Terminal\" to do script \"cd '{}' && target/release/core-reactor-kv {}\"",
                        std::env::current_dir().unwrap().display(),
                        node_str
                    ),
                ])
                .spawn()
                .expect("Failed to spawn macOS terminal window");
        }  else {
            // Linux Track: Wrap the binary call in bash execution with a fallback shell
            // This forces gnome-terminal to stay alive so you can inspect any boot panic logs!
            Command::new("gnome-terminal")
                .args(&[
                    "--",
                    "bash",
                    "-c",
                    &format!("target/release/core-reactor-kv {}; echo '⚠️ Process exited! Press Enter to close window...'; read", node_str)
                ])
                .spawn()
                .expect("Failed to spawn Linux gnome-terminal window");
        }


        // A small 100ms microsecond buffer so the OS can allocate port files sequentially
        thread::sleep(Duration::from_millis(300));
    }

    println!(" [Launcher] All 4 terminal sessions deployed! You can close this window.");
}
