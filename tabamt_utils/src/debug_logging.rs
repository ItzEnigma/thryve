// A simple logging utility to log messages to a file for debugging purposes.
// Probably will not be compiled in production builds.
// This is a very basic implementation.

use std::fs::OpenOptions;
use std::io::Write;

// Log a message to the log file.
// @param message - The message to log.
pub fn log(message: String){
    // Open the log file in append mode, create it if it doesn't exist.
    // Note: The file path is hardcoded for simplicity.
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)   // create if doesn’t exist
        .open("E:\\My Projects\\Rust\\TaBaMT TestLib\\log\\output.log")
        .expect("Failed To Open File");

    // Write the message to the file.
    writeln!(file, "{}", message).expect("Failed To Write To Line");
}