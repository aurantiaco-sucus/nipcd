use std::fs::{File, Permissions};
use log::{error, info};
use nipcd::Instance;

const LOCAL_NIPC_COMM_SOCKET: &str = "~/.nipcd_comm";

fn main() {
    env_logger::init();
    info!("NanoIPC Daemon by Midnight233");
    if let Err(err) =  File::create(LOCAL_NIPC_COMM_SOCKET) {
        error!("Error creating local communication socket.")
    }
    let instance = Instance::default();
    instance.launch_unix_socket_server(LOCAL_NIPC_COMM_SOCKET).unwrap();
}