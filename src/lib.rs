extern crate core;

use std::collections::{HashMap, VecDeque};
use std::io::{BufReader, BufWriter, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use libnipc::{NominalPrimitive, NominalType, ReadExt, WriteExt};
use log::{error, info};

#[derive(Default)]
pub struct Instance {
    domains: RwLock<HashMap<String, Domain>>,
    streams: Arc<RwLock<HashMap<String, UnixStream>>>
}

impl Instance {
    pub fn launch_unix_socket_server<S: AsRef<Path>>(&self, addr: S) -> Option<()> {
        let listener = UnixListener::bind(addr).ok()?;
        while let Ok((mut stream, addr)) = listener.accept() {
            let incoming_addr = addr.as_pathname()
                .map(|x| x.to_string_lossy().to_string())
                .unwrap_or("<Unknown Path>".to_string());
            info!("Incoming connection: {incoming_addr}");
            let domain_name = if let Some(val) = stream.read_arb() { val } else {
                error!("Error reading domain name from {incoming_addr}.");
                continue;
            };
            let domain_name = if let Ok(val) = String::from_utf8(domain_name)
            { val } else {
                error!("Error interpreting domain name from {incoming_addr}.");
                continue;
            };
            self.domains.write().unwrap()
                .insert(domain_name.clone(), Domain::default());
            self.streams.write().unwrap()
                .insert(domain_name.clone(), stream.try_clone().unwrap());
            let outgoing_dict = self.streams.clone();
            let jh: JoinHandle<Option<()>> = thread::spawn(move || {
                let mut incoming = stream;
                let outgoing_dict = outgoing_dict;
                loop {
                    let cmd = incoming.read_u32()?;
                }
            });
            thread::spawn(move || {
                if let Err(err) = jh.join() {
                    error!("Client exited unexpectedly.")
                } else {
                    info!("Client exited gracefully.")
                }
            });
        }
        Some(())
    }
}

#[derive(Default)]
pub struct Domain {
    functions: RwLock<HashMap<String, FuncDef>>,
    func_impls: RwLock<HashMap<String, Vec<String>>>,
}

type BlockMarshaller = Box<dyn Fn(
    &mut BufReader<UnixStream>,
    &mut BufWriter<UnixStream>
) -> Option<()>>;

fn bm_b32() -> BlockMarshaller {
    Box::new(|incoming, outgoing| {
        let b32: [u8; 4] = incoming.read_arr()?;
        outgoing.write_arr(b32)?;
        Some(())
    })
}

fn bm_b64() -> BlockMarshaller {
    Box::new(|incoming, outgoing| {
        let b32: [u8; 8] = incoming.read_arr()?;
        outgoing.write_arr(b32)?;
        Some(())
    })
}

fn bm_bf(fixed_len: u32) -> BlockMarshaller {
    Box::new(move |incoming, outgoing| {
        let bf = incoming.read_vec(fixed_len as usize)?;
        outgoing.write_slice(&bf);
        Some(())
    })
}

fn bm_arb() -> BlockMarshaller {
    Box::new(move |incoming, outgoing| {
        let len = incoming.read_u32()?;
        let dt = incoming.read_vec(len as usize)?;
        outgoing.write_u32(len)?;
        outgoing.write_slice(&dt)?;
        Some(())
    })
}

fn bm_arb_checked(pred: fn(&[u8]) -> bool) -> BlockMarshaller {
    Box::new(move |incoming, outgoing| {
        let len = incoming.read_u32()?;
        let dt = incoming.read_vec(len as usize)?;
        if !pred(&dt) {
            None
        } else {
            outgoing.write_u32(len)?;
            outgoing.write_slice(&dt)?;
            Some(())
        }
    })
}

fn bm_dual(a: BlockMarshaller, b: BlockMarshaller) -> BlockMarshaller {
    Box::new(move |incoming, outgoing| {
        a(incoming, outgoing)?;
        b(incoming, outgoing)
    })
}

fn bm_rep(marshaller: BlockMarshaller) -> BlockMarshaller {
    Box::new(move |incoming, outgoing| {
        let len = incoming.read_u32()?;
        outgoing.write_u32(len);
        for _ in 0..len {
            marshaller(incoming, outgoing);
        }
        Some(())
    })
}

fn bm_prim(prim: NominalPrimitive) -> BlockMarshaller {
    match prim {
        NominalPrimitive::U32 |
        NominalPrimitive::I32 |
        NominalPrimitive::F32 => bm_b32(),
        NominalPrimitive::U64 |
        NominalPrimitive::I64 |
        NominalPrimitive::F64 => bm_b64(),
        NominalPrimitive::String => bm_arb(),
    }
}

fn bm_nominal(nominal: NominalType) -> BlockMarshaller {
    match nominal {
        NominalType::Primitive(prim) => bm_prim(prim),
        NominalType::Array(prim) => bm_rep(bm_prim(prim)),
        NominalType::Map(prim1, prim2) =>
            bm_rep(bm_dual(bm_prim(prim1), bm_prim(prim2))),
        NominalType::Block(len) => bm_bf(len),
        NominalType::Arbitrary => bm_arb(),
    }
}

struct FuncDef {
    params: Vec<NominalType>,
    param_names: Vec<String>,
    ret_type: NominalType,
    marshaller: Vec<BlockMarshaller>
}

impl FuncDef {
    pub fn new(params: Vec<NominalType>, param_names: Vec<String>, ret_type: NominalType) -> Self {
        let marshaller = params.iter()
            .map(|x| bm_nominal(*x))
            .collect::<Vec<_>>();
        Self {
            params,
            param_names,
            ret_type,
            marshaller,
        }
    }
}