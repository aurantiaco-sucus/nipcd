use std::collections::{BTreeMap, HashMap};
use std::io::{BufWriter, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex, RwLock};
use std::{iter, thread};
use crate::NominalPrimitive::Str;

struct Instance {
    clients: HashMap<String, Client>
}

struct Client {
    functions: HashMap<String, Function>,
    outgoing: Arc<Mutex<BufWriter<UnixStream>>>,
}

struct Function {
    parameters: Vec<NominalType>,
    return_type: NominalType,
    implementers: Vec<String>,
}

impl Function {
    pub fn marshal(&self, call_stream: &mut impl Read, impl_stream: &mut BufWriter<UnixStream>) {
        let mut blocks = Vec::new();
        for ty in &self.parameters {
            match ty {
                NominalType::Primitive(prim) => {
                    blocks.push(prim.marshal_from(call_stream));
                }
                NominalType::Array(prim) => {
                    let count = fetch_u32(call_stream);
                    blocks.push(count.to_le_bytes().to_vec());
                    for _ in 0..count {
                        blocks.push(prim.marshal_from(call_stream));
                    }
                }
                NominalType::Map(kp, vp) => {
                    let count = fetch_u32(call_stream);
                    blocks.push(count.to_le_bytes().to_vec());
                    for _ in 0..count {
                        blocks.push(kp.marshal_from(call_stream));
                        blocks.push(vp.marshal_from(call_stream));
                    }
                }
                NominalType::Block(len) => {
                    blocks.push(fetch_blk(call_stream, *len));
                }
                NominalType::Arbitrary => {
                    blocks.push(fetch_var(call_stream));
                }
            }
        }
        for block in blocks {
            send_blk(impl_stream, &block);
        }
    }
}

#[derive(Clone, Copy)]
enum NominalPrimitive {
    I32, U32, F32, // B32
    I64, U64, F64, // B64
    Str, // V,
    Bool, // B32,
}

impl NominalPrimitive {
    fn size(&self) -> u32 {
        match self {
            NominalPrimitive::I32 |
            NominalPrimitive::U32 |
            NominalPrimitive::F32 |
            NominalPrimitive::Bool => 4,
            NominalPrimitive::I64 |
            NominalPrimitive::U64 |
            NominalPrimitive::F64 => 8,
            NominalPrimitive::Str => 0,
        }
    }

    fn marshal_from(&self, stream: &mut impl Read) -> Vec<u8> {
        let size = self.size();
        if size == 0 {
            fetch_var(stream)
        } else {
            fetch_blk(stream, size)
        }
    }
}

impl NominalPrimitive {
    fn try_from_u8(data: u8) -> Option<Self> {
        match data {
            0 => Some(Self::I32),
            1 => Some(Self::U32),
            2 => Some(Self::F32),
            3 => Some(Self::I64),
            4 => Some(Self::U64),
            5 => Some(Self::F64),
            6 => Some(Self::Str),
            7 => Some(Self::Bool),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum NominalType {
    Primitive(NominalPrimitive),
    Array(NominalPrimitive), // B32(Length), V(Inner typed data)*n
    Map(NominalPrimitive, NominalPrimitive), // B32(Length), [V(Key typed data), V(Value typed data)]*n
    Block(u32), // V(Inner data)
    Arbitrary, // B32(Length), B?(Inner data)
}

impl NominalType {
    fn try_from_stream(stream: &mut impl Read) -> Option<Self> {
        let b1 = fetch_u32(stream);
        let b1 = b1.to_le_bytes();
        let nominal = b1[0];
        let prim_first = b1[1];
        let prim_second = b1[2];
        match nominal {
            0 => Some(Self::Primitive(NominalPrimitive::try_from_u8(prim_first)?)),
            1 => Some(Self::Array(NominalPrimitive::try_from_u8(prim_first)?)),
            2 => Some(Self::Map(
                NominalPrimitive::try_from_u8(prim_first)?,
                NominalPrimitive::try_from_u8(prim_second)?,
            )),
            3 => Some(Self::Block(fetch_u32(stream))),
            _ => None
        }
    }

    fn into_block(self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::Primitive(prim) => {
                buf.push(0);
                buf.push(prim as u8);
                buf.extend(iter::repeat(0).take(2));
            }
            Self::Array(prim) => {
                buf.push(1);
                buf.push(prim as u8);
                buf.extend(iter::repeat(0).take(2));
            }
            Self::Map(kp, vp) => {
                buf.push(2);
                buf.push(kp as u8);
                buf.push(vp as u8);
                buf.push(0);
            }
            Self::Block(len) => {
                buf.push(3);
                buf.extend(iter::repeat(0).take(3));
                buf.extend_from_slice(&len.to_le_bytes());
            }
            Self::Arbitrary => {
                buf.push(4);
                buf.extend(iter::repeat(0).take(3));
            }
        }
        buf
    }
}

fn fetch_u32(stream: &mut impl Read) -> u32 {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).unwrap();
    u32::from_le_bytes(buf)
}

fn send_u32(stream: &mut impl Write, val: u32) {
    stream.write_all(&val.to_le_bytes()).unwrap();
}

fn fetch_i32(stream: &mut impl Read) -> i32 {
    unsafe { std::mem::transmute(fetch_u32(stream)) }
}

fn send_i32(stream: &mut impl Write, val: i32) {
    send_u32(stream, unsafe { std::mem::transmute(val) })
}

fn fetch_bool(stream: &mut impl Read) -> bool {
    fetch_u32(stream) != 0
}

fn send_bool(stream: &mut impl Write, val: bool) {
    send_u32(stream, if val { 1 } else { 0 })
}

fn fetch_u64(stream: &mut impl Read) -> u64 {
    let mut buf = [0u8; 8];
    stream.read_exact(&mut buf).unwrap();
    u64::from_le_bytes(buf)
}

fn send_u64(stream: &mut impl Write, val: u64) {
    stream.write_all(&val.to_le_bytes()).unwrap();
}

fn fetch_i64(stream: &mut impl Read) -> i64 {
    unsafe { std::mem::transmute(fetch_u64(stream)) }
}

fn send_i64(stream: &mut impl Write, val: i64) {
    send_u64(stream, unsafe { std::mem::transmute(val) })
}

fn fetch_blk(stream: &mut impl Read, len: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    stream.take(len as u64).read_to_end(&mut buf).unwrap();
    buf
}

fn send_blk(stream: &mut impl Write, buf: &[u8]) {
    send_u32(stream, buf.len() as u32);
    stream.write_all(buf).unwrap();
}

fn fetch_var(stream: &mut impl Read) -> Vec<u8> {
    let len = fetch_u32(stream);
    let mut buf = Vec::new();
    stream.take(len as u64).read_to_end(&mut buf).unwrap();
    buf
}

fn send_var(stream: &mut impl Write, buf: &[u8]) {
    send_u32(stream, buf.len() as u32);
    stream.write_all(buf).unwrap();
}

fn fetch_str(stream: &mut impl Read) -> Option<String> {
    String::from_utf8(fetch_var(stream)).ok()
}

fn send_str(stream: &mut impl Write, val: &str) {
    send_var(stream, val.as_bytes());
}

macro_rules! ordinal_enum {
    (enum $name:ident { $($variant:ident = $value:expr,)*$(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        enum $name {
            $($variant = $value,)*
        }

        impl $name {
            fn from_u32(val: u32) -> Option<Self> {
                match val {
                    $($value => Some(Self::$variant),)*
                    _ => None,
                }
            }

            fn to_u32(self) -> u32 {
                match self {
                    $($name::$variant => $value,)*
                }
            }
        }
    };
}

ordinal_enum!{
    enum IncomingHeader {
        Register = 0,
        CallOwned = 1,
        Call = 2,
        ImplementOwned = 3,
        Implement = 4,
        Peers = 5,
        ListOwned = 6,
        List = 7,
    }
}

fn main() {
    let instance = Arc::new(RwLock::new(Instance {
        clients: HashMap::new(),
    }));
    let server = UnixListener::bind("~/nipcd.socket").unwrap();
    while let Ok((stream, _)) = server.accept() {
        let instance = instance.clone();
        let instance2 = instance.clone();
        let instance_name = Arc::new(Mutex::new(String::new()));
        let instance_name2 = instance_name.clone();
        let jh = thread::spawn(move || {
            let mut stream = stream;
            let client_name = fetch_str(&mut stream).unwrap();
            instance_name.lock().unwrap().push_str(&client_name);
            let mut lock = instance.write().unwrap();
            lock.clients.insert(client_name.clone(), Client {
                functions: HashMap::new(),
                outgoing: Arc::new(Mutex::new(BufWriter::new(stream.try_clone().unwrap()))),
            });
            drop(lock);
            loop {
                match IncomingHeader::from_u32(fetch_u32(&mut stream)).unwrap() {
                    IncomingHeader::Register => {
                        let name = fetch_str(&mut stream).unwrap();
                        let parameters = {
                            let count = fetch_u32(&mut stream);
                            let mut parameters = Vec::new();
                            for _ in 0..count {
                                parameters.push(NominalType::try_from_stream(&mut stream)
                                    .unwrap());
                            }
                            parameters
                        };
                        let return_type = NominalType::try_from_stream(&mut stream)
                            .unwrap();
                        let mut lock = instance.write().unwrap();
                        lock.clients.get_mut(&client_name).unwrap().functions.insert(
                            name,
                            Function {
                                parameters,
                                return_type,
                                implementers: vec![],
                            },
                        );
                        drop(lock);
                    }
                    IncomingHeader::CallOwned => {
                        let name = fetch_str(&mut stream).unwrap();
                        let impl_name = fetch_str(&mut stream).unwrap();
                        let mut lock = instance.read().unwrap();
                        let lock = &mut lock;
                        let mut impl_writer = lock.clients.get(&impl_name).unwrap().outgoing
                            .lock().unwrap();
                        let function = lock.clients.get(&client_name).unwrap().functions
                            .get(&name).unwrap();
                        function.marshal(&mut stream, &mut impl_writer);
                    }
                    IncomingHeader::Call => {
                        let client_name = fetch_str(&mut stream).unwrap();
                        let name = fetch_str(&mut stream).unwrap();
                        let impl_name = fetch_str(&mut stream).unwrap();
                        let mut lock = instance.read().unwrap();
                        let lock = &mut lock;
                        let mut impl_writer = lock.clients.get(&impl_name).unwrap().outgoing
                            .lock().unwrap();
                        let function = lock.clients.get(&client_name).unwrap().functions
                            .get(&name).unwrap();
                        function.marshal(&mut stream, &mut impl_writer);
                    }
                    IncomingHeader::ImplementOwned => {
                        let name = fetch_str(&mut stream).unwrap();
                        let mut lock = instance.write().unwrap();
                        let lock = &mut lock;
                        let function = lock.clients.get_mut(&client_name).unwrap().functions
                            .get_mut(&name).unwrap();
                        function.implementers.push(client_name.clone());
                    }
                    IncomingHeader::Implement => {
                        let target_name = fetch_str(&mut stream).unwrap();
                        let name = fetch_str(&mut stream).unwrap();
                        let mut lock = instance.write().unwrap();
                        let lock = &mut lock;
                        let function = lock.clients.get_mut(&target_name).unwrap().functions
                            .get_mut(&name).unwrap();
                        function.implementers.push(client_name.clone());
                    }
                    IncomingHeader::Peers => {
                        let mut lock = instance.read().unwrap();
                        let lock = &mut lock;
                        let mut outgoing = lock.clients.get(&client_name).unwrap().outgoing
                            .lock().unwrap();
                        send_u32(&mut *outgoing, lock.clients.len() as u32);
                        for name in lock.clients.keys() {
                            send_str(&mut *outgoing, name);
                        }
                    }
                    IncomingHeader::ListOwned => {
                        let mut lock = instance.read().unwrap();
                        let lock = &mut lock;
                        let mut outgoing = lock.clients.get(&client_name).unwrap().outgoing
                            .lock().unwrap();
                        send_u32(&mut *outgoing, lock.clients.len() as u32);
                        for (name, function) in lock.clients.get(&client_name).unwrap().functions
                            .iter()
                        {
                            send_str(&mut *outgoing, name);
                            send_u32(&mut *outgoing, function.parameters.len() as u32);
                            for parameter in &function.parameters {
                                send_blk(&mut *outgoing, &(parameter.into_block()));
                            }
                        }
                    }
                    IncomingHeader::List => {
                        let target_name = fetch_str(&mut stream).unwrap();
                        let mut lock = instance.read().unwrap();
                        let lock = &mut lock;
                        let mut outgoing = lock.clients.get(&client_name).unwrap().outgoing
                            .lock().unwrap();
                        send_u32(&mut *outgoing, lock.clients.get(&target_name).unwrap().functions
                            .len() as u32);
                        for (name, function) in lock.clients.get(&target_name).unwrap().functions
                            .iter()
                        {
                            send_str(&mut *outgoing, name);
                            send_u32(&mut *outgoing, function.parameters.len() as u32);
                            for parameter in &function.parameters {
                                send_blk(&mut *outgoing, &(parameter.into_block()));
                            }
                        }
                    }
                }
            }
        });
        thread::spawn(move || {
            let _ = jh.join();
            instance2.write().unwrap().clients.remove(instance_name2.lock().unwrap().as_str());
        });
    }
}
