extern crate prost;
extern crate bytes;
#[macro_use]
extern crate prost_derive;
extern crate zmq;

use std::ops::Deref;

pub mod cmd {
	include!(concat!(env!("OUT_DIR"), "/cmd.rs"));
}

pub mod socket;