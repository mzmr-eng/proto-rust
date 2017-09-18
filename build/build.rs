extern crate prost_build;
extern crate glob;

use std::env;
use std::path::PathBuf;
use std::fs; 

fn main() {

	//find proto files
	let protos : Vec<_>= glob::glob("./proto/*.proto")
		.unwrap()
		.map(|r| r.unwrap())
		.collect();
	

	let includes : Vec<_> = glob::glob("./proto/")
		.unwrap()
		.map(|r| r.unwrap())
		.collect();

	prost_build::compile_protos(&protos[..],
	                            &includes[..],
	                            None).unwrap();
}


