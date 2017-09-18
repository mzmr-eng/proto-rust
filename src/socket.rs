use std;
use zmq;
use prost;


pub type Result<T> = std::result::Result<T,Error>;

#[derive(Debug)]
pub enum Error {
	ZMQ(zmq::Error),
	IO(std::io::Error),
}

impl std::convert::From<zmq::Error> for Error {
	fn from(err:zmq::Error) -> Self {
		Error::ZMQ(err)
	}
}

impl std::convert::From<std::io::Error> for Error {
	fn from(err:std::io::Error) -> Self {
		Error::IO(err)
	}
}

pub trait Socket {
	fn bind(&mut self, uri : &str) -> Result<()>;
	fn connect(&mut self, uri : &str) -> Result<()>;
}

pub trait SendSock {
	type Message;
	fn send(&mut self, msg : &Self::Message) -> Result<()>;
}

pub trait RecvSock  {
	fn can_recv(&mut self) -> Result<bool>;
	fn recv<'a,'b>(&'a mut self, buffer : &'b mut [u8]) -> Result<<(&'a mut Self, &'b mut [u8]) as RecvData>::Message> where (&'a mut Self, &'b mut [u8]):RecvData {
		RecvData::recv((self,buffer))
	}
}

pub trait RecvData {
	type Message;
	fn recv(self) -> Result<Self::Message>;
}

pub struct Context {
	ctx : zmq::Context,
}

impl Context {
	pub fn new() -> Self {
    	let ctx = zmq::Context::new();
    	Context { ctx : ctx }
	}

	pub fn xpub<M,R>(&self) -> Result<XPub<M,R>> {
		let sock = self.ctx.socket(zmq::XPUB)?;
		let mut send_buffer = Vec::new();
		Ok(XPub { socket : sock , m : std::marker::PhantomData , send_buffer : send_buffer})
	}
	pub fn xsub<M,R>(&self) -> Result<XSub<M,R>> {
		let sock = self.ctx.socket(zmq::XSUB)?;
		let mut buffer = Vec::with_capacity(2048);
		Ok(XSub { socket : sock , m : std::marker::PhantomData , topic_buf : buffer })
	}
}

pub struct Topic<'a, 'b,M> {
	socket : &'a mut zmq::Socket, 
	pub topic : &'b [u8],
	m : std::marker::PhantomData<M>,
	send_buffer : &'a mut Vec<u8>,
}

pub struct XPub<M,R> {
	socket : zmq::Socket,
	m : std::marker::PhantomData<(M,R)>,
	send_buffer : Vec<u8>,
}

pub struct XSub<M,R> {
	socket : zmq::Socket,
	m : std::marker::PhantomData<(M,R)>,
	topic_buf : Vec<u8>,
}

impl<M,R> XPub<M,R> {
	pub fn topic<'a, 'b>(&'a mut self, topic:&'b [u8]) -> Topic<'a,'b,M> {
		Topic {
			socket : &mut self.socket,
			topic : topic,
			m : std::marker::PhantomData,
			send_buffer : &mut self.send_buffer,
		}
	}
}

impl<M,R> XSub<M,R> {
	pub fn subscribe(&mut self, topic : &[u8]) -> Result<()> {
		self.topic_buf.clear();
		self.topic_buf.push(1);
		self.topic_buf.extend_from_slice(topic);
    	self.socket.send(&self.topic_buf[..],0)?;
    	Ok(())
	}
	pub fn unsubscribe(&mut self, topic : &[u8]) -> Result<()> {
		self.topic_buf.clear();
		self.topic_buf.push(0);
		self.topic_buf.extend_from_slice(topic);
    	self.socket.send(&self.topic_buf[..],0)?;
    	Ok(())
	}
}

impl<M,R> Socket for XPub<M,R> {
	fn bind(&mut self, uri : &str) -> Result<()> {
		self.socket.bind(uri)?;
		Ok(())
	}
	fn connect(&mut self, uri : &str) -> Result<()> {
		self.socket.connect(uri)?;
		Ok(())
	}
}

impl<M,R> Socket for XSub<M,R> {
	fn bind(&mut self, uri : &str) -> Result<()> {
		self.socket.bind(uri)?;
		Ok(())
	}
	fn connect(&mut self, uri : &str) -> Result<()> {
		self.socket.connect(uri)?;
		Ok(())
	}
}

pub enum SubscriptionInfo<'a,'b,M,R> {
	Subscribe(Topic<'a,'b, M>),
	Unsubscribe(&'b [u8]),
	Message(R),
}

pub trait Subscription<'a,'b,S,R> : {
	fn info(self) -> SubscriptionInfo<'a,'b,S,R>;
	fn from_info(SubscriptionInfo<'a,'b,S,R>) -> Self;
}

impl<'a,'b,M,R> Subscription<'a,'b,M,R> for SubscriptionInfo<'a,'b,M,R> {
	fn info(self) -> Self {
		self
	}
	fn from_info(info : Self) -> Self {
		info
	}
}

impl<'a, 'b, M:prost::Message> SendSock for Topic<'a,'b,  M> {
	type Message=M;
	fn send(&mut self, msg:&Self::Message) -> Result<()> {
        self.socket.send(self.topic, zmq::SNDMORE)?;

        let msg = {
        	self.send_buffer.clear();
    		msg.encode_length_delimited(self.send_buffer)?;
    		&self.send_buffer[..]
    	};
    	self.socket.send(msg,0)?;
    	Ok(())
	}
}

impl<'a, 'b, M:prost::Message, R:prost::Message> RecvData for (&'a mut XPub<M,R>, &'b mut [u8]) {
	type Message = SubscriptionInfo<'a,'b,M,R>;

	fn recv(self) -> Result<Self::Message> {
		let (xpub,buffer) = self;
		let len = xpub.socket.recv_into(buffer, 0)?;

		let s = match buffer[0] {
			0 => {
				SubscriptionInfo::Unsubscribe(&buffer[1..len])
			},
			1 => {
				SubscriptionInfo::Subscribe(Topic {socket : &mut xpub.socket, topic : &buffer[1..len], m : std::marker::PhantomData, send_buffer : &mut xpub.send_buffer })
			},
			_ => {
    			let mut cursor = std::io::Cursor::new(&buffer[1..len]);
    			let cmd = R::decode_length_delimited(&mut cursor)?;
    			SubscriptionInfo::Message(cmd)
    		}
    	};

    	Ok(s)
    }
}

impl<M:prost::Message, R:prost::Message> RecvSock for XPub<M,R>
{
	fn can_recv(&mut self) -> Result<bool> {
		let mut items = [
            self.socket.as_poll_item(zmq::POLLIN),
        ];
        zmq::poll(&mut items, 0)?;

        Ok(items[0].is_readable())
    }
}

impl<M:prost::Message, R:prost::Message> SendSock for XSub<M,R> {
	type Message=R;

	fn send(&mut self, msg:&Self::Message) -> Result<()> {

        let msg = {

        	self.topic_buf.clear();
        	self.topic_buf.push(2);
    		msg.encode_length_delimited(&mut self.topic_buf)?;
    		&self.topic_buf[..]
    	};
    	self.socket.send(msg,0)?;
    	Ok(())
	}

}

impl<'a, 'b, M:prost::Message, R:prost::Message> RecvData for (&'a mut XSub<M,R>, &'b mut [u8]) {
	type Message = (&'b [u8], M);

	fn recv(self) -> Result<Self::Message> {
		let (sock,buf) = self;
		let len = sock.socket.recv_into(buf, 0)?;
		if len > buf.len() {
			println!("len={}, buf={}", len, buf.len());
			panic!();
		}
		let (topic,buf) = buf.split_at_mut(len);

		let len = sock.socket.recv_into(buf, 0)?;
    	let mut cursor = std::io::Cursor::new(&buf[0..len]);
    	let cmd = M::decode_length_delimited(&mut cursor)?;

    	Ok((buf,cmd))
    }
}

impl<M:prost::Message, R:prost::Message> RecvSock for XSub<M,R> {
	
	fn can_recv(&mut self) -> Result<bool> {
		let mut items = [
            self.socket.as_poll_item(zmq::POLLIN),
        ];
        zmq::poll(&mut items, 0)?;

        Ok(items[0].is_readable())
    }
}