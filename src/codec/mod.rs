pub use bytes::{Buf, BufMut};

pub trait Encode {
    fn write_to<W: BufMut>(&self, writer: &mut W);
}

pub trait Decode: Sized {
    fn read_from<R: Buf>(reader: &mut R) -> std::io::Result<Self>;
}
