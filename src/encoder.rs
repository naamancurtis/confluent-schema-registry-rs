use serde::Serialize;

pub trait Encoder<T>
where
    T: Sized + Serialize,
{
    fn encode(&self, data: T) -> Vec<u8>;
}
