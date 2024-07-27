use std::collections::HashMap;
use std::ptr::NonNull;
use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, NextArg, KeyType, RedisModuleStreamID, RedisValue, NotifyEvent, RedisModule_StreamAdd, raw, REDISMODULE_STREAM_ADD_AUTOID, RedisModuleKey, RedisModuleString, Status};
use redis_module::key::{KeyFlags, KeyMode, RedisKey};
use std::io::Read;
use serde_json::Map;
use std::os::raw::c_int;
use serde_json::json;

static mut MAP: HashMap<String, String> = HashMap::new();

fn redis_string<T: Into<Vec<u8>>>(ctx: &Context, value: T) -> RedisString{
    RedisString::create(NonNull::new(ctx.ctx), value)
}

fn stream_add(ctx: &Context, key_name: &[u8], message: serde_json::Value){
    let mut id = RedisModuleStreamID{ ms: 0, seq: 0 };
    let stream_message_id = &mut id as *mut RedisModuleStreamID;

    let mut message_vector: Vec<RedisString> = vec![];
    for (key, value) in message.as_object().unwrap() {
        message_vector.push(redis_string(ctx, key.as_str()));
        message_vector.push(redis_string(ctx, value.as_str().unwrap())); // what about int, float, etc?
    }
    let mut args = message_vector.iter()
                .map(|v| v.inner)
                .collect::<Vec<_>>();

    let key = raw::open_key(ctx.ctx, redis_string(ctx, key_name).inner, raw::KeyMode::WRITE);
    let status: Status = unsafe {
            RedisModule_StreamAdd.unwrap()(
                key,
                REDISMODULE_STREAM_ADD_AUTOID as c_int,
                stream_message_id,
                (&mut args).as_mut_ptr(),
                (message_vector.len() as i64) / 2
            )
    }.into();
    match status {
        Status::Ok => {}
        Status::Err => {panic!("err")}
    }
    raw::close_key(key);
}

fn stream_read_from(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let stream_key = args.next_arg()?;

    let stream = ctx.open_key(&stream_key);
    let key_type = stream.key_type();

    if key_type != KeyType::Stream {
        return Err(RedisError::WrongType);
    }

    let mut iter = stream.get_stream_iterator(false)?;
    let element = iter.next();
    let id_to_keep = iter.next().as_ref().map_or_else(
        || RedisModuleStreamID {
            ms: u64::MAX,
            seq: u64::MAX,
        },
        |e| e.id,
    );

    let stream = ctx.open_key_writable(&stream_key);
    stream.trim_stream_by_id(id_to_keep, false)?;
    Ok(match element {
        Some(e) => RedisValue::BulkString(format!("{}-{}", e.id.ms, e.id.seq)),
        None => RedisValue::Null,
    })
}

fn hello_mul(_: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let nums = args
        .into_iter()
        .skip(1)
        .map(|s| s.parse_integer())
        .collect::<Result<Vec<i64>, RedisError>>()?;

    let product = nums.iter().product();

    let mut response = nums;
    response.push(product);

    Ok(response.into())
}


fn on_stream(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    println!("{:?} {} {:?}", event_type, event, key);
    let stream = ctx.open_key(&RedisString::create(NonNull::new(ctx.ctx), key));

    stream_add(ctx, "out_test".as_bytes(), json!({"a": "b", "c": "d"}));
    for record in stream.get_stream_range_iterator(None, None, false, false).unwrap() {
        let mut message = serde_json::Map::new();
        for field in record.fields {
            println!("FIELD {:?} {:?}", field.0.to_string(), field.1.to_string());
            let value: serde_json::Value = serde_json::from_str(field.1.to_string().as_str()).unwrap();
            message.insert(field.0.to_string(), value);

        }
        println!("{:?}", message)
    }
}

//////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use serde_json::Value;
    use super::*;

    #[test]
    fn it_works() {
        let val: Value = serde_json::from_str(String::from_str("\"x\"").unwrap().as_str()).unwrap();

    }
}

#[cfg(not(test))]
redis_module! {
    name: "hello",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    rdb_save: [],
    commands: [
        ["hello.mul", hello_mul, "", 0, 0, 0],
    ],
    event_handlers: [
        [@STREAM: on_stream],
    ]
}