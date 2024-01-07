use std::ptr::NonNull;
use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, NextArg, KeyType, RedisModuleStreamID, RedisValue, NotifyEvent, RedisModule_StreamAdd, raw, REDISMODULE_STREAM_ADD_AUTOID, RedisModuleKey, RedisModuleString};
use redis_module::key::{KeyFlags, KeyMode, RedisKey};
use std::io::Read;

fn redis_string<T: Into<Vec<u8>>>(ctx: &Context, value: T) -> RedisString{
    RedisString::create(NonNull::new(ctx.ctx), value)
}

fn stream_add(ctx: &Context, key_name: &[u8]){
    let mut id = RedisModuleStreamID{ ms: 0, seq: 0 };
    let raw = &mut id as *mut RedisModuleStreamID;
    let vals = [redis_string(ctx, "A"), redis_string(ctx, "B")];
    let mut args = vals.iter()
                .map(|v| v.inner)
                .collect::<Vec<_>>();

    let key = raw::open_key(ctx.ctx, redis_string(ctx, "test").inner, raw::KeyMode::WRITE);
    let res = unsafe {
            raw::RedisModule_StreamAdd.unwrap()(
                key,
                1,
                raw,
                (&mut args).as_mut_ptr(),
                1
            )
        };
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
    let stream = ctx.open_key(&RedisString::create(NonNull::new(ctx.ctx), key));
    stream_add(ctx, key);
    stream_add(ctx, key);
    let iter = stream.get_stream_iterator(false);
    let key_type = stream.key_type();
}

//////////////////////////////////////////////////////

redis_module! {
    name: "hello",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        ["hello.mul", hello_mul, "", 0, 0, 0],
    ],
    // event_handlers: [
    //     [@STREAM: on_stream],
    // ]
}