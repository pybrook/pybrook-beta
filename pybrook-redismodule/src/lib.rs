use redis_module::key::{HMGetResult};
use redis_module::logging::log_warning;
use redis_module::{raw, redis_module, Context, RedisModuleStreamID, NotifyEvent, RedisModule_StreamAdd, RedisResult, RedisString, RedisValue, Status, REDISMODULE_STREAM_ADD_AUTOID, RedisError};
use serde::{Deserialize, Serialize};
use serde_json::{json};
use serde_json::{Map, Value};
use std::collections::{HashMap};
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::str;
use std::sync::{LazyLock, RwLock};
use redis_module_macros::command;

const MSG_ID_FIELD: &str = "@pb@msg_id";


#[derive(Debug, Clone, Serialize, Deserialize)]
struct DependencyField {
  src: String,
  dst: String,
}

impl DependencyField {
  fn new(name: &str) -> Self {
    Self {
      src: String::from(name),
      dst: String::from(name),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Dependency {
  stream_key: String, // could be some kind of wildcard in the future,
  // to support partitioning
  // tagging would still work the same way
  fields: Vec<DependencyField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DependencyResolver {
  inputs: Vec<Dependency>,
  output_stream_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputTagger {
  stream_key: String,
  obj_id_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrookConfig {
  dependency_resolvers: HashMap<String, DependencyResolver>,
  input_taggers: HashMap<String, InputTagger>,
}

#[derive(Debug)]
enum DependencyResolverError {
  MissingIdField,
  HMGETError,
  KeyDropError,
}

impl DependencyResolver {
  fn process_message(
    &self,
    ctx: &Context,
    dependency: &Dependency,
    message: &Map<String, Value>,
  ) -> Result<(), DependencyResolverError> {
    // println!("{:?}", message);
    let message_id = message
      .get(MSG_ID_FIELD)
      .ok_or(DependencyResolverError::MissingIdField)?;
    let total_dependencies: i64 = self.inputs.len() as i64;
    let mut message_cloned = message.clone();
    let mut dependency_map = serde_json::Map::from_iter(
      dependency
        .fields
        .clone()
        .into_iter()
        .map(|f| {
          (
            String::from(f.dst),
            message_cloned.remove(&f.src).unwrap_or(Value::Null),
          )
        })
        .collect::<HashMap<String, Value>>(),
    );

    let deps_map_key = redis_string(
      ctx,
      format!("__{}:{}@pb@dmap__", self.output_stream_key, message_id),
    );
    let dcount_key = redis_string(
      ctx,
      format!("__{}:{}@pb@dcount__", self.output_stream_key, message_id),
    );
    if incr(ctx, &dcount_key).eq(&total_dependencies) {
      let key = ctx.open_key(&deps_map_key);
      let fields: Vec<RedisString> = self
        .inputs
        .iter()
        .map(|d| d.fields.iter().map(|f| redis_string(ctx, f.dst.clone())))
        .flatten()
        .collect();
      let values: Option<HMGetResult<_, String>> = key
        .hash_get_multi(&fields)
        .map_err(|_| DependencyResolverError::HMGETError)?;
      let mut dependency_redis_hmap: Map<String, Value> = match values {
        None => Map::new(),
        Some(values) => Map::from_iter(
          values
            .into_iter()
            .map(|(k, v)| {
              (
                k.to_string(),
                serde_json::from_str(&v).unwrap_or(Value::Null),
              )
            })
            .collect::<HashMap<String, Value>>(),
        ),
      };
      dependency_redis_hmap.append(&mut dependency_map);
      stream_add(ctx, self.output_stream_key.as_bytes(), &Value::from(dependency_redis_hmap));
      let key = ctx.open_key_writable(&deps_map_key);
      key.delete()
        .map_err(|_| DependencyResolverError::KeyDropError)?;
      ctx.open_key_writable(&dcount_key)
        .delete()
        .map_err(|_| DependencyResolverError::KeyDropError)?;
      Ok(())
    } else {
      let key = ctx.open_key_writable(&deps_map_key);
      for (field, value) in dependency_map {
        key.hash_set(
          &field,
          redis_string(ctx, serde_json::to_string(&value).unwrap_or("null".into())),
        );
      }
      Ok(())
    }
  }
}

#[derive(Debug)]
enum TaggerError {
  InvalidIdField,
  CouldNotParseId,
}

fn incr(ctx: &Context, key_redis_str: &RedisString) -> i64 {
  let incr_key= ctx.open_key(&key_redis_str);
  let value: i64 = String::from_utf8_lossy(incr_key.read().unwrap().unwrap_or(&[]).into())
    .parse()
    .unwrap_or(0);
  let incr_key_writable = ctx.open_key_writable(&key_redis_str);
  incr_key_writable.write(&(value + 1).to_string()).unwrap();
  value + 1
}

impl InputTagger {
  fn tag_message(
    &self,
    ctx: &Context,
    message: &mut Map<String, Value>,
  ) -> Result<(), TaggerError> {
    let object_id: String = match message
      .get(&self.obj_id_field)
      .ok_or(TaggerError::InvalidIdField)?
    {
      Value::Bool(p) => Some(p.to_string()),
      Value::Number(n) => Some(n.to_string()),
      Value::String(s) => Some(s.to_string()),
      _ => None,
    }
      .ok_or(TaggerError::CouldNotParseId)?;
    message.insert(
      MSG_ID_FIELD.into(),
      incr(
        ctx,
        &redis_string(
          ctx,
          format!("__{}:{}:{}__", self.stream_key, object_id, MSG_ID_FIELD),
        ),
      )
        .into(),
    );
    Ok(())
  }
}

static PB_CONFIG: LazyLock<RwLock<BrookConfig>> = LazyLock::new(|| {
  let mut taggers = HashMap::new();
  taggers.insert(
    String::from(":ztm-report"),
    InputTagger {
      stream_key: ":ztm-report".into(),
      obj_id_field: "vehicle_number".into(),
    },
  );
  let resolvers = vec![
    DependencyResolver {
      inputs: vec![Dependency {
        stream_key: ":ztm-report".into(),
        fields: vec![DependencyField::new("lat"), DependencyField::new("lon")],
      }],
      output_stream_key: ":direction-args".into(),
    },
    DependencyResolver {
      output_stream_key: ":direction-report".into(),
      inputs: vec![
        Dependency {
          stream_key: ":direction".into(),
          fields: vec![DependencyField::new("result")],
        },
        Dependency {
          stream_key: ":ztm-report".into(),
          fields: vec![DependencyField::new("lat"), DependencyField::new("lon")],
        },
      ],
    },
  ]
    .into_iter()
    .map(|r| (r.output_stream_key.clone(), r))
    .collect::<HashMap<String, DependencyResolver>>();

  let state = BrookConfig {
    dependency_resolvers: resolvers,
    input_taggers: taggers,
  };
  RwLock::new(state)
});

static RESOLVER_MAP: LazyLock<
  RwLock<HashMap<String, Vec<(DependencyResolver, Dependency)>>>,
> = LazyLock::new(|| {
  let state = PB_CONFIG.read().unwrap();
  let mut map = HashMap::new();
  for resolver in state.dependency_resolvers.values() {
    for (stream_key, dependency) in resolver.inputs.iter().map(|d| (d.stream_key.clone(), d)) {
      map.entry(stream_key)
        .or_insert(Vec::new())
        .push((resolver.clone(), dependency.clone()));
    }
  }
  RwLock::new(map)
});

#[inline]
fn redis_string<T: Into<Vec<u8>>>(ctx: &Context, value: T) -> RedisString {
  RedisString::create(NonNull::new(ctx.ctx), value)
}

fn stream_add(ctx: &Context, key_name: &[u8], message: &Value) {
  let mut id = RedisModuleStreamID { ms: 0, seq: 0 };
  let stream_message_id = &mut id as *mut RedisModuleStreamID;

  let mut message_vector: Vec<RedisString> = vec![];
  for (key, value) in message.as_object().unwrap() {
    message_vector.push(redis_string(ctx, key.as_str()));
    message_vector.push(redis_string(
      ctx,
      serde_json::to_string(value).unwrap().as_bytes(),
    )); // what about int, float, etc?
  }
  let mut args = message_vector.iter().map(|v| v.inner).collect::<Vec<_>>();

  let key = raw::open_key(
    ctx.ctx,
    redis_string(ctx, key_name).inner,
    raw::KeyMode::WRITE,
  );
  let status: Status = unsafe {
    RedisModule_StreamAdd.unwrap()(
      key,
      REDISMODULE_STREAM_ADD_AUTOID as c_int,
      stream_message_id,
      (&mut args).as_mut_ptr(),
      (message_vector.len() as i64) / 2,
    )
  }
    .into();
  match status {
    Status::Ok => {}
    Status::Err => {
      panic!("err")
    }
  }
  raw::close_key(key);
}

fn on_stream(ctx: &Context, _event_type: NotifyEvent, _event: &str, key: &'static [u8]) {
  // consider trimming the stream? better than logging the last ID, assuming that this is single threaded
  let key_string = redis_string(ctx, key);
  let stream = ctx.open_key(&key_string);

  let state = PB_CONFIG.read().unwrap();
  let resolver_map = RESOLVER_MAP.read().unwrap();
  // println!("HELLO");
  let stream_key_str = str::from_utf8(key.into()).expect("Only UTF-8 keys are supported!");
  // println!("{stream_key_str} {state:?}");
  // log_warning(format!("Stream: {stream_key_str}"));
  let tagger = state.input_taggers.get(stream_key_str);
  stream_add(
    ctx,
    format!("{stream_key_str}-test").as_bytes(),
    &json!({"message": "none"}),
  );

  // let stream_id = state.stream_read_ids.get(stream_key_str);
  let mut last_record_id = RedisModuleStreamID { ms: 0, seq: 0 };
  for record in stream
    .get_stream_range_iterator(None, None, true, false)
    .unwrap()
  {
    let mut message = serde_json::Map::new();
    for field in record.fields {
      let key = field.0.to_string();
      let value_json = field.1.to_string();
      let value = serde_json::from_str(value_json.as_str()).unwrap_or(Value::Null);
      if value.is_null() {
        log_warning(format!(
          "WARNING: Invalid JSON in message {}-{}, key {}, value {:?}",
          record.id.ms, record.id.seq, key, value_json
        ))
      }
      message.insert(key, value);
    }
    last_record_id = record.id;
    // println!("{}", serde_json::to_string(&message).unwrap());
    if let Some(input_tagger) = tagger {
      if let Err(e) = input_tagger.tag_message(ctx, &mut message) {
        log_warning(format!(
          "WARNING: Tagger error {e:?} for stream {stream_key_str}"
        ));
        continue;
      }
      // log_warning(format!("WARNING: Tagged msg for stream {stream_key_str}"));
    }
    if let Some(dependency_resolvers) = resolver_map.get(stream_key_str) {
      for (resolver, dependency) in dependency_resolvers {
        resolver
          .process_message(ctx, &dependency, &message)
          .unwrap();
      }
    }
  }
  let stream = ctx.open_key_writable(&key_string);
  stream
    .trim_stream_by_id(
      RedisModuleStreamID {
        ms: last_record_id.ms,
        seq: u64::MAX,
      },
      false,
    )
    .unwrap();
  // state.stream_read_ids.insert(stream_key_str, last_record_id); // this should happen even if the function fails
}

#[command(
    {
        name: "pb.setconfig",
        summary: "sets the PyBrook Config",
        flags: [Write],
        arity: 2,
        key_spec: [
            {
                flags: [ReadWrite],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 0, steps : 1, limit : 0 }),
            }
        ]
    }
)]
fn set_config(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
  let mut config = PB_CONFIG.write()?;
  let string = args.get(1).ok_or(RedisError::WrongType)?.to_string();
  let cfg = serde_json::from_str::<BrookConfig>(&string)?;
  *config = cfg.clone();
  Ok(RedisValue::Null)
}

//////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use super::*;
  use std::str::FromStr;
  use Value;

  #[test]
  fn it_works() {
    let val: Value = serde_json::from_str(String::from_str("\"x\"").unwrap().as_str()).unwrap();
  }
}

fn init(_ctx: &Context, _args: &[RedisString]) -> Status {
  let config = PB_CONFIG.read().unwrap();
  log_warning(format!("{}", serde_json::to_string(&*config).unwrap()));
  Status::Ok
}

fn deinit(_ctx: &Context) -> Status {
  Status::Ok
}

#[cfg(not(test))]
redis_module! {
    name: "pybrook",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
    ],
    event_handlers: [
        [@STREAM: on_stream],
    ]
}
