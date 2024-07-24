# Provided commands

`BROOK.REGISTER_PROJECT <project>`
Initializes a project

`BROOK.REGISTER_INPUT <project> <name> <input_stream> <obj_id_field>`

Registers an input stream, usually the id is the same as the input stream
This enables tagging with message ids, which are in the following form: `<obj_id>:<obj_message_id>`, where `<obj_id>` is derived from the `<obj_id_field>`, 
and `<obj_message_id>` is an incremental counter bound to the corresponding object.

`BROOK.REGISTER_DEPENDENCIES <project> <out_stream> <N> <dep_stream_1> <dep_key_1> ... <dep_stream_N> <dep_key_N>`

Registers a list of dependencies, incl their input streams. `out_stream` must be unique for `project`, otherwise will be erased
<!-- TODO: add historical deps -->

`BROOK.REMOVE_ABANDONED_DEPENDENCIES <project> <out_stream_1> <out_stream_N>`


# Core concepts

- Streams should be used only for Artificial Fields, Input Reports and Output Reports, meaning:


For every Input Report, a Message ID is first calculated.
Then all the required fields are sent to corresponding Dependency Resolver HMAPS directly, without an intermediate stream.
If the HMAP has the LENGTH of DEPS_NO, it is sent to an argument stream, which then received by a PyBrook consumer group. 

Thus, a dependency resolver is just a function called by the redis module internally, when a Stream Event occurs,
caused either by an Artificial Field generation or by Input Report arrival.