# Provided commands

`BROOK.REGISTER_PROJECT <project>`
Initializes a project

`BROOK.REGISTER_INPUT <project> <name> <input_stream> <obj_id_field>`

Registers an input stream, usually the id is the same as the input stream
This enables tagging with message ids, which are in the following form: `<obj_id>:<obj_message_id>`, where `<obj_id>` is derived from the `<obj_id_field>`, 
and `<obj_message_id>` is an incremental counter bound to the corresponding object.

`BROOK.REGISTER_DEPENDENCIES <count>`