# Sandbox Binary Protocol

## 1. Overview
This is a simple protocol created for the purposes of learning more about implementing binary protocols over TCP, using
Netty. This is a frame based protocol. All frames begin with a fixed 5-octet header followed by a variable length
payload:

    +--------------------------+
    | Length (16)              |
    +-------------+------------+
    | Type (8)    |
    +-------------+------------+
    | Stream Id (16)           |
    +-------------+------------+------------------------------------+
    | Payload (0...)                                                |
    +---------------------------------------------------------------+

__Length__ - The length of the frame payload expressed as an unsigned 16-bit integer. The value of this field does not
include the header length.

__Type__ - The type of the frame. The frame type determines the format and semantics of the frame.

__Stream Id__ - The stream identifier expressed as an unsigned 16-bit integer. Stream id 0x0 is reserved for connection
control frames.

## 2. Frame Definitions

### Query
Query messages (type=0x0) must send a non-negative _stream id_. _Stream ids_ are used to allow for out of order
messaging. If a client sends 3 queries, Q1, Q2, and Q3, the server may respond in any order, e.g. R3, R1, R2.
_Stream id_ allows for the client to determine which _response_ corresponds to which _query_.

### Response
Query messages (type=0x1) must send the _stream id_ of the _query_ they are responding to. If a _query_ sends a
_stream id_ of X, the _response_ will send back a _stream id_ of X.

### Ping
Ping messages (type=0x2) is a connection control message used to determine if an idle connection is still functional.
Ping messages use a _stream id_ of _0x0_.