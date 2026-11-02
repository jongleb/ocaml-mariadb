open Printf
open Util

module B = Binding_wrappers
module T = Ffi_generated.Types

module Time = Time
module Field = Field
module Row = Row
module Status = Wait_status

type t = [`Nonblocking] Common.t
type mariadb = t

type error = Common.error
type 'a result = [`Ok of 'a | `Wait of Status.t | `Error of error]

type server_option = Common.server_option

type 'a start = unit -> 'a result
type 'a cont = Status.t -> 'a result
type 'a nonblocking = 'a start * 'a cont

let map_result f = function
  | `Ok x -> `Ok (f x)
  | `Wait _ as w -> w
  | `Error _ as e -> e

type options =
  | Nonblocking

let options raw = function
  | Nonblocking ->
      B.mysql_options raw T.Options.nonblock Ctypes.null

let init () =
  match B.mysql_init () with
  | Some raw ->
      options raw Nonblocking;
      Some raw
  | None ->
      None

let handle_void = function
  | 0 -> `Ok
  | s -> `Wait (Status.of_int s)

let handle_opt mariadb = function
  | 0, Some _ -> `Ok mariadb
  | 0, None -> `Error (Common.error mariadb)
  | s, _ -> `Wait (Status.of_int s)

let handle_int mariadb = function
  | 0, 0 -> `Ok ()
  | 0, _ -> `Error (Common.error mariadb)
  | s, _ -> `Wait (Status.of_int s)

let handle_char mariadb = function
  | 0, '\000' -> `Ok ()
  | 0, _ -> `Error (Common.error mariadb)
  | s, _ -> `Wait (Status.of_int s)

let connect_start mariadb =
  let open Common in
  handle_opt mariadb
    (B.mysql_real_connect_start
      mariadb.raw
      mariadb.host
      mariadb.user
      mariadb.pass
      mariadb.db
      mariadb.port
      mariadb.socket
      mariadb.flags)

let connect_cont mariadb status =
  handle_opt mariadb
    (B.mysql_real_connect_cont mariadb.Common.raw (Status.to_int status))

let connect mariadb =
  (connect_start mariadb, connect_cont mariadb)

let close_start mariadb =
  handle_void (B.mysql_close_start mariadb.Common.raw)

let close_cont mariadb status =
  handle_void (B.mysql_close_cont mariadb.Common.raw status)

let close mariadb =
  (close_start mariadb, close_cont mariadb)

let fd mariadb =
  Obj.magic @@ B.mysql_get_socket mariadb.Common.raw

let timeout mariadb =
  B.mysql_get_timeout_value mariadb.Common.raw

let timeout_ms mariadb =
  B.mysql_get_timeout_value_ms mariadb.Common.raw

let set_character_set_start mariadb =
  let charset = Option.some mariadb.Common.charset in
  handle_int mariadb
    (B.mysql_set_character_set_start mariadb.Common.raw charset)

let set_character_set_cont mariadb status =
  handle_int mariadb (B.mysql_set_character_set_cont mariadb.Common.raw status)

let set_character_set mariadb =
  (set_character_set_start mariadb, set_character_set_cont mariadb)

let select_db_start mariadb =
  let db = Option.some mariadb.Common.db in
  handle_int mariadb (B.mysql_select_db_start mariadb.Common.raw db)

let select_db_cont mariadb status =
  handle_int mariadb (B.mysql_select_db_cont mariadb.Common.raw status)

let select_db mariadb =
  (select_db_start mariadb, select_db_cont mariadb)

let change_user_start mariadb =
  let user = Option.some mariadb.Common.user in
  let pass = Option.some mariadb.Common.pass in
  handle_char mariadb
    (B.mysql_change_user_start mariadb.Common.raw user pass mariadb.Common.db)

let change_user_cont mariadb status =
  handle_char mariadb (B.mysql_change_user_cont mariadb.Common.raw status)

let change_user mariadb =
  (change_user_start mariadb, change_user_cont mariadb)

let set_server_option_start mariadb opt =
  let opt = Common.int_of_server_option opt in
  handle_int mariadb (B.mysql_set_server_option_start mariadb.Common.raw opt)

let set_server_option_cont mariadb status =
  handle_int mariadb (B.mysql_set_server_option_cont mariadb.Common.raw status)

let set_server_option mariadb opt =
  (set_server_option_start mariadb opt, set_server_option_cont mariadb)

let ping_start mariadb =
  handle_int mariadb (B.mysql_ping_start mariadb.Common.raw)

let ping_cont mariadb status =
  handle_int mariadb (B.mysql_ping_cont mariadb.Common.raw status)

let ping mariadb =
  (ping_start mariadb, ping_cont mariadb)

let autocommit_start mariadb auto =
  handle_char mariadb (B.mysql_autocommit_start mariadb.Common.raw auto)

let autocommit_cont mariadb status =
  handle_char mariadb (B.mysql_autocommit_cont mariadb.Common.raw status)

let autocommit mariadb auto =
  (autocommit_start mariadb auto, autocommit_cont mariadb)

let commit_start mariadb =
  handle_char mariadb (B.mysql_commit_start mariadb.Common.raw)

let commit_cont mariadb status =
  handle_char mariadb (B.mysql_commit_cont mariadb.Common.raw status)

let commit mariadb =
  (commit_start mariadb, commit_cont mariadb)

let rollback_start mariadb =
  handle_char mariadb (B.mysql_rollback_start mariadb.Common.raw)

let rollback_cont mariadb status =
  handle_char mariadb (B.mysql_rollback_cont mariadb.Common.raw status)

let rollback mariadb =
  (rollback_start mariadb, rollback_cont mariadb)

type text_query =
  { query : char Ctypes.ptr
  ; len   : int
  }

let text_query query =
  { query = char_ptr_buffer_of_string query
  ; len   = String.length query
  }

let real_query_start mariadb q =
  handle_int mariadb (B.mysql_real_query_start mariadb.Common.raw q.query q.len)

let real_query_cont mariadb _q status =
  handle_int mariadb (B.mysql_real_query_cont mariadb.Common.raw status)

let real_query mariadb query =
  let q = text_query query in
  (real_query_start mariadb q, real_query_cont mariadb q)

let start_txn mariadb =
  real_query mariadb "START TRANSACTION"

let build_stmt mariadb raw =
  `Ok (Common.Stmt.init mariadb raw)

type prep_stmt =
  { raw   : B.stmt
  ; query : char Ctypes.ptr
  ; len   : int
  }

let handle_prepare mariadb stmt = function
  | 0, 0 -> build_stmt mariadb stmt.raw
  | 0, _ -> `Error (Common.error mariadb)
  | s, _ -> `Wait (Status.of_int s)

let prepare_start mariadb stmt =
  handle_prepare mariadb stmt
    (B.mysql_stmt_prepare_start stmt.raw stmt.query stmt.len)

let prepare_cont mariadb stmt status =
  handle_prepare mariadb stmt (B.mysql_stmt_prepare_cont stmt.raw status)

let prepare mariadb query =
  match Common.stmt_init mariadb with
  | Some raw ->
      let stmt =
        { raw
        ; query = char_ptr_buffer_of_string query
        ; len   = String.length query
        } in
      `Ok (prepare_start mariadb stmt, prepare_cont mariadb stmt)
  | None -> `Error (Common.error mariadb)

module Res = struct
  type t = [`Nonblocking] Common.Res.t

  let num_rows =
    Common.Res.num_rows

  let affected_rows =
    Common.Res.affected_rows

  let insert_id =
    Common.Res.insert_id

  let handle_fetch (type t) (module R : Row.S with type t = t) res = function
    | 0, 0 ->
        `Ok (Common.Res.build_row (module R) res)
    | 0, 1 ->
        let stmt = res.Common.Res.stmt in
        `Error (B.mysql_stmt_errno stmt, B.mysql_stmt_error stmt)
    | 0, r when r = T.Return_code.no_data ->
        `Ok None
    | 0, r when r = T.Return_code.data_truncated ->
        `Error (2032, "truncated data")
    | s, _ ->
        `Wait (Status.of_int s)

  let fetch_start (type t) (module R : Row.S with type t = t) res =
    handle_fetch (module R) res (B.mysql_stmt_fetch_start res.Common.Res.stmt)

  let fetch_cont (type t) (module R : Row.S with type t = t) res status =
    handle_fetch (module R) res
      (B.mysql_stmt_fetch_cont res.Common.Res.stmt status)

  let fetch (type t) (module R : Row.S with type t = t) res =
    (fetch_start (module R) res, fetch_cont (module R) res)

  let handle_free = function
    | 0 -> `Ok
    | s -> `Wait (Status.of_int s)

  let free_start res =
    handle_free (B.mysql_free_result_start res)

  let free_cont res status =
    handle_free (B.mysql_free_result_cont res status)

  let free res =
    (free_start res, free_cont res)
end

module Stmt = struct
  type t = [`Nonblocking] Common.Stmt.t
  type 'a result = [`Ok of 'a | `Wait of Status.t | `Error of error]

  let init =
    Common.Stmt.init

  let handle_execute stmt = function
    | 0, 0 -> `Ok stmt
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let execute_start stmt =
    handle_execute stmt (B.mysql_stmt_execute_start stmt.Common.Stmt.raw)

  let execute_cont stmt status =
    handle_execute stmt (B.mysql_stmt_execute_cont stmt.Common.Stmt.raw status)

  let execute stmt params =
    let n = B.mysql_stmt_param_count stmt.Common.Stmt.raw in
    let len = Array.length params in
    if n <> len then
      let err = sprintf "parameter count mismatch: %d (expected %d)" len n in
      `Error (2034, err)
    else
      match Common.Stmt.bind_params stmt params with
      | `Ok bound -> `Ok (execute_start bound, execute_cont bound)
      | `Error _ as err -> err

  let handle_store_result stmt = function
    | 0, 0 -> Common.Stmt.bind_result stmt
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let store_result_start stmt =
    handle_store_result stmt
      (B.mysql_stmt_store_result_start stmt.Common.Stmt.raw)

  let store_result_cont stmt status =
    handle_store_result stmt
      (B.mysql_stmt_store_result_cont stmt.Common.Stmt.raw status)

  let store_result stmt =
    (store_result_start stmt, store_result_cont stmt)

  let handle_free_result stmt = function
    | 0, '\000' -> `Ok ()
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let free_result_start stmt =
    handle_free_result stmt (B.mysql_stmt_free_result_start stmt.Common.Stmt.raw)

  let free_result_cont stmt status =
    handle_free_result stmt (B.mysql_stmt_free_result_cont stmt.Common.Stmt.raw status)

  let free_result stmt =
    (free_result_start stmt, free_result_cont stmt)

  let handle_char_unit stmt = function
    | 0, '\000' -> `Ok ()
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let close_start stmt =
    handle_char_unit stmt (B.mysql_stmt_close_start stmt.Common.Stmt.raw)

  let close_cont stmt status =
    handle_char_unit stmt (B.mysql_stmt_close_cont stmt.Common.Stmt.raw status)

  let close stmt =
    (close_start stmt, close_cont stmt)

  let handle_reset stmt = function
    | 0, '\000' -> `Ok ()
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let reset_start stmt =
    handle_reset stmt (B.mysql_stmt_reset_start stmt.Common.Stmt.raw)

  let reset_cont stmt status =
    handle_reset stmt (B.mysql_stmt_reset_cont stmt.Common.Stmt.raw status)

  let reset stmt =
    (reset_start stmt, reset_cont stmt)

  let handle_next stmt = function
    | 0, 0 -> `Ok true
    | 0, -1 -> `Ok false
    | 0, _ -> `Error (Common.Stmt.error stmt)
    | s, _ -> `Wait (Status.of_int s)

  let next_result_start stmt =
    handle_next stmt (B.mysql_stmt_next_result_start stmt.Common.Stmt.raw)

  let next_result_cont stmt status =
    handle_next stmt (B.mysql_stmt_next_result_cont stmt.Common.Stmt.raw status)
end

module type Wait = sig
  module IO : sig
    type 'a future
    val (>>=) : 'a future -> ('a -> 'b future) -> 'b future
    val return : 'a -> 'a future
  end

  val wait : t -> Status.t -> Status.t IO.future
end

module type S = sig
  type error = int * string
  type 'a future
  type 'a result = ('a, error) Stdlib.result

  module Time : sig
    type t

    val year : t -> int
    val month : t -> int
    val day : t -> int
    val hour : t -> int
    val minute : t -> int
    val second : t -> int
    val microsecond : t -> int

    val time : hour:int -> minute:int -> second:int
            -> ?microsecond:int -> unit -> t
    val local_timestamp : float -> t
    val utc_timestamp : float -> t
    val date : year:int -> month:int -> day:int -> unit -> t
    val datetime : year:int -> month:int -> day:int
                -> hour:int -> minute:int -> second:int
                -> ?microsecond:int -> unit -> t
  end

  module Field : sig
    type t

    type value =
      [ `Null
      | `Int of int
      | `Int64 of Int64.t
      | `UInt64 of Unsigned.UInt64.t
      | `Float of float
      | `String of string
      | `Bytes of bytes
      | `Time of Time.t
      ]

    val name : t -> string
    val value : t -> value
    val null_value : t -> bool
    val can_be_null : t -> bool

    val int : t -> int
    val int64 : t -> Int64.t
    val uint64 : t -> Unsigned.UInt64.t
    val float : t -> float
    val string : t -> string
    val bytes : t -> bytes
    val time : t -> Time.t

    val int_opt : t -> int option
    val int64_opt : t -> Int64.t option
    val uint64_opt : t -> Unsigned.UInt64.t option
    val float_opt : t -> float option
    val string_opt : t -> string option
    val bytes_opt : t -> bytes option
    val time_opt : t -> Time.t option
  end

  module Row : sig
    module type S = sig
      type t
      val build : int -> (int -> Field.t) -> t
    end

    module StringMap : Map.S with type key = string

    module Array : (S with type t = Field.t array)
    module Map : (S with type t = Field.t StringMap.t)
    module Hashtbl : (S with type t = (string, Field.t) Hashtbl.t)
  end

  module Res : sig
    type t

    val num_rows : t -> int
    val affected_rows : t -> int
    val insert_id : t -> int
    val fetch : (module Row.S with type t = 'r) -> t -> 'r option result future
  end

  module Stmt : sig
    type t

    val execute : t -> Field.value array -> Res.t result future
    val reset : t -> unit result future
    val sqlstate : t -> string
    val close : t -> unit result future
  end

  type t

  type flag =
    | Compress
    | Found_rows
    | Ignore_sigpipe
    | Ignore_space
    | Interactive
    | Local_files
    | Multi_results
    | Multi_statements
    | No_schema
    | Odbc
    | Ssl
    | Remember_options

  type protocol =
    | Default
    | Tcp
    | Socket
    | Pipe
    | Memory

  type client_option =
    | Connect_timeout of int
    | Compress
    | Named_pipe of string
    | Init_command of string
    | Read_default_file of string
    | Read_default_group of string
    | Set_charset_dir of string
    | Set_charset_name of string
    | Local_infile of bool
    | Protocol of protocol
    | Shared_memory_base_name of string
    | Read_timeout of int
    | Write_timeout of int
    | Secure_auth of bool
    | Report_data_truncation of bool
    | Reconnect of bool
    | Ssl_verify_server_cert of bool
    | Plugin_dir of string
    | Default_auth of string
    | Bind of string
    | Ssl_key of string
    | Ssl_cert of string
    | Ssl_ca of string
    | Ssl_capath of string
    | Ssl_cipher of string
    | Ssl_crl of string
    | Ssl_crlpath of string
    | Connect_attr_reset
    | Connect_attr_add of string * string
    | Connect_attr_delete of string
    | Server_public_key of string
    | Enable_cleartext_plugin of bool

  type server_option =
    | Multi_statements of bool

  val connect : ?host:string
             -> ?user:string
             -> ?pass:string
             -> ?db:string -> ?port:int -> ?socket:string
             -> ?flags:flag list
             -> ?options:client_option list -> unit
             -> t result future

  val close : t -> unit future
  val library_end : unit -> unit
  val set_character_set : t -> string -> unit result future
  val select_db : t -> string -> unit result future
  val change_user : t -> string -> string -> string option -> unit result future
  val get_server_info : t -> string
  val get_server_version : t -> int
  val get_host_info : t -> string
  val get_proto_info : t -> int
  val set_client_option : t -> client_option -> unit
  val set_server_option : t -> server_option -> unit result future
  val ping : t -> unit result future
  val autocommit : t -> bool -> unit result future
  val start_txn : t -> unit result future
  val commit : t -> unit result future
  val rollback : t -> unit result future
  val prepare : t -> string -> Stmt.t result future

  type exec_result = { affected_rows : int; insert_id : int }

  val exec : t -> string -> exec_result result future

  val sqlstate : t -> string
end

module Make (W : Wait) = struct
  type t = mariadb

  type 'a future = 'a W.IO.future
  type error = int * string
  type 'a result = ('a, error) Stdlib.result

  let (>>=) = W.IO.(>>=)
  let return = W.IO.return
  let return_unit = return ()

  type flag = Common.flag =
    | Compress
    | Found_rows
    | Ignore_sigpipe
    | Ignore_space
    | Interactive
    | Local_files
    | Multi_results
    | Multi_statements
    | No_schema
    | Odbc
    | Ssl
    | Remember_options

  type protocol = Common.protocol =
    | Default
    | Tcp
    | Socket
    | Pipe
    | Memory

  type client_option = Common.client_option =
    | Connect_timeout of int
    | Compress
    | Named_pipe of string
    | Init_command of string
    | Read_default_file of string
    | Read_default_group of string
    | Set_charset_dir of string
    | Set_charset_name of string
    | Local_infile of bool
    | Protocol of protocol
    | Shared_memory_base_name of string
    | Read_timeout of int
    | Write_timeout of int
    | Secure_auth of bool
    | Report_data_truncation of bool
    | Reconnect of bool
    | Ssl_verify_server_cert of bool
    | Plugin_dir of string
    | Default_auth of string
    | Bind of string
    | Ssl_key of string
    | Ssl_cert of string
    | Ssl_ca of string
    | Ssl_capath of string
    | Ssl_cipher of string
    | Ssl_crl of string
    | Ssl_crlpath of string
    | Connect_attr_reset
    | Connect_attr_add of string * string
    | Connect_attr_delete of string
    | Server_public_key of string
    | Enable_cleartext_plugin of bool

  type server_option = Common.server_option =
    | Multi_statements of bool

  let rec nonblocking m (r, k) =
    match r with
    | `Ok v -> return (Ok v)
    | `Wait s -> W.wait m s >>= fun s -> nonblocking m (k s, k)
    | `Error e -> return (Error e)

  let rec nonblocking' m (r, k) =
    match r with
    | `Ok -> return_unit
    | `Wait s -> W.wait m s >>= fun s -> nonblocking' m (k s, k)

  module Time = Time
  module Field = Field
  module Row = Row

  module Res = struct
    type t = Res.t

    let fetch (type t) (module R : Row.S with type t = t) res =
      nonblocking res.Common.Res.mariadb (Res.fetch (module R) res)

    let num_rows =
      Res.num_rows

    let affected_rows =
      Res.affected_rows

    let insert_id =
      Res.insert_id

    let free = (* not public, but to avoid shadowing *)
      Res.free
  end

  module Stmt = struct
    type t = Stmt.t

    let free_meta stmt =
      match stmt.Common.Stmt.meta with
      | None -> return ()
      | Some { res; _ } ->
          stmt.Common.Stmt.meta <- None;
          nonblocking' stmt.Common.Stmt.mariadb (Res.free res)

    let free_meta_and_result stmt =
      match stmt.Common.Stmt.meta with
      | None -> return (Ok ())
      | Some { res; _ } ->
          stmt.Common.Stmt.meta <- None;
          nonblocking' stmt.Common.Stmt.mariadb (Res.free res) >>= fun () ->
          nonblocking stmt.Common.Stmt.mariadb (Stmt.free_result stmt)

    let reset_without_freeing stmt =
      nonblocking stmt.Common.Stmt.mariadb (Stmt.reset stmt)

    let handle_execute = function
      | Ok stmt -> nonblocking stmt.Common.Stmt.mariadb (Stmt.store_result stmt)
      | Error _ as e -> return e

    let execute stmt ps =
      free_meta stmt >>= fun () ->
      match Stmt.execute stmt ps with
      | `Ok nb -> nonblocking stmt.Common.Stmt.mariadb nb >>= handle_execute
      | `Error e -> return (Error e)

    let reset stmt =
      free_meta_and_result stmt
      >>= function
      | Ok () -> nonblocking stmt.Common.Stmt.mariadb (Stmt.reset stmt)
      | Error _ as e -> return e

    let close stmt =
      free_meta_and_result stmt
      >>= function
      | Ok () -> nonblocking stmt.Common.Stmt.mariadb (Stmt.close stmt)
      | Error _ as e -> return e

    let sqlstate = Common.Stmt.sqlstate
  end

  module Streaming = struct
    open Ctypes

    module Config = struct
      type t =
        { max_field_bytes : int
        ; max_row_bytes : int
        }

      type error =
        | Non_positive_max_field_bytes of int
        | Non_positive_max_row_bytes of int
        | Field_limit_exceeds_row_limit of
            { max_field_bytes : int
            ; max_row_bytes : int
            }

      let unbuffered ~max_field_bytes ~max_row_bytes =
        if max_field_bytes <= 0 then
          Error (Non_positive_max_field_bytes max_field_bytes)
        else if max_row_bytes <= 0 then
          Error (Non_positive_max_row_bytes max_row_bytes)
        else if max_field_bytes > max_row_bytes then
          Error
            (Field_limit_exceeds_row_limit
               { max_field_bytes
               ; max_row_bytes
               })
        else
          Ok { max_field_bytes; max_row_bytes }
    end

    module Control = struct
      type t =
        | Continue
        | Stop
    end

    module Completion = struct
      type t =
        | Exhausted of { rows_read : int }
        | Stopped of { rows_read : int }
    end

    module Error = struct
      type connector =
        { code : int
        ; message : string
        ; sqlstate : string
        }

      type operation =
        | Connector_failed of connector
        | Connection_lost of connector
        | No_result_set
        | Parameter_count_mismatch of
            { actual : int
            ; expected : int
            }
        | Fixed_width_truncation of
            { column : int
            ; length : Unsigned.UInt64.t
            ; buffer_bytes : int
            }
        | Field_limit_exceeded of
            { column : int
            ; length : Unsigned.UInt64.t
            ; max_field_bytes : int
            }
        | Row_limit_exceeded of
            { length : Unsigned.UInt64.t
            ; max_row_bytes : int
            }
        | Callback_failed of exn
        | Runtime_failed of exn

      type t =
        | Operation_failed of operation
        | Cleanup_failed of
            { operation : operation option
            ; cleanup : connector
            }
    end

    module type Lifecycle = sig
      val catch :
        (unit -> 'a future) ->
        (exn -> 'a future) ->
        'a future

      val no_cancel : 'a future -> 'a future
      val pause : unit -> unit future
    end

    module Make (L : Lifecycle) = struct
      let execute
          (type row)
          ~config
          (module R : Row.S with type t = row)
          ~on_row
          stmt
          params =
        let module Execution = struct
          type fetch_outcome =
            | Fetched_row
            | End_of_result

          type column_buffer =
            | Fixed_width_buffer of int
            | Variable_width_buffer of unit ptr

          type bound_result =
            { meta : Common.Res.meta
            ; column_buffers : column_buffer array
            ; scratch_bytes : int
            }
        end in
        let open Execution in

        let connector_snapshot (code, message) : Error.connector =
          let code, message =
            if code = 0 then
              Common.error stmt.Common.Stmt.mariadb
            else
              code, message
          in
          { code
          ; message
          ; sqlstate = Common.Stmt.sqlstate stmt
          }
        in

        let operation_error error =
          match connector_snapshot error with
          | { code = 2006 | 2013; _ } as connector ->
              Error.Connection_lost connector
          | connector -> Error.Connector_failed connector
        in

        let ( let* ) future continue =
          future >>= function
          | Ok value -> continue value
          | Error operation -> return (Error operation)
        in

        let bind_parameters () =
          let expected = stmt.Common.Stmt.num_params in
          let actual = Array.length params in
          if actual <> expected then
            Error (Error.Parameter_count_mismatch { actual; expected })
          else
            match Common.Stmt.bind_params stmt params with
            | `Ok _ -> Ok ()
            | `Error error -> Error (operation_error error)
        in

        let cleanup () =
          Stmt.free_meta_and_result stmt >>= fun released ->
          Stmt.reset_without_freeing stmt >>= fun reset_result ->
          match released, reset_result with
          | Ok (), Ok () -> return (Ok ())
          | Error error, _
          | Ok (), Error error ->
              return (Error (connector_snapshot error))
        in

        let nonblocking_stmt handle start cont =
          nonblocking
            stmt.Common.Stmt.mariadb
            ( handle start
            , fun status -> handle (cont (Status.to_int status)) )
          >>= function
          | Ok value -> return (Ok value)
          | Error error -> return (Error (operation_error error))
        in

        let execute_statement () =
          let handle = function
            | 0, 0 -> `Ok ()
            | 0, _ -> `Error (Common.Stmt.error stmt)
            | status, _ -> `Wait (Status.of_int status)
          in
          nonblocking_stmt
            handle
            (B.mysql_stmt_execute_start stmt.Common.Stmt.raw)
            (B.mysql_stmt_execute_cont stmt.Common.Stmt.raw)
        in

        let fetch () =
          let handle = function
            | 0, 0 -> `Ok Fetched_row
            | 0, result when result = T.Return_code.data_truncated ->
                `Ok Fetched_row
            | 0, result when result = T.Return_code.no_data ->
                `Ok End_of_result
            | 0, _ -> `Error (Common.Stmt.error stmt)
            | status, _ -> `Wait (Status.of_int status)
          in
          nonblocking_stmt
            handle
            (B.mysql_stmt_fetch_start stmt.Common.Stmt.raw)
            (B.mysql_stmt_fetch_cont stmt.Common.Stmt.raw)
        in

        let bind_buffer result column buffer bytes =
          let binding = !@(result.Bind.bind +@ column) in
          result.Bind.buffers.(column) <- buffer;
          setf binding T.Bind.buffer buffer;
          setf
            binding
            T.Bind.buffer_length
            (Unsigned.ULong.of_int bytes)
        in

        let bind_result () =
          match B.mysql_stmt_result_metadata stmt.Common.Stmt.raw with
          | None -> Error Error.No_result_set
          | Some raw ->
              let field_count = B.mysql_num_fields raw in
              let bindings =
                try Common.Stmt.alloc_result raw field_count
                with exception_ ->
                  B.mysql_free_result raw;
                  raise exception_
              in
              stmt.Common.Stmt.meta <-
                Some { res = raw; result = bindings };
              let buffer_sizes =
                Array.init field_count (fun column ->
                    let binding = bindings.Bind.bind +@ column in
                    let declared_type =
                      getf (!@binding) T.Bind.buffer_type
                    in
                    let buffer_type =
                      if
                        declared_type = T.Type.year
                        || declared_type = T.Type.short
                      then begin
                        setf (!@binding) T.Bind.buffer_type T.Type.long;
                        T.Type.long
                      end
                      else
                        declared_type
                    in
                    Common.Stmt.buffer_size buffer_type)
              in
              let variable_count =
                Array.fold_left
                  (fun count size ->
                    if size < 0 then count + 1 else count)
                  0
                  buffer_sizes
              in
              let scratch_bytes =
                if variable_count = 0 then
                  0
                else
                  min
                    (min 4096 config.Config.max_field_bytes)
                    (max 1 (config.Config.max_row_bytes / variable_count))
              in
              let column_buffers =
                Array.mapi
                  (fun column size ->
                    if size < 0 then
                      let scratch_buffer =
                        Common.Stmt.malloc scratch_bytes
                      in
                      bind_buffer
                        bindings
                        column
                        scratch_buffer
                        scratch_bytes;
                      Variable_width_buffer scratch_buffer
                    else begin
                      bind_buffer
                        bindings
                        column
                        (Common.Stmt.malloc size)
                        size;
                      Fixed_width_buffer size
                    end)
                  buffer_sizes
              in
              if B.mysql_stmt_bind_result stmt.Common.Stmt.raw bindings.Bind.bind
              then
                Ok
                  { meta =
                      Common.Res.meta bindings raw bindings.Bind.buffers
                  ; column_buffers
                  ; scratch_bytes
                  }
              else
                Error (operation_error (Common.Stmt.error stmt))
        in

        let uint64_of_ulong length =
          Unsigned.UInt64.of_int64 (Unsigned.ULong.to_int64 length)
        in

        let capture_runtime_failure work =
          try work ()
          with exception_ -> Error (Error.Runtime_failed exception_)
        in

        let max_field_length =
          Unsigned.ULong.of_int config.Config.max_field_bytes
        in

        let fetch_truncated_columns bound =
          let bindings = bound.meta.Common.Res.result in
          let fields = bound.meta.Common.Res.fields in
          let fetch_column column row_bytes =
            if Field.null_value fields.(column) then
              Ok row_bytes
            else
              let length = !@(bindings.Bind.length +@ column) in
              if Unsigned.ULong.compare length max_field_length > 0 then
                Error
                  (Error.Field_limit_exceeded
                     { column
                     ; length = uint64_of_ulong length
                     ; max_field_bytes = config.Config.max_field_bytes
                     })
              else
                let length = Unsigned.ULong.to_int length in
                if length > config.Config.max_row_bytes - row_bytes then
                  Error
                    (Error.Row_limit_exceeded
                       { length =
                           Unsigned.UInt64.add
                             (Unsigned.UInt64.of_int row_bytes)
                             (Unsigned.UInt64.of_int length)
                       ; max_row_bytes = config.Config.max_row_bytes
                       })
                else
                  let row_bytes = row_bytes + length in
                  match bound.column_buffers.(column) with
                  | Fixed_width_buffer buffer_bytes ->
                      if length <= buffer_bytes then
                        Ok row_bytes
                      else
                        Error
                          (Error.Fixed_width_truncation
                             { column
                             ; length =
                                 Unsigned.UInt64.of_int length
                             ; buffer_bytes
                             })
                  | Variable_width_buffer _ ->
                      if length <= bound.scratch_bytes then
                        Ok row_bytes
                      else
                        let binding = bindings.Bind.bind +@ column in
                        bind_buffer
                          bindings
                          column
                          (Common.Stmt.malloc length)
                          length;
                        if
                          B.mysql_stmt_fetch_column
                            stmt.Common.Stmt.raw
                            binding
                            column
                        then
                          Ok row_bytes
                        else
                          Error (operation_error (Common.Stmt.error stmt))
          in
          let rec fetch_next column row_bytes =
            if column = bindings.Bind.n then
              Ok ()
            else
              match fetch_column column row_bytes with
              | Error operation -> Error operation
              | Ok row_bytes -> fetch_next (column + 1) row_bytes
          in
          fetch_next 0 0
        in

        let restore_scratch_buffers bound =
          let bindings = bound.meta.Common.Res.result in
          Array.iteri
            (fun column -> function
              | Fixed_width_buffer _ -> ()
              | Variable_width_buffer scratch_buffer ->
                  bind_buffer
                    bindings
                    column
                    scratch_buffer
                    bound.scratch_bytes)
            bound.column_buffers
        in

        let invoke_callback row bound =
          L.catch
            (fun () -> on_row row >>= fun control -> return (Ok control))
            (fun exception_ ->
              return (Error (Error.Callback_failed exception_)))
          >>= fun outcome ->
          restore_scratch_buffers bound;
          return outcome
        in

        let rec consume bound rows_read =
          let* fetched = fetch () in
          match fetched with
          | End_of_result ->
              return (Ok (Completion.Exhausted { rows_read }))
          | Fetched_row ->
              (match capture_runtime_failure (fun () ->
                 match fetch_truncated_columns bound with
                 | Error operation -> Error operation
                 | Ok () ->
                     let meta = bound.meta in
                     Ok
                       (R.build
                          meta.Common.Res.result.Bind.n
                          (fun column -> meta.Common.Res.fields.(column))))
               with
               | Error operation ->
                   restore_scratch_buffers bound;
                   return (Error operation)
               | Ok row ->
                   let* control = invoke_callback row bound in
                   let rows_read = rows_read + 1 in
                   (match control with
                    | Control.Continue ->
                        (if rows_read mod 256 = 0 then
                           L.pause ()
                         else
                           return ())
                        >>= fun () -> consume bound rows_read
                    | Control.Stop ->
                        return (Ok (Completion.Stopped { rows_read }))))
        in

        let cleanup_failed operation cleanup_error =
          Error
            (Error.Cleanup_failed
               { operation
               ; cleanup = cleanup_error
               })
        in

        let finish operation cleanup_result =
          match operation, cleanup_result with
          | Ok completion, Ok () -> Ok completion
          | Error operation, Ok () ->
              Error (Error.Operation_failed operation)
          | Ok _, Error cleanup_error ->
              cleanup_failed None cleanup_error
          | Error operation, Error cleanup_error ->
              cleanup_failed (Some operation) cleanup_error
        in

        let run () =
          cleanup () >>= function
          | Error cleanup_error ->
              return (cleanup_failed None cleanup_error)
          | Ok () ->
              let operation =
                let* () =
                  return (capture_runtime_failure bind_parameters)
                in
                let* () = execute_statement () in
                let* bound =
                  return (capture_runtime_failure bind_result)
                in
                consume bound 0
              in
              operation >>= fun operation ->
              cleanup () >>= fun cleanup_result ->
              return (finish operation cleanup_result)
        in

        L.no_cancel
          (L.catch run (fun exception_ ->
             return
               (Error
                  (Error.Operation_failed
                     (Error.Runtime_failed exception_)))))
    end
  end

  let connect ?host ?user ?pass ?db ?(port=0) ?socket ?(flags=[]) ?(options=[]) () =
    match init () with
    | Some raw ->
        let mariadb = Common.
          { raw
          ; host    = char_ptr_opt_buffer_of_string host
          ; port    = port
          ; user    = char_ptr_opt_buffer_of_string user
          ; pass    = char_ptr_opt_buffer_of_string pass
          ; db      = char_ptr_opt_buffer_of_string db
          ; socket  = char_ptr_opt_buffer_of_string socket
          ; flags   = Common.int_of_flags flags
          ; charset = None
          } in
        List.iter (Common.set_client_option mariadb) options;
        nonblocking mariadb (connect mariadb)
    | None ->
        return (Error (2008, "out of memory"))

  let close m = nonblocking' m (close m)

  let library_end = Common.library_end

  let set_character_set m c =
    let c = Some (char_ptr_buffer_of_string c) in
    m.Common.charset <- c;
    nonblocking m (set_character_set m)

  let select_db m db =
    m.Common.db <- Some (char_ptr_buffer_of_string db);
    nonblocking m (select_db m)

  let change_user m user pass db =
    m.Common.user <- Some (char_ptr_buffer_of_string user);
    m.Common.pass <- Some (char_ptr_buffer_of_string pass);
    m.Common.db <- char_ptr_opt_buffer_of_string db;
    nonblocking m (change_user m)

  let get_server_info = Common.get_server_info

  let get_server_version = Common.get_server_version

  let get_host_info = Common.get_host_info

  let get_proto_info = Common.get_proto_info

  let set_client_option = Common.set_client_option

  let set_server_option m opt = nonblocking m (set_server_option m opt)

  let ping m = nonblocking m (ping m)

  let autocommit m b = nonblocking m (autocommit m b)

  let start_txn m = nonblocking m (start_txn m)

  let commit m = nonblocking m (commit m)

  let rollback m = nonblocking m (rollback m)

  let prepare m q =
    match prepare m q with
    | `Ok nb -> nonblocking m nb
    | `Error e -> return (Error e)

  type exec_result = Common.exec_result =
    { affected_rows : int
    ; insert_id     : int
    }

  let handle_exec m =
    match Common.query_result m with
    | Ok result ->
        return (Ok result)
    | Error (Some res) ->
        nonblocking' m (Res.free res) >>= fun () ->
        return (Error (0, "exec: statement returned a result set, use prepare"))
    | Error None ->
        return (Error (Common.error m))

  let exec m q =
    nonblocking m (real_query m q)
    >>= function
    | Ok () -> handle_exec m
    | Error _ as e -> return e

  let sqlstate = Common.sqlstate
end
