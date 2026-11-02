open Lwt.Syntax

module Wait = struct
  module IO = struct
    type 'a future = 'a Lwt.t

    let (>>=) future continue = Lwt.bind future continue
    let return value = Lwt.return value
  end

  let wait mariadb status =
    let fd =
      Lwt_unix.of_unix_file_descr (Mariadb.Nonblocking.fd mariadb)
    in
    let idle, _ = Lwt.task () in
    let readable =
      if Mariadb.Nonblocking.Status.read status then
        Lwt_unix.wait_read fd
      else
        idle
    in
    let writable =
      if Mariadb.Nonblocking.Status.write status then
        Lwt_unix.wait_write fd
      else
        idle
    in
    let timeout =
      match
        Mariadb.Nonblocking.Status.timeout status,
        Mariadb.Nonblocking.timeout mariadb
      with
      | true, 0 -> Lwt.return_unit
      | true, seconds -> Lwt_unix.timeout (float_of_int seconds)
      | false, _ -> idle
    in
    Lwt.catch
      (fun () ->
        let* _ = Lwt.nchoose [readable; writable; timeout] in
        Lwt.return
          (Mariadb.Nonblocking.Status.create
             ~read:(Lwt_unix.readable fd)
             ~write:(Lwt_unix.writable fd)
             ()))
      (function
       | Lwt_unix.Timeout ->
           Lwt.return (Mariadb.Nonblocking.Status.create ~timeout:true ())
       | exception_ -> Lwt.fail exception_)
end

module M = Mariadb.Nonblocking.Make (Wait)
module Streaming = M.Streaming.Make (Lwt)

type mode =
  | Buffered
  | Streaming

type arguments =
  { mode : mode
  ; rows : int
  ; row_bytes : int
  }

type argument_error =
  | Missing_arguments
  | Unknown_mode of string
  | Non_positive_rows of int
  | Non_positive_row_bytes of int
  | Invalid_integer of string

let parse_positive raw make_error =
  match int_of_string_opt raw with
  | None -> Error (Invalid_integer raw)
  | Some value when value <= 0 -> Error (make_error value)
  | Some value -> Ok value

let parse_arguments () =
  if Array.length Sys.argv <> 4 then
    Error Missing_arguments
  else
    let mode =
      match Sys.argv.(1) with
      | "buffered" -> Ok Buffered
      | "streaming" -> Ok Streaming
      | raw -> Error (Unknown_mode raw)
    in
    match
      mode,
      parse_positive Sys.argv.(2) (fun rows -> Non_positive_rows rows),
      parse_positive Sys.argv.(3)
        (fun bytes -> Non_positive_row_bytes bytes)
    with
    | Ok mode, Ok rows, Ok row_bytes ->
        Ok { mode; rows; row_bytes }
    | Error error, _, _
    | _, Error error, _
    | _, _, Error error ->
        Error error

let environment name default =
  match Sys.getenv_opt name with
  | Some value -> value
  | None -> default

let or_fail operation = function
  | Ok value -> Lwt.return value
  | Error (code, message) ->
      Lwt.fail_with
        (Printf.sprintf "%s failed: (%d) %s" operation code message)

let memory_kib name =
  let input = open_in "/proc/self/status" in
  Fun.protect
    ~finally:(fun () -> close_in input)
    (fun () ->
      let rec read () =
        match input_line input with
        | line ->
            (match String.split_on_char ':' line with
             | [key; raw] when key = name ->
                 (match
                    String.split_on_char ' ' (String.trim raw)
                    |> List.filter (fun item -> item <> "")
                  with
                  | value :: _ -> int_of_string value
                  | [] -> failwith ("missing value for " ^ name))
             | _ -> read ())
        | exception End_of_file ->
            failwith ("missing " ^ name ^ " in /proc/self/status")
      in
      read ())

let consume_buffered statement =
  let* result = M.Stmt.execute statement [||] in
  let* buffered = or_fail "buffered execute" result in
  let rec consume rows =
    let* result = M.Res.fetch (module M.Row.Array) buffered in
    let* row = or_fail "buffered fetch" result in
    match row with
    | None -> Lwt.return rows
    | Some fields ->
        ignore (M.Field.string fields.(0));
        consume (rows + 1)
  in
  consume 0

let consume_streaming statement row_bytes =
  let config =
    match
      M.Streaming.Config.unbuffered
        ~max_field_bytes:row_bytes
        ~max_row_bytes:row_bytes
    with
    | Ok config -> config
    | Error _ -> invalid_arg "invalid memory-test limits"
  in
  let rows = ref 0 in
  let* result =
    Streaming.execute
      ~config
      (module M.Row.Array)
      ~on_row:(fun fields ->
        ignore (M.Field.string fields.(0));
        incr rows;
        Lwt.return M.Streaming.Control.Continue)
      statement
      [||]
  in
  match result with
  | Ok (M.Streaming.Completion.Exhausted { rows_read }) ->
      assert (rows_read = !rows);
      Lwt.return rows_read
  | Ok (M.Streaming.Completion.Stopped _) ->
      Lwt.fail_with "memory stream unexpectedly stopped"
  | Error _ ->
      Lwt.fail_with "memory stream failed"

let run arguments =
  let baseline_rss_kib = memory_kib "VmRSS" in
  let* result =
    M.connect
      ~host:(environment "OCAML_MARIADB_HOST" "127.0.0.1")
      ~user:(environment "OCAML_MARIADB_USER" "root")
      ~pass:(environment "OCAML_MARIADB_PASS" "")
      ~db:(environment "OCAML_MARIADB_DB" "mysql")
      ~port:(int_of_string (environment "OCAML_MARIADB_PORT" "33067"))
      ()
  in
  let* connection = or_fail "connect" result in
  Lwt.finalize
    (fun () ->
      let query =
        Printf.sprintf
          "SELECT REPEAT('x', %d) FROM seq_1_to_%d"
          arguments.row_bytes
          arguments.rows
      in
      let* result = M.prepare connection query in
      let* statement = or_fail "prepare memory query" result in
      Lwt.finalize
        (fun () ->
          let cpu_started = Sys.time () in
          let wall_started = Unix.gettimeofday () in
          let minor_started = Gc.minor_words () in
          let* rows_read =
            match arguments.mode with
            | Buffered -> consume_buffered statement
            | Streaming ->
                consume_streaming statement arguments.row_bytes
          in
          let cpu_seconds = Sys.time () -. cpu_started in
          let wall_seconds = Unix.gettimeofday () -. wall_started in
          let minor_words = Gc.minor_words () -. minor_started in
          Gc.full_major ();
          Printf.printf
            "mode=%s rows=%d rows_read=%d row_bytes=%d total_bytes=%s \
             baseline_rss_kib=%d final_rss_kib=%d high_water_rss_kib=%d \
             cpu_seconds=%.3f wall_seconds=%.3f minor_words=%.0f\n%!"
            (match arguments.mode with
             | Buffered -> "buffered"
             | Streaming -> "streaming")
            arguments.rows
            rows_read
            arguments.row_bytes
            (Unsigned.UInt64.to_string
               (Unsigned.UInt64.mul
                  (Unsigned.UInt64.of_int arguments.rows)
                  (Unsigned.UInt64.of_int arguments.row_bytes)))
            baseline_rss_kib
            (memory_kib "VmRSS")
            (memory_kib "VmHWM")
            cpu_seconds
            wall_seconds
            minor_words;
          Lwt.return_unit)
        (fun () ->
          let* result = M.Stmt.close statement in
          or_fail "close memory statement" result))
    (fun () -> M.close connection)

let () =
  match parse_arguments () with
  | Ok arguments -> Lwt_main.run (run arguments)
  | Error _ ->
      prerr_endline
        "usage: streaming_memory.exe (buffered|streaming) ROWS ROW_BYTES";
      exit 2
