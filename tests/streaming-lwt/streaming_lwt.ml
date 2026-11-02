open Lwt.Syntax

module Wait = struct
  type behavior =
    | Normal
    | Reject_once of exn

  let behavior = ref Normal

  module IO = struct
    type 'a future = 'a Lwt.t

    let (>>=) future continue = Lwt.bind future continue
    let return value = Lwt.return value
  end

  let wait mariadb status =
    match !behavior with
    | Reject_once exception_ ->
        behavior := Normal;
        Lwt.fail exception_
    | Normal ->
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
               Lwt.return
                 (Mariadb.Nonblocking.Status.create ~timeout:true ())
           | exception_ -> Lwt.fail exception_)
end

module M = Mariadb.Nonblocking.Make (Wait)
module Streaming = M.Streaming.Make (Lwt)

let environment name default =
  match Sys.getenv_opt name with
  | Some value -> value
  | None -> default

let or_fail operation = function
  | Ok value -> Lwt.return value
  | Error (code, message) ->
      Lwt.fail_with
        (Printf.sprintf "%s failed: (%d) %s" operation code message)

let exec_or_fail connection operation query =
  let* result = M.exec connection query in
  let* _ = or_fail operation result in
  Lwt.return_unit

let config ~max_field_bytes ~max_row_bytes =
  match
    M.Streaming.Config.unbuffered ~max_field_bytes ~max_row_bytes
  with
  | Ok config -> config
  | Error _ -> invalid_arg "invalid streaming test limits"

let connect () =
  let* result =
    M.connect
      ~host:(environment "OCAML_MARIADB_HOST" "127.0.0.1")
      ~user:(environment "OCAML_MARIADB_USER" "root")
      ~pass:(environment "OCAML_MARIADB_PASS" "")
      ~db:(environment "OCAML_MARIADB_DB" "mysql")
      ~port:(int_of_string (environment "OCAML_MARIADB_PORT" "33067"))
      ()
  in
  or_fail "connect" result

let with_connection use =
  let* connection = connect () in
  Lwt.finalize
    (fun () -> use connection)
    (fun () -> M.close connection)

let with_statement connection operation query use =
  let* result = M.prepare connection query in
  let* statement = or_fail ("prepare " ^ operation) result in
  Lwt.finalize
    (fun () -> use statement)
    (fun () ->
      let* result = M.Stmt.close statement in
      or_fail ("close " ^ operation) result)

let execute query config =
  with_connection (fun connection ->
    with_statement connection "streaming query" query (fun statement ->
      let rows = ref [] in
      let* result =
        Streaming.execute
          ~config
          (module M.Row.Array)
          ~on_row:(fun row ->
            rows := Array.map M.Field.value row :: !rows;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      Lwt.return (result, List.rev !rows)))

let test_limits () =
  let* result, rows =
    execute
      "SELECT CAST(REPEAT('x', 1024) AS BINARY)"
      (config ~max_field_bytes:1024 ~max_row_bytes:1024)
  in
  assert (
    result = Ok (M.Streaming.Completion.Exhausted { rows_read = 1 }));
  assert (rows = [[| `String (String.make 1024 'x') |]]);
  let* result, _ =
    execute
      "SELECT CAST(REPEAT('x', 1025) AS BINARY)"
      (config ~max_field_bytes:1024 ~max_row_bytes:1024)
  in
  (match result with
   | Error
       (M.Streaming.Error.Operation_failed
          (M.Streaming.Error.Field_limit_exceeded
             { column = 0
             ; length
             ; max_field_bytes = 1024
             })) ->
       assert (Unsigned.UInt64.to_int length = 1025)
   | _ -> failwith "expected field-limit error");
  let* result, _ =
    execute
      "SELECT REPEAT('a', 512), REPEAT('b', 512)"
      (config ~max_field_bytes:512 ~max_row_bytes:1024)
  in
  assert (
    result = Ok (M.Streaming.Completion.Exhausted { rows_read = 1 }));
  let* result, _ =
    execute
      "SELECT REPEAT('a', 512), REPEAT('b', 513)"
      (config ~max_field_bytes:600 ~max_row_bytes:1024)
  in
  (match result with
   | Error
       (M.Streaming.Error.Operation_failed
          (M.Streaming.Error.Row_limit_exceeded
             { length
             ; max_row_bytes = 1024
             })) ->
       assert (Unsigned.UInt64.to_int length = 1025)
   | _ -> failwith "expected row-limit error");
  Lwt.return_unit

let test_backpressure_stop_and_reuse () =
  with_connection (fun connection ->
    with_statement
      connection
      "backpressure"
      "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
      (fun statement ->
        let streaming_config =
          config ~max_field_bytes:1024 ~max_row_bytes:1024
        in
        let callback_count = ref 0 in
        let first_callback, notify_first_callback = Lwt.wait () in
        let gate, open_gate = Lwt.wait () in
        let stream =
          Streaming.execute
            ~config:streaming_config
            (module M.Row.Array)
            ~on_row:(fun _ ->
              incr callback_count;
              if !callback_count = 1 then begin
                Lwt.wakeup_later notify_first_callback ();
                gate
              end
              else
                Lwt.return M.Streaming.Control.Continue)
            statement
            [||]
        in
        let* () = first_callback in
        let* () = Lwt_unix.sleep 0.05 in
        assert (!callback_count = 1);
        Lwt.cancel stream;
        (match Lwt.state stream with
         | Lwt.Sleep -> ()
         | Lwt.Return _
         | Lwt.Fail _ ->
             failwith "native cancellation escaped no_cancel");
        Lwt.wakeup_later open_gate M.Streaming.Control.Continue;
        let* result = stream in
        assert (
          result = Ok
            (M.Streaming.Completion.Exhausted { rows_read = 3 }));
        assert (!callback_count = 3);
        let callback_count = ref 0 in
        let* result =
          Streaming.execute
            ~config:streaming_config
            (module M.Row.Array)
            ~on_row:(fun _ ->
              incr callback_count;
              Lwt.return M.Streaming.Control.Stop)
            statement
            [||]
        in
        assert (
          result = Ok
            (M.Streaming.Completion.Stopped { rows_read = 1 }));
        assert (!callback_count = 1);
        let values = ref [] in
        let* result =
          Streaming.execute
            ~config:streaming_config
            (module M.Row.Array)
            ~on_row:(fun row ->
              values := M.Field.int row.(0) :: !values;
              Lwt.return M.Streaming.Control.Continue)
            statement
            [||]
        in
        assert (
          result = Ok
            (M.Streaming.Completion.Exhausted { rows_read = 3 }));
        assert (List.rev !values = [1; 2; 3]);
        let* result = M.Stmt.execute statement [||] in
        let* buffered = or_fail "buffered execute after streaming" result in
        assert (M.Res.num_rows buffered = 3);
        Lwt.return_unit))

let test_empty_result connection =
  with_statement
    connection
    "empty result"
    "SELECT 1 WHERE FALSE"
    (fun statement ->
      let callbacks = ref 0 in
      let* result =
        Streaming.execute
          ~config:(config ~max_field_bytes:16 ~max_row_bytes:16)
          (module M.Row.Array)
          ~on_row:(fun _ ->
            incr callbacks;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      assert (
        result = Ok
          (M.Streaming.Completion.Exhausted { rows_read = 0 }));
      assert (!callbacks = 0);
      Lwt.return_unit)

let test_long_values connection =
  let* () =
    exec_or_fail
      connection
      "create streaming_values"
      "CREATE TEMPORARY TABLE streaming_values \
       (id INTEGER PRIMARY KEY, text_value LONGTEXT, blob_value LONGBLOB)"
  in
  let cases =
    List.map
      (fun size ->
        String.init
          size
          (fun index -> Char.chr (97 + (index mod 26))),
        Bytes.init
          size
          (fun index -> Char.chr (index land 255)))
      [0; 1; 4095; 4096; 4097; 65536]
  in
  let* () =
    with_statement
      connection
      "streaming value insert"
      "INSERT INTO streaming_values VALUES (?, ?, ?)"
      (fun statement ->
        Lwt_list.iteri_s
          (fun id (text, blob) ->
            let* result =
              M.Stmt.execute
                statement
                [| `Int id; `String text; `Bytes blob |]
            in
            let* _ = or_fail "insert streaming value" result in
            Lwt.return_unit)
          cases)
  in
  with_statement
    connection
    "streaming value select"
    "SELECT id, text_value, blob_value \
     FROM streaming_values ORDER BY id"
    (fun statement ->
      let received = ref [] in
      let* result =
        Streaming.execute
          ~config:
            (config ~max_field_bytes:65536 ~max_row_bytes:140000)
          (module M.Row.Array)
          ~on_row:(fun row ->
            received :=
              ( M.Field.int row.(0)
              , M.Field.bytes row.(1)
              , M.Field.bytes row.(2)
              )
              :: !received;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      (match result with
       | Ok
           (M.Streaming.Completion.Exhausted
              { rows_read }) ->
           assert (rows_read = List.length cases)
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Field_limit_exceeded
                 { column; length; max_field_bytes })) ->
           failwith
             (Printf.sprintf
                "field %d length %s exceeded %d"
                column
                (Unsigned.UInt64.to_string length)
                max_field_bytes)
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Row_limit_exceeded
                 { length; max_row_bytes })) ->
           failwith
             (Printf.sprintf
                "row length %s exceeded %d"
                (Unsigned.UInt64.to_string length)
                max_row_bytes)
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Connector_failed
                 { code; message; sqlstate })) ->
           failwith
             (Printf.sprintf
                "connector (%d/%s): %s"
                code
                sqlstate
                message)
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Connection_lost _)) ->
           failwith "long-value connection was lost"
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Runtime_failed exception_)) ->
           failwith (Printexc.to_string exception_)
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Fixed_width_truncation
                 { column; length; buffer_bytes })) ->
           failwith
             (Printf.sprintf
                "fixed field %d length %s exceeded %d"
                column
                (Unsigned.UInt64.to_string length)
                buffer_bytes)
       | Error
           (M.Streaming.Error.Operation_failed
              M.Streaming.Error.No_result_set) ->
           failwith "long-value query had no result set"
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Parameter_count_mismatch _)) ->
           failwith "long-value parameter count mismatch"
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Callback_failed exception_)) ->
           failwith (Printexc.to_string exception_)
       | Error (M.Streaming.Error.Cleanup_failed _) ->
           failwith "long-value cleanup failed"
       | Ok (M.Streaming.Completion.Stopped _) ->
           failwith "long-value streaming was stopped");
      assert (
        List.rev !received
        = List.mapi
            (fun id (text, blob) ->
              id, Bytes.of_string text, blob)
            cases);
      Lwt.return_unit)

let test_year_value connection =
  let* () =
    exec_or_fail
      connection
      "create streaming_year"
      "CREATE TEMPORARY TABLE streaming_year (value YEAR NOT NULL)"
  in
  let* () =
    exec_or_fail
      connection
      "insert streaming_year"
      "INSERT INTO streaming_year VALUES (2024)"
  in
  with_statement
    connection
    "streaming year select"
    "SELECT value FROM streaming_year"
    (fun statement ->
      let years = ref [] in
      let* result =
        Streaming.execute
          ~config:(config ~max_field_bytes:16 ~max_row_bytes:16)
          (module M.Row.Array)
          ~on_row:(fun row ->
            years := M.Field.int row.(0) :: !years;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      assert (
        result = Ok
          (M.Streaming.Completion.Exhausted { rows_read = 1 }));
      assert (!years = [2024]);
      Lwt.return_unit)

let test_callback_failure_and_reuse connection =
  with_statement
    connection
    "callback failure"
    "SELECT 1 UNION ALL SELECT 2"
    (fun statement ->
      let* result =
        Streaming.execute
          ~config:(config ~max_field_bytes:16 ~max_row_bytes:16)
          (module M.Row.Array)
          ~on_row:(fun _ -> Lwt.fail_with "consumer failed")
          statement
          [||]
      in
      (match result with
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Callback_failed _)) ->
           ()
       | _ -> failwith "expected callback failure");
      let rows = ref 0 in
      let* result =
        Streaming.execute
          ~config:(config ~max_field_bytes:16 ~max_row_bytes:16)
          (module M.Row.Array)
          ~on_row:(fun _ ->
            incr rows;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      assert (
        result = Ok
          (M.Streaming.Completion.Exhausted { rows_read = 2 }));
      assert (!rows = 2);
      Lwt.return_unit)

let test_limit_failure_and_reuse connection =
  with_statement
    connection
    "limit failure reuse"
    "SELECT REPEAT('x', 1025)"
    (fun statement ->
      let* result =
        Streaming.execute
          ~config:
            (config ~max_field_bytes:1024 ~max_row_bytes:1024)
          (module M.Row.Array)
          ~on_row:(fun _ -> Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      (match result with
       | Error
           (M.Streaming.Error.Operation_failed
              (M.Streaming.Error.Field_limit_exceeded _)) ->
           ()
       | _ -> failwith "expected reusable field-limit failure");
      let rows = ref 0 in
      let* result =
        Streaming.execute
          ~config:
            (config ~max_field_bytes:2048 ~max_row_bytes:2048)
          (module M.Row.Array)
          ~on_row:(fun _ ->
            incr rows;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      assert (
        result = Ok
          (M.Streaming.Completion.Exhausted { rows_read = 1 }));
      assert (!rows = 1);
      Lwt.return_unit)

let connection_id connection =
  with_statement
    connection
    "connection id"
    "SELECT CONNECTION_ID()"
    (fun statement ->
      let* result = M.Stmt.execute statement [||] in
      let* buffered = or_fail "execute connection id" result in
      let* result = M.Res.fetch (module M.Row.Array) buffered in
      let* row = or_fail "fetch connection id" result in
      match row with
      | Some row -> Lwt.return (M.Field.int row.(0))
      | None -> Lwt.fail_with "connection id query returned no row")

type interruption =
  | Kill_query
  | Kill_connection

let test_interruption interruption =
  with_connection (fun connection ->
    with_connection (fun control ->
      let* id = connection_id connection in
      let* result =
        M.prepare connection
          "WITH RECURSIVE sequence(n) AS \
           (SELECT 1 UNION ALL \
            SELECT n + 1 FROM sequence WHERE n < 1000) \
           SELECT n, SLEEP(0.002), REPEAT('x', 4096) FROM sequence"
      in
      let* statement = or_fail "prepare interrupted stream" result in
      Lwt.finalize
        (fun () ->
          let first_callback, notify_first_callback = Lwt.wait () in
          let gate, open_gate = Lwt.wait () in
          let callbacks = ref 0 in
          let stream =
            Streaming.execute
              ~config:
                (config ~max_field_bytes:4096 ~max_row_bytes:8192)
              (module M.Row.Array)
              ~on_row:(fun _ ->
                incr callbacks;
                if !callbacks = 1 then begin
                  Lwt.wakeup_later notify_first_callback ();
                  gate
                end
                else
                  Lwt.return M.Streaming.Control.Continue)
              statement
              [||]
          in
          let* () = first_callback in
          let command =
            match interruption with
            | Kill_query -> Printf.sprintf "KILL QUERY %d" id
            | Kill_connection -> Printf.sprintf "KILL CONNECTION %d" id
          in
          let* () = exec_or_fail control command command in
          Lwt.wakeup_later open_gate M.Streaming.Control.Continue;
          let* result = stream in
          (match interruption, result with
           | Kill_query,
             Error
               (M.Streaming.Error.Operation_failed
                  (M.Streaming.Error.Connector_failed _)) ->
               ()
           | Kill_connection,
             Error
               (M.Streaming.Error.Operation_failed
                  (M.Streaming.Error.Connection_lost _))
           | Kill_connection,
             Error (M.Streaming.Error.Cleanup_failed _) ->
               ()
           | Kill_query, Error (M.Streaming.Error.Cleanup_failed _) ->
               failwith "KILL QUERY unexpectedly failed cleanup"
           | Kill_query,
             Error
               (M.Streaming.Error.Operation_failed
                  (M.Streaming.Error.Connection_lost
                     { code; message; _ })) ->
               failwith
                 (Printf.sprintf
                    "KILL QUERY was classified lost: (%d) %s"
                    code
                    message)
           | Kill_connection,
             Error
               (M.Streaming.Error.Operation_failed
                  (M.Streaming.Error.Connector_failed
                     { code; message; _ })) ->
               failwith
                 (Printf.sprintf
                    "KILL CONNECTION was not classified lost: (%d) %s"
                    code
                    message)
           | _ -> failwith "expected typed interrupted operation");
          match interruption with
          | Kill_connection ->
              let* result = M.ping connection in
              (match result with
               | Error (2006, _) -> Lwt.return_unit
               | _ ->
                   Lwt.fail_with
                     "disconnected connection was not marked discard-only")
          | Kill_query ->
              let* result =
                Streaming.execute
                  ~config:
                    (config
                       ~max_field_bytes:4096
                       ~max_row_bytes:8192)
                  (module M.Row.Array)
                  ~on_row:(fun _ -> Lwt.return M.Streaming.Control.Stop)
                  statement
                  [||]
              in
              assert (
                result = Ok
                  (M.Streaming.Completion.Stopped
                     { rows_read = 1 }));
              Lwt.return_unit)
        (fun () ->
          let* result = M.Stmt.close statement in
          match interruption with
          | Kill_query -> or_fail "close interrupted stream" result
          | Kill_connection -> Lwt.return_unit)))

let test_wide_result connection =
  let query =
    "SELECT REPEAT('x', 100)"
    ^ String.concat "" (List.init 19 (fun _ -> ", REPEAT('x', 0)"))
  in
  with_statement connection "wide result" query (fun statement ->
      let received = ref [] in
      let* result =
        Streaming.execute
          ~config:(config ~max_field_bytes:128 ~max_row_bytes:128)
          (module M.Row.Array)
          ~on_row:(fun row ->
            received :=
              (Array.length row, M.Field.string row.(0)) :: !received;
            Lwt.return M.Streaming.Control.Continue)
          statement
          [||]
      in
      assert (
        result = Ok (M.Streaming.Completion.Exhausted { rows_read = 1 }));
      assert (!received = [(20, String.make 100 'x')]);
      Lwt.return_unit)

let test_scheduler_progress () =
  with_connection (fun connection ->
      with_statement
        connection
        "scheduler progress"
        "SELECT REPEAT('x', 4096) FROM seq_1_to_2000"
        (fun statement ->
          let pauses = ref 0 in
          let module Counting_lifecycle = struct
            let catch = Lwt.catch
            let no_cancel = Lwt.no_cancel

            let pause () =
              incr pauses;
              Lwt.pause ()
          end in
          let module Counting_streaming =
            M.Streaming.Make (Counting_lifecycle)
          in
          let* result =
            Counting_streaming.execute
              ~config:(config ~max_field_bytes:4096 ~max_row_bytes:4096)
              (module M.Row.Array)
              ~on_row:(fun _ -> Lwt.return M.Streaming.Control.Continue)
              statement
              [||]
          in
          assert (
            result
            = Ok (M.Streaming.Completion.Exhausted { rows_read = 2000 }));
          assert (!pauses = 7);
          Lwt.return_unit))

let test_values_and_failures () =
  with_connection (fun connection ->
    let* () = test_empty_result connection in
    let* () = test_long_values connection in
    let* () = test_year_value connection in
    let* () = test_callback_failure_and_reuse connection in
    let* () = test_limit_failure_and_reuse connection in
    test_wide_result connection)

let test_runtime_wait_failure () =
  let* connection = connect () in
  let* result = M.prepare connection "SELECT SLEEP(0.05), 1" in
  let* statement = or_fail "prepare runtime wait failure" result in
  Wait.behavior := Wait.Reject_once (Failure "injected wait failure");
  let* result =
    Streaming.execute
      ~config:(config ~max_field_bytes:16 ~max_row_bytes:16)
      (module M.Row.Array)
      ~on_row:(fun _ -> Lwt.return M.Streaming.Control.Continue)
      statement
      [||]
  in
  (match result with
   | Error
       (M.Streaming.Error.Operation_failed
          (M.Streaming.Error.Runtime_failed
             (Failure message)))
     when String.equal message "injected wait failure" ->
       ()
   | _ -> failwith "expected typed runtime failure");
  Lwt.return_unit

let main () =
  let* () = test_limits () in
  let* () = test_backpressure_stop_and_reuse () in
  let* () = test_values_and_failures () in
  let* () = test_scheduler_progress () in
  let* () = test_interruption Kill_query in
  let* () = test_interruption Kill_connection in
  test_runtime_wait_failure ()

let () = Lwt_main.run (main ())
