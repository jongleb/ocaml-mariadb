let () =
  let module Identity_wait = struct
    module IO = struct
      type 'a future = Future of 'a

      let (>>=) (Future value) continue = continue value
      let return value = Future value
    end

    let wait _ _ = failwith "streaming unit test unexpectedly waited for I/O"
  end in
  let module M = Mariadb.Nonblocking.Make (Identity_wait) in
  assert (
    Result.is_ok
      (M.Streaming.Config.unbuffered
         ~max_field_bytes:1024
         ~max_row_bytes:4096));
  assert (
    M.Streaming.Config.unbuffered
      ~max_field_bytes:0
      ~max_row_bytes:4096
    = Error (M.Streaming.Config.Non_positive_max_field_bytes 0));
  assert (
    M.Streaming.Config.unbuffered
      ~max_field_bytes:1024
      ~max_row_bytes:0
    = Error (M.Streaming.Config.Non_positive_max_row_bytes 0));
  assert (
    M.Streaming.Config.unbuffered
      ~max_field_bytes:8192
      ~max_row_bytes:4096
    = Error
        (M.Streaming.Config.Field_limit_exceeds_row_limit
           { max_field_bytes = 8192
           ; max_row_bytes = 4096
           }))
