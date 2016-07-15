-module(fifo_s3).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% meh we have to include that for the retry
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([
         main/1,
         start/0,
         make_config/4,
         delete/3,
         list/2
        ]).

-export([
         download/3,
         new_stream/3,
         new_stream/4,
         stream_length/1,
         get_part/2,
         get_stream/1
        ]).

-export([
         upload/4,
         new_upload/3,
         put_upload/3,
         put_upload/2,
         complete_upload/1,
         abort_upload/1
        ]).

-ignore_xref([
              main/1,
              list/2,
              upload/4,
              download/3,
              delete/3,
              new_stream/4,
              start/0,
              make_config/4,
              new_stream/3,
              get_part/2,
              put_upload/3,
              stream_length/1,
              get_stream/1,
              new_upload/3,
              put_upload/2,
              abort_upload/1,
              complete_upload/1
             ]).

-record(download, {
          bucket            :: string(),
          key               :: string(),
          conf              :: term(),
          size              :: pos_integer(),
          part  = 0         :: non_neg_integer(),
          chunk = 1048576   :: pos_integer()
         }).

-record(upload, {
          bucket            :: string(),
          key               :: string(),
          conf              :: term(),
          id                :: string(),
          etags = []        :: [{non_neg_integer(), string()}],
          part = 1          :: non_neg_integer()
         }).

start() ->
    application:ensure_all_started(fifo_s3).

-spec list(Bucket :: binary() | string(), Config :: term()) ->
                  {ok, [string()]} | {error, term()}.

list(Bucket, Config) when is_binary(Bucket) ->
    list(binary_to_list(Bucket), Config);

list(Bucket, Config) ->
    try erlcloud_s3:list_objects(Bucket, Config) of
        List ->
            case proplists:get_value(contents, List) of
                undefined ->
                    {ok, []};
                C ->
                    {ok, [proplists:get_value(key, O) || O <- C]}
            end
    catch
        _:E ->
            lager:error("Metadata fetch error: ~p", [E]),
            {error, E}
    end.

delete(Bucket, Key, Config) when is_binary(Bucket) ->
    delete(binary_to_list(Bucket), Key, Config);

delete(Bucket, Key, Config) when is_binary(Key) ->
    delete(Bucket, binary_to_list(Key), Config);

delete(Bucket, Key, Config) ->
    erlcloud_s3:delete_object(Bucket, Key, Config).

new_stream(Bucket, Key, Config) ->
    new_stream(Bucket, Key, Config, []).

new_stream(Bucket, Key, Config, Opts) when is_binary(Bucket) ->
    new_stream(binary_to_list(Bucket), Key, Config, Opts);

new_stream(Bucket, Key, Config, Opts) when is_binary(Key) ->
    new_stream(Bucket, binary_to_list(Key), Config, Opts);

new_stream(Bucket, Key, Config, Opts) ->
    try erlcloud_s3:get_object_metadata(Bucket, Key, Config) of
        Metadata ->
            case proplists:get_value(content_length, Metadata) of
                undefined ->
                    {error, not_found};
                SizeS ->
                    Size = list_to_integer(SizeS),
                    CS = proplists:get_value(chunk_size, Opts, 1048576),
                    D = #download{
                           bucket = Bucket,
                           key = Key,
                           conf = Config,
                           size = Size,
                           chunk = CS
                          },
                    {ok, D}
            end
    catch
        _:E ->
            lager:error("Metadata fetch error: ~p", [E]),
            {error, E}
    end.

stream_length(#download{size=S, chunk=C}) ->
    X = S / C,
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

get_part(P, #download{bucket=B, key=K, conf=Conf, chunk=C, size=Size})
  when is_number(P)->
    {Start, End} = start_stop(P, C, Size),
    Range = build_range(Start, End),
    try erlcloud_s3:get_object(B, K, [{range, Range}], Conf) of
        Data ->
            content(Data)
    catch
        _:E ->
            {error, E}
    end.

get_stream(#download{part=P, size=S, chunk=C})
  when P*C >=  S ->
    {ok, done};

get_stream(St = #download{part=P}) ->
    case get_part(P, St) of
        {ok, D} ->
            {ok, D, St#download{part = P+1}};
        E ->
            E
    end.

upload(Bucket, Key, Value, Config) when is_binary(Bucket) ->
    upload(binary_to_list(Bucket), Key, Value, Config);
upload(Bucket, Key, Value, Config) when is_binary(Key) ->
    upload(Bucket, binary_to_list(Key), Value, Config);
upload(Bucket, Key, Value, Config) ->
    erlcloud_s3:put_object(Bucket, Key, Value, Config).


download(Bucket, Key, Config) when is_binary(Bucket) ->
    download(binary_to_list(Bucket), Key, Config);
download(Bucket, Key, Config) when is_binary(Key) ->
    download(Bucket, binary_to_list(Key), Config);
download(Bucket, Key, Config) ->
    Data = erlcloud_s3:get_object(Bucket, Key, Config),
    content(Data).

content(Data) ->
    case proplists:get_value(content, Data) of
        undefined ->
            {error, content};
        D ->
            {ok, D}
    end.
-spec new_upload(Bucket :: binary() | string(),
                 Key :: binary() | string(),
                 Config :: term()) ->
                        {ok, #upload{}} |
                        {error, term()}.

new_upload(Bucket, Key, Config) when is_binary(Bucket) ->
    new_upload(binary_to_list(Bucket), Key, Config);

new_upload(Bucket, Key, Config) when is_binary(Key) ->
    new_upload(Bucket, binary_to_list(Key), Config);

new_upload(Bucket, Key, Config) when
      is_list(Bucket), is_list(Key) ->
    case erlcloud_s3:start_multipart(Bucket, Key, [], [], Config) of
        {ok, [{uploadId, Id}]} ->
            U = #upload{
                   bucket = Bucket,
                   key = Key,
                   conf = Config,
                   id = Id
                  },
            {ok, U};
        {error, Error} ->
            {error, Error}
    end.

put_upload(P, V, U = #upload{bucket=B, key=K, conf=C, id=Id, etags=Ts})
  when is_integer(P), is_binary(V) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            {ok, U#upload{etags = [{P, ETag} | Ts]}};
        {error, Error} ->
            {error, Error}
    end.

put_upload(V, U = #upload{bucket=B, key=K, conf=C, part=P, id=Id, etags=Ts})
  when is_binary(V) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            {ok, U#upload{part = P+1, etags = [{P, ETag} | Ts]}};
        {error, Error} ->
            {error, Error}
    end.

complete_upload(#upload{bucket=B, key=K, conf=C, id=Id, etags=Ts}) ->
    erlcloud_s3:complete_multipart(B, K, Id, lists:sort(Ts), [], C).

abort_upload(#upload{bucket=B, key=K, conf=C, id=Id}) ->
    erlcloud_s3:abort_multipart(B, K, Id, [], [], C).

make_config(AKey, SKey, Host, Port) when is_binary(AKey) ->
    make_config(binary_to_list(AKey), SKey, Host, Port);
make_config(AKey, SKey, Host, Port) when is_binary(SKey) ->
    make_config(AKey, binary_to_list(SKey), Host, Port);
make_config(AKey, SKey, Host, Port) when is_binary(Host) ->
    make_config(AKey, SKey, binary_to_list(Host), Port);
make_config(AKey, SKey, Host, Port) when is_number(Port) ->
    C = erlcloud_s3:new(AKey, SKey, Host, Port),
    C#aws_config{
      %% hackney is having ssl issues ... yay ...
      http_client = http_client(),
      s3_scheme = s3_scheme(),
      hackney_pool = default_retry,
      retry = fun erlcloud_retry:default_retry/1}.

http_client() ->
    get_env_atom(http_client, lhttpc).


s3_scheme() ->
    case get_env_atom(scheme, https) of
        https ->
            "https://";
        http ->
            "http://"
    end.

get_env_atom(Key, Default) ->
    case application:get_env(fifo_s3, Key, Default) of
        A when is_atom(A) ->
            A;
        L when is_list(L) ->
            list_to_atom(L)
    end.

%%%===================================================================
%%% Escript functions
%%%===================================================================

main(Args0) ->
    Conf0 = [{port, 443}, {chunk_size, 5242880}],
    Conf1 = case os:getenv("AWS_ACCESS_KEY_ID") of
                false ->
                    Conf0;
                AKey ->
                    [{access_key, list_to_binary(AKey)} | Conf0]
            end,
    Conf2 = case os:getenv("AWS_SECRET_ACCESS_KEY") of
                false ->
                    Conf1;
                SKey ->
                    [{secret_key, list_to_binary(SKey)} | Conf1]
            end,
    {Args1, Conf3} = mk_config(Args0, Conf2),
    start(),
    exec(Args1, Conf3).


mk_config(["-c", Concurrency | R], Conf) ->
    mk_config(["--concurrency", Concurrency | R], Conf);
mk_config(["--concurrency", Concurrency | R], Conf) ->
    C = list_to_integer(Concurrency),
    application:set_env(fifo_s3, download_pool_size, C),
    application:set_env(fifo_s3, download_pool_overflow, 0),
    application:set_env(fifo_s3, upload_pool_size, C),
    application:set_env(fifo_s3, upload_pool_overflow, 0),
    application:set_env(fifo_s3, download_preload_chunks, C),
    mk_config(R, Conf);
mk_config(["-h", Host | R], Conf) ->
    mk_config(["--host", Host | R], Conf);
mk_config(["--host", Host | R], Conf) ->
    mk_config(R, lists:keystore(host, 1, Conf, {host, list_to_binary(Host)}));

mk_config(["-p", Port | R], Conf) ->
    mk_config(["--port", Port | R], Conf);
mk_config(["--port", Port | R], Conf) ->
    mk_config(R, lists:keystore(port, 1, Conf, {port, list_to_integer(Port)}));

mk_config(["-s", Size | R], Conf) ->
    mk_config(["--chunk_size", Size | R], Conf);
mk_config(["--chunk_size", Size | R], Conf) ->
    C0 = lists:keystore(
           chunk_size, 1, Conf, {chunk_size, list_to_integer(Size)}),
    mk_config(R, C0);

mk_config(["-b", Bucket | R], Conf) ->
    mk_config(["--bucket", Bucket | R], Conf);
mk_config(["--bucket", Bucket | R], Conf) ->
    C0 = lists:keystore(bucket, 1, Conf, {bucket, list_to_binary(Bucket)}),
    mk_config(R, C0);
mk_config(R, Conf) ->
    {R, Conf}.
exec(["help"], _) ->
    io:format(
      "Simple S3 client based on fifo_s3\n"
      "\n"
      "fifo_s3 <options> <command>\n"
      "\n"
      "The following envoironment variables are used:\n"
      "AWS_ACCESS_KEY_ID\n"
      "AWS_SECRET_ACCESS_KEY\n"
      "\n"
      "Supported options are:\n"
      "--host|-h <host>        - host to up and download from\n"
      "--port|-p <port>        - port for the connection\n"
      "--bucket|-b <bucket>    - bucket to write to\n"
      "--concurrency|-c <cons> - number of concurrent conncetions\n"
      "--chunk_size|-s <bytes> - bytes per multipart chunk\n"
      "\n"
      "Commands:\n"
      "help                     - displays this help\n"
      "md5 <key>                - calculates md5 of a key\n"
      "get <key> <file>         - downloads an object to a file\n"
      "put <key> <file>         - uploads an object to a file\n"
     );
exec(["md5", Key], Config) ->
    {ok, D} = fifo_s3_download:new(Key, Config),
    Sum = calculate_md5(D, crypto:hash_init(md5)),
    io:format("MD5 Sum: ~s~n", [Sum]);

exec(["get", Key, File], Config) ->
    {ok, F} = file:open(File, [write]),
    {ok, D} = fifo_s3_download:new(Key, Config),
    safe_file(D, F);

exec(["put", Key, File], Config) ->
    {chunk_size, Size} = lists:keyfind(chunk_size, 1, Config),
    {ok, F} = file:open(File, [read, binary]),
    {ok, D} = fifo_s3_upload:new(Key, Config),
    write_file(D, F, Size, 0);
exec(C, Config) ->
    io:format("Unrecognized command ~s\n", [C]),
    exec(["help"], Config).


calculate_md5(D, H) ->
    case fifo_s3_download:get(D) of
        {ok, done} ->
            base16:encode(crypto:hash_final(H));
        {ok, Data} ->
            calculate_md5(D, crypto:hash_update(H, Data))
    end.

safe_file(D, F) ->
    case fifo_s3_download:get(D) of
        {ok, done} ->
            file:close(F);
        {ok, Data} ->
            file:write(F, Data),
            safe_file(D, F)
    end.

write_file(U, F, Size, Read) ->
    case file:read(F, Size) of
        {ok, Data} ->
            fifo_s3_upload:part(U, Data),
            Read1 = Read + byte_size(Data),
            write_file(U, F, Size, Read1);
        eof ->
            file:close(F),
            fifo_s3_upload:done(U),
            io:format("Done~n")
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

build_range(Start, Stop) when Start < Stop ->
    lists:flatten(io_lib:format("bytes=~p-~p", [Start, Stop])).

start_stop(P, Size, Max) ->
    Start = P*Size,
    End = case (P+1)*Size of
              EndX when EndX > Max ->
                  Max;
              EndX ->
                  EndX
          end,
    {Start, End - 1}.


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

start_stop_test() ->
    Size = 10,
    Max = 25,
    ?assertEqual({0, 9}, start_stop(0, Size, Max)),
    ?assertEqual({10, 19}, start_stop(1, Size, Max)),
    ?assertEqual({20, 24}, start_stop(2, Size, Max)).

-endif.
