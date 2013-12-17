-module(fifo_s3).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         make_config/4,
         delete/3,
         list/2
        ]).

-export([
         new_stream/3,
         stream_length/1,
         get_part/2,
         get_stream/1
        ]).

-export([
         new_upload/3,
         put_upload/3,
         put_upload/2,
         complete_upload/1,
         abort_upload/1
        ]).

-ignore_xref([
              list/2,
              delete/3,
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
          chunk = 1024*1024 :: pos_integer()
         }).

-record(upload, {
          bucket            :: string(),
          key               :: string(),
          conf              :: term(),
          id                :: string(),
          etags = []        :: [{non_neg_integer(), string()}],
          part = 1          :: non_neg_integer()
         }).

list(Bucket, Config) when is_binary(Bucket) ->
    list(binary_to_list(Bucket), Config);

list(Bucket, Config) ->
    try erlcloud_s3:list_objects(Bucket, Config) of
        List ->
            case proplists:get_value(contents, List) of
                undefined ->
                    {ok, []};
                C ->
                    [proplists:get_value(key, O) || O <- C]
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

new_stream(Bucket, Key, Config) when is_binary(Bucket) ->
    new_stream(binary_to_list(Bucket), Key, Config);

new_stream(Bucket, Key, Config) when is_binary(Key) ->
    new_stream(Bucket, binary_to_list(Key), Config);

new_stream(Bucket, Key, Config) ->
    try erlcloud_s3:list_objects(Bucket, Config) of
        List ->
            case proplists:get_value(contents, List) of
                undefined ->
                    {error, not_found};
                Content ->
                    case find_size(Content, Key) of
                        not_found ->
                            {error, not_found};
                        {ok, Size} ->
                            D = #download{
                                   bucket = Bucket,
                                   key = Key,
                                   conf = Config,
                                   size = Size
                                  },
                            {ok, D}
                    end
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
            case proplists:get_value(content, Data) of
                undefined ->
                    {error, content};
                D ->
                    {ok, D}
            end
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

new_upload(Bucket, Key, Config) when is_binary(Bucket) ->
    new_upload(binary_to_list(Bucket), Key, Config);
new_upload(Bucket, Key, Config) when is_binary(Key) ->
    new_upload(Bucket, binary_to_list(Key), Config);
new_upload(Bucket, Key, Config) ->
    case erlcloud_s3:start_multipart(Bucket, Key, [], [], Config) of
        {ok, [{uploadId,Id}]} ->
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
    erlcloud_s3:new(AKey, SKey, Host, Port).

%%%===================================================================
%%% Internal functions
%%%===================================================================

build_range(Start, Stop) when Start > Stop ->
	lists:flatten(io_lib:format("bytes=~p-~p", [Start, Stop])).

find_size([], _) ->
    not_found;

find_size([O|R], File) ->
    case proplists:get_value(key, O) of
        Name when Name =:= File ->
            {ok, proplists:get_value(size, O)};
        _ ->
            find_size(R, File)
    end.

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
