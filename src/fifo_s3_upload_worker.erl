-module(fifo_s3_upload_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-define(POOL, s3_upload).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {retries = 3}).


start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({part, {From, Ref, B, K, Id, P, C}, V}, State) ->
    upload(From, Ref, B, K, Id, P, C, V, State#state.retries),
    poolboy:checkin(?POOL, self()),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

upload(From, Ref, B, K, Id, P, C, V, 0) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            From ! {ok, Ref, {P, ETag}};
        E ->
            From ! {error, Ref, E}
    end;
upload(From, Ref, B, K, Id, P, C, V, Try) ->
    case erlcloud_s3:upload_part(B, K, Id, P, V, [], C) of
        {ok, [{etag, ETag}]} ->
            From ! {ok, Ref, {P, ETag}};
        _E ->
            upload(From, Ref, B, K, Id, P, C, V, Try - 1)
    end.
