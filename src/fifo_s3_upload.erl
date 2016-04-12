%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 30 Dec 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(fifo_s3_upload).

-behaviour(gen_server).

%% API
-export([new/2, new/6,
         start_link/6,
         part/2, part/3,
         abort/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3,
         done/1, done/2]).

-define(SERVER, ?MODULE).

-define(DONE_TIMEOUT, 100).

-define(POOL, s3_upload).

-record(state, {
          uploads = [],
          etags = [],
          part = 1,
          done_from,
          conf,
          id,
          bucket,
          key
         }).

%%%===================================================================
%%% API
%%%===================================================================

new(Key, Options) ->
    AKey = proplists:get_value(access_key, Options),
    SKey = proplists:get_value(secret_key, Options),
    Host = proplists:get_value(host, Options),
    Port = proplists:get_value(port, Options),
    Bucket = proplists:get_value(bucket, Options),
    new(AKey, SKey, Host, Port, Bucket, Key).

new(AKey, SKey, Host, Port, Bucket, Key) when is_binary(Bucket) ->
    new(AKey, SKey, Host, Port, binary_to_list(Bucket), Key);

new(AKey, SKey, Host, Port, Bucket, Key) when is_binary(Key) ->
    new(AKey, SKey, Host, Port, Bucket, binary_to_list(Key));

new(AKey, SKey, Host, Port, Bucket, Key) ->
    fifo_s3_upload_sup:start_child(AKey, SKey, Host, Port, Bucket, Key).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(AKey, SKey, Host, Port, Bucket, Key) ->
    gen_server:start_link(?MODULE, [AKey, SKey, Host, Port, Bucket, Key], []).

part(PID, Part) ->
    part(PID, Part, infinity).

part(PID, Data, Timeout) ->
    case process_info(PID) of
        undefined ->
            {error, failed};
        _ ->
            case gen_server:call(PID, part, Timeout) of
                {ok, Worker, D} ->
                    gen_server:cast(Worker, {part, D, Data}),
                    ok;
                E ->
                    E
            end
    end.

done(PID) ->
    done(PID, infinity).

done(PID, Timeout) ->
    case process_info(PID) of
        undefined ->
            {error, failed};
        _ ->
            gen_server:call(PID, done, Timeout)
    end.

abort(PID) ->
    case process_info(PID) of
        undefined ->
            {error, failed};
        _ ->
            gen_server:cast(PID, abort)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([AKey, SKey, Host, Port, Bucket, Key]) ->
    Conf = fifo_s3:make_config(AKey, SKey, Host, Port),
    case erlcloud_s3:start_multipart(Bucket, Key, [], [], Conf) of
        {ok, [{uploadId, Id}]} ->
            {ok, #state{
                    bucket = Bucket,
                    key = Key,
                    conf = Conf,
                    id = Id
                   }};
        E ->
            {stop, E}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(part, _From, State =
                #state{
                   bucket=B, key=K, conf=C, id=Id, part=P,
                   uploads = Uploads}) ->
    io:format("[~p] started.~n", [P]),
    Worker = poolboy:checkout(?POOL, true, infinity),
    Ref =  make_ref(),
    io:format("[~p] Worker ~p / Ref: ~p.~n", [P, Worker, Ref]),
    Reply = {ok, Worker, {self(), Ref, B, K, Id, P, C}},
    {reply, Reply, State#state{uploads=[{Ref, Worker} | Uploads], part=P + 1}};

handle_call(done, _From, State = #state{bucket=B, key=K, conf=C, id=Id,
                                        etags=Ts, uploads=[]}) ->
    TS1 = lists:sort(Ts),
    io:format("Etags: ~p~n", [TS1]),
    erlcloud_s3:complete_multipart(B, K, Id, TS1, [], C),
    {stop, normal, ok, State};

handle_call(done, From, State) ->
    timer:send_after(?DONE_TIMEOUT, {done, From}),
    {noreply, State};

handle_call(abort, _From, State = #state{bucket=B, key=K, conf=C, id=Id}) ->
    erlcloud_s3:abort_multipart(B, K, Id, [], [], C),
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({done, From}, State = #state{bucket=B, key=K, conf=C, id=Id,
                                         etags=Ts, uploads=[]}) ->
    erlcloud_s3:complete_multipart(B, K, Id, lists:sort(Ts), [], C),

    gen_server:reply(From, ok),
    {stop, normal, State};
handle_info({done, From}, State) ->
    timer:send_after(?DONE_TIMEOUT, {done, From}),
    {noreply, State};
handle_info({ok, Ref, TagData}, State = #state{uploads=Uploads, etags=ETs}) ->
    Uploads1 = [{R, W} || {R, W} <- Uploads, R =/= Ref],
    {noreply, State#state{uploads=Uploads1, etags=[TagData | ETs]}};
handle_info({error, _Ref, E}, State) ->
    {stop, E, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(normal, _State) ->

    ok;

terminate(_Reason, #state{bucket=B, key=K, conf=C, id=Id}) ->
    erlcloud_s3:abort_multipart(B, K, Id, [], [], C).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
