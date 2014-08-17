%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 30 Dec 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(fifo_s3_download).

-behaviour(gen_server).

%% API
-export([new/2, new/6, new/7,
         start_link/7,
         get/1, get/2,
         abort/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(DONE_TIMEOUT, 100).

-define(POOL, s3_download).

-record(state, {
          parts = [],
          part = 0,
          chunk_size = 1048576,
          size,
          conf,
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
    new(AKey, SKey, Host, Port, Bucket, Key, Options).

new(AKey, SKey, Host, Port, Bucket, Key) ->
    new(AKey, SKey, Host, Port, Bucket, Key, []).
new(AKey, SKey, Host, Port, Bucket, Key, Opts) when is_binary(Bucket) ->
    new(AKey, SKey, Host, Port, binary_to_list(Bucket), Key, Opts);

new(AKey, SKey, Host, Port, Bucket, Key, Opts) when is_binary(Key) ->
    new(AKey, SKey, Host, Port, Bucket, binary_to_list(Key), Opts);

new(AKey, SKey, Host, Port, Bucket, Key, Opts) ->
    fifo_s3_download_sup:start_child(AKey, SKey, Host, Port, Bucket, Key, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(AKey, SKey, Host, Port, Bucket, Key, Opts) ->
    gen_server:start_link(?MODULE,
                          [AKey, SKey, Host, Port, Bucket, Key, Opts], []).

get(PID) ->
    get(PID, infinity).

get(PID, Timeout) ->
    case process_info(PID) of
        undefined ->
            {error, failed};
        _ ->
            case gen_server:call(PID, get, Timeout) of
                {ok, done} ->
                    {ok, done};
                {ok, Worker} ->
                    Res = gen_server:call(Worker, get, Timeout),
                    pooler:return_member(?POOL, Worker, ok),
                    Res;
                E ->
                    E
            end
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
init([AKey, SKey, Host, Port, Bucket, Key, Opts]) ->
    Conf = fifo_s3:make_config(AKey, SKey, Host, Port),
    CS = proplists:get_value(chunk_size, Opts, 1048576),
    State = #state{
               bucket = Bucket,
               key = Key,
               conf = Conf,
               chunk_size = CS
              },
    {ok, State, 0}.

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
handle_call(get, _From,
            State=#state{part=P, size=S, chunk_size=C, parts=[]})
  when P*C >= S ->
    {stop, normal, {ok, done}, State};

handle_call(get, _From,
            State = #state{part=P, size=S, chunk_size=C, parts=[D|Ds]})
  when P*C >= S ->
    {reply, {ok, D}, State#state{parts=Ds}};
handle_call(get, _From, State =
                #state{part=P, size=S, chunk_size=C,
                       bucket=B, key=K, conf=Conf,
                       parts = [D|Ds]}) ->
    Worker = pooler:take_member(?POOL),
    gen_server:cast(Worker, {download, self(), P, B, K, Conf, C, S}),
    {reply, {ok, D}, State#state{parts=Ds ++ [Worker], part=P + 1}};
handle_call(abort, _From, State = #state{parts=Ds}) ->
    [begin
         gen_server:cast(W, cancle),
         pooler:return_member(?POOL, W, ok)
     end || W <- Ds],
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
handle_info(timeout, State =
                #state{
                   bucket = Bucket,
                   key = Key,
                   conf = Conf,
                   chunk_size = CS
                  }) ->
    {ok, DPara} = application:get_env(fifo_s3, download_preload_chunks),
    try erlcloud_s3:get_object_metadata(Bucket, Key, Conf) of
        Metadata ->
            case proplists:get_value(content_length, Metadata) of
                undefined ->
                    {error, not_found};
                SizeS ->
                    Size = list_to_integer(SizeS),
                    {P, Ds} = build_initial_downloads(
                                0, Size, CS, DPara, Bucket, Key, Conf),
                    State1 = State#state{
                               size = Size,
                               part = P,
                               parts = Ds
                              },
                    {noreply, State1}
            end
    catch
        _:E ->
            lager:error("List error: ~p", [E]),
            {stop, {error, E}, State}
    end;

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

terminate(_Reason, #state{parts=Ds}) ->
    [begin
         gen_server:cast(W, cancle),
         pooler:return_member(?POOL, W, ok)
     end || W <- Ds],
    ok.

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

build_initial_downloads(P, S, C, M, B, K, Conf) ->
    {P1, L} = build_initial_downloads(P, S, C, M, B, K, Conf, []),
    {P1, lists:reverse(L)}.

build_initial_downloads(P, S, C, _, _, _, _, Acc)
  when P*C >= S ->
    {P, Acc};
build_initial_downloads(P, _, _, M, _, _, _, Acc)
  when P =:= M ->
    {P, Acc};

build_initial_downloads(P, S, C, M, B, K, Conf, Acc) ->
    Worker = pooler:take_member(?POOL),
    gen_server:cast(Worker, {download, self(), P, B, K, Conf, C, S}),
    build_initial_downloads(P+1, S, C, M, B, K, Conf, [Worker|Acc]).
